# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Documentation
# 
# This code does not handle recursive top level hierarchies in its current form 

# MARKDOWN ********************

#  ## Libraries

# CELL ********************

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, concat_ws, when, expr, min, max, length,
    monotonically_increasing_id, split, explode, trim, upper
)
from pyspark.sql.types import StringType, LongType
from delta.tables import DeltaTable
from datetime import datetime, timedelta
from functools import reduce
import re



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DataCheck = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

target_table = 'Enr.enr_accountSchedule'

# Force a rebuild even when the source fingerprint is unchanged (schema change / manual rebuild).
force_refresh = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Loading and Preparation

# CELL ********************

%run FM_Utility

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Loading source tables...")
acc_schedule_lines = FM_Utility.load_cleaned_dataframe("Raw","AccScheduleLine")
acc_schedule_lines = FM_Utility.add_company_key(acc_schedule_lines)
# display(acc_schedule_lines)
company = spark.read.table('DP.dp_company')
accounts = spark.read.table('DP.dp_glaccounts').drop('totaling')

# --- Change-detection self-skip (slow-changing dimension) ---
# Fingerprint the raw structural inputs (AccScheduleLine + GLAccount: count + max rowversion).
# GLAccount is read from Raw, not DP.dp_glaccounts, because the BC rowversion lives on the raw
# source (DP transforms may drop it). If unchanged since the last successful build and not
# force_refresh, skip — the existing table stays intact and dependents still see success.
NOTEBOOK_KEY = "ENR_AccountSchedule"
_fp = FM_Utility.source_fingerprint([
    FM_Utility.load_cleaned_dataframe("Raw", "AccScheduleLine"),
    FM_Utility.load_cleaned_dataframe("Raw", "GLAccount", "camel"),
])
if not force_refresh and not FM_Utility.has_changed(NOTEBOOK_KEY, _fp):
    notebookutils.notebook.exit("skipped — no source change")

# Use the FM_Utility to clean column names. This is expected to handle
# renaming of columns to a consistent "camelCase" format.
acc_schedule_lines = FM_Utility.remove_column_number(acc_schedule_lines, "camel")
# Remove rows with NULL rowNo as they can't participate in hierarchy
acc_schedule_lines = acc_schedule_lines.filter(col('rowNo').isNotNull())
# Basic cleaning and selection for the accounts table
accounts_cleaned = accounts.select(
    col('no').cast(LongType()).alias('accountKey'),
    # accountKeyStr preserves the original Code[20] account No (e.g. "07740") as text.
    # accountKey (numeric) is used ONLY for the numeric totaling-range join below; the
    # persisted leaf_account_key / level keys must keep the zero-padding so they match
    # Account.accountScheduleKey (raw `no`) in the Power BI relationship.
    col('no').cast(StringType()).alias('accountKeyStr'),
    col('incomeBalance'),
    col('name').alias("fullAccountName"), # just for legacy purposes  can be deletede
    concat_ws(" - ", col('no'), col('name')).alias('accountName')
)

# Filter for relevant schedules and rows, and select necessary columns.
# This is a key base DataFrame we will work with.
base_hierarchy_nodes = acc_schedule_lines.filter(
    (upper(col("scheduleName")).like('PBI%'))  | 
    (upper(col("scheduleName")).like('POWERBI%')) & (col("rowNo") != 'CALC') & (col("totaling").isNotNull())
).select(
    col('scheduleName').alias('accountScheduleName'),
    col('rowNo'),
    col('lineNo').cast('long'),
    col('companyKey').cast('string'),
    col('totaling').cast('string'),
    col('totalingType').cast('string'),
    col('description').cast('string').alias('name'),
    col('indentation').cast('long')
).cache()

if DataCheck:
    print("--- DataCheck: Initial Filtered Hierarchy Nodes ---")
    print(f"Count: {base_hierarchy_nodes.count()}")
    base_hierarchy_nodes.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 1: Build the Multi-Level Hierarchy
# #

# CELL ********************


# **Refactoring Note:** The logic now iterates through each `accountScheduleName` to correctly handle schedules with different hierarchy structures.
print("Building hierarchy edges for all schedules...")
initial_edges = base_hierarchy_nodes.filter(
    col('totalingType') != 'Posting Accounts'
).select(
    'accountScheduleName',
    col('rowNo').alias('parent_rowNo'),
    explode(split(col('totaling'), '\|')).alias('child_rowNo')
).withColumn('child_rowNo', trim(col('child_rowNo')))

edges_with_ranges = initial_edges.filter(col('child_rowNo').contains('..'))
edges_without_ranges = initial_edges.filter(~col('child_rowNo').contains('..'))

hierarchy_edges = edges_without_ranges
if edges_with_ranges.count() > 0:
    expanded_ranges = edges_with_ranges.withColumn(
        'start', split(col('child_rowNo'), '\.\.').getItem(0).cast('int')
    ).withColumn(
        'end', split(col('child_rowNo'), '\.\.').getItem(1).cast('int')
    ).withColumn(
        'child_rowNo_expanded', explode(expr('sequence(start, end)'))
    ).select(
        'accountScheduleName', 'parent_rowNo', col('child_rowNo_expanded').cast('string').alias('child_rowNo')
    )
    hierarchy_edges = expanded_ranges.unionByName(edges_without_ranges)

schedule_names = [row['accountScheduleName'] for row in base_hierarchy_nodes.select('accountScheduleName').distinct().collect()]
print(f"Found schedules to process: {schedule_names}")

all_schedules_paths = []

for schedule_name in schedule_names:
    print(f"\n--- Processing schedule: {schedule_name} ---")
    
    schedule_nodes_base = base_hierarchy_nodes.filter(col('accountScheduleName') == schedule_name)
    schedule_edges = hierarchy_edges.filter(col('accountScheduleName') == schedule_name)

    nodes_with_levels = schedule_nodes_base.select('accountScheduleName', 'rowNo', 'name', length('rowNo').alias('level_len'))
    
    if nodes_with_levels.count() == 0:
        print(f"Skipping schedule {schedule_name} as it has no hierarchy nodes.")
        continue

    level_lengths = sorted([row['level_len'] for row in nodes_with_levels.select('level_len').distinct().collect()])
    level_mapping = {length: f"level{i+1}" for i, length in enumerate(level_lengths)}
    
    mapping_expr = expr("map(" + ", ".join([f"{k}, '{v}'" for k, v in level_mapping.items()]) + ")")
    nodes_with_levels = nodes_with_levels.withColumn("level", mapping_expr[col("level_len")])

    paths = nodes_with_levels.filter(col('level') == 'level1').select(
        'accountScheduleName', col('rowNo').alias('level1_Key'), col('name').alias('level1_Name')
    )

    # FIX: Loop until the true depth of the hierarchy is reached, not just based on RowNo length.
    max_depth = 20 # A safe upper limit for hierarchy depth to prevent infinite loops.
    for i in range(2, max_depth + 1):
        previous_level_key = f'level{i-1}_Key'
        
        if previous_level_key not in paths.columns or paths.filter(col(previous_level_key).isNotNull()).count() == 0:
            break

        current_level_name = f'level{i}'
        
        child_nodes = nodes_with_levels.alias('child_nodes')
        edges = schedule_edges.alias('edges')

        paths = paths.join(
            edges,
            (col(previous_level_key) == col('edges.parent_rowNo')) & (paths['accountScheduleName'] == col('edges.accountScheduleName')),
            'left'
        ).join(
            child_nodes,
            (col('edges.child_rowNo') == col('child_nodes.rowNo')) & (paths['accountScheduleName'] == col('child_nodes.accountScheduleName')),
            'left'
        ).select(
            paths['*'],
            col('child_nodes.rowNo').alias(f'{current_level_name}_Key'),
            col('child_nodes.name').alias(f'{current_level_name}_Name')
        ).distinct()

    all_schedules_paths.append(paths)

if not all_schedules_paths:
    print("⚠️ No schedules were processed or all were empty. Writing an empty fixed-schema table.")
    spark.createDataFrame([], FM_Utility.account_schedule_schema(10)) \
        .write.mode('overwrite').option('overwriteSchema', 'True').saveAsTable(target_table)
    notebookutils.notebook.exit("No account schedule data - wrote empty table")

non_hierarchical_combined = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), all_schedules_paths)

all_level_key_cols = sorted([c for c in non_hierarchical_combined.columns if c.startswith('level') and c.endswith('_Key')])
non_hierarchical = non_hierarchical_combined.withColumn('leaf_rowNo', expr(f"coalesce({', '.join(reversed(all_level_key_cols))})"))

if DataCheck:
    print("\n--- DataCheck: Combined Hierarchy Paths from all Schedules ---")
    print(f"Count: {non_hierarchical.count()}")
    non_hierarchical.show(5, truncate=False)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#  ## Part 2: Attach GL Accounts to the Hierarchy

# CELL ********************

print("Parsing leaf node totaling rules for GL accounts...")
leaf_definitions = base_hierarchy_nodes.filter(
    col('totalingType') == 'Posting Accounts'
).select(
    'accountScheduleName',
    col('rowNo').alias('leaf_rowNo'),
    explode(split(col('totaling'), '\|')).alias('totaling_part')
)

#Cast start and end of ranges to LongType for correct numeric comparison.
leaf_ranges = leaf_definitions.withColumn(
    'start_no', trim(split(col('totaling_part'), '\.\.').getItem(0)).cast(LongType())
).withColumn(
    'end_no',
    when(col('totaling_part').contains('..'), trim(split(col('totaling_part'), '\.\.').getItem(1)))
    .otherwise(trim(split(col('totaling_part'), '\.\.').getItem(0)))
    .cast(LongType())
).select('accountScheduleName', 'leaf_rowNo', 'start_no', 'end_no')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Joining hierarchy with GL accounts...")
hierarchy_with_ranges = non_hierarchical.join(leaf_ranges, ['accountScheduleName', 'leaf_rowNo'], 'inner')

# This join now uses numeric comparison, which is correct.
final_hierarchy = hierarchy_with_ranges.join(
    accounts_cleaned, expr("accountKey >= start_no AND accountKey <= end_no"), 'inner'
).drop('start_no', 'end_no', 'leaf_rowNo')

if DataCheck:
    print("--- DataCheck: Final Hierarchy with GL Accounts ---")
    print(f"Count: {final_hierarchy.count()}")
    final_hierarchy.show(5, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 3: Final Formatting and Write to Delta

# CELL ********************

print("Formatting final output...")
if final_hierarchy.count() > 0:
    # Create a persistent column for the final account key before coalescing.
    # Use the text account No (zero-padded, e.g. "07740"), NOT the numeric accountKey,
    # so leaf_account_key matches Account.accountScheduleKey in the Power BI join.
    final_hierarchy_with_leaf_key = final_hierarchy.withColumn("leaf_account_key", col("accountKeyStr"))

    # This logic implements the "coalesce up" behavior from the original script.
    level_key_cols = sorted([c for c in final_hierarchy_with_leaf_key.columns if c.startswith('level') and c.endswith('_Key')])
    level_name_cols = sorted([c for c in final_hierarchy_with_leaf_key.columns if c.startswith('level') and c.endswith('_Name')])
    
    account_level_num = len(level_key_cols) + 1
    
    temp_df = final_hierarchy_with_leaf_key.withColumn(f'level{account_level_num}_Key', lit(None).cast(StringType())) \
                                           .withColumn(f'level{account_level_num}_Name', lit(None).cast(StringType()))
                             
    all_level_keys = sorted([c for c in temp_df.columns if c.startswith('level') and c.endswith('_Key')])
    all_level_names = sorted([c for c in temp_df.columns if c.startswith('level') and c.endswith('_Name')])

    for i in range(len(all_level_names) - 2, -1, -1):
        current_level_name = all_level_names[i]
        next_level_name = all_level_names[i+1]
        current_level_key = all_level_keys[i]
        next_level_key = all_level_keys[i+1]

        temp_df = temp_df.withColumn(
            next_level_name,
            when((col(next_level_name).isNull()) & (col(current_level_name).isNotNull()), col('accountName'))
            .otherwise(col(next_level_name))
        )
        temp_df = temp_df.withColumn(
            next_level_key,
            when((col(next_level_key).isNull()) & (col(current_level_key).isNotNull()), col('accountKeyStr'))
            .otherwise(col(next_level_key))
        )
        
    # Drop the original, temporary account columns, but keep the new persistent leaf_account_key.
    final_df = temp_df.drop('accountKey', 'accountKeyStr', 'accountName')
    # Pad to the fixed level1..level10 contract; also casts leaf_account_key to string.
    final_df = FM_Utility.pad_account_schedule_levels(final_df, 10)

else:
    print("Source data was empty, writing an empty fixed-schema table.")
    final_df = spark.createDataFrame([], FM_Utility.account_schedule_schema(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Writing final data to {target_table}...")
final_df.write.mode('overwrite').option('overwriteSchema', 'True').saveAsTable(target_table)

FM_Utility.commit_watermark(NOTEBOOK_KEY, _fp)
print("Script finished successfully.")
base_hierarchy_nodes.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 4: Validation Against Original Table

# CELL ********************

# This section compares the output of this script with an existing table
# to validate the results. It can be disabled by setting DataCheck to False.

# In[11]:
if DataCheck:
    print("\n\n--- Starting Validation Against Original Table ---")
    
    # Specify the name of the original table to compare against.
    original_table_name = "Enr.enr_accountSchedule"
    
    if spark.catalog.tableExists(original_table_name) and spark.catalog.tableExists(target_table):
        # Load the newly created table and the original one
        new_df = spark.read.table(target_table)
        original_df = spark.read.table(original_table_name)
        
        print(f"Comparing new table '{target_table}' ({new_df.count()} rows) with original table '{original_table_name}' ({original_df.count()} rows).")
        
        # FIX: Standardize column names to handle inconsistencies like 'level1Key' vs 'level1_Key'.
        def standardize_col_name(c):
            # Converts 'level1Key' to 'level1_Key' and 'level1Name' to 'level1_Name'
            return re.sub(r'(level[0-9]+)(Key|Name)', r'\1_\2', c)

        # Apply standardization to both dataframes
        new_df_std = new_df
        for c in new_df.columns:
            new_df_std = new_df_std.withColumnRenamed(c, standardize_col_name(c))
            
        original_df_std = original_df
        for c in original_df.columns:
            original_df_std = original_df_std.withColumnRenamed(c, standardize_col_name(c))

        # 1. Schema Comparison (on standardized names)
        print("\n--- 1. Schema Comparison ---")
        new_cols = set(new_df_std.columns)
        original_cols = set(original_df_std.columns)
        
        cols_only_in_new = new_cols - original_cols
        cols_only_in_original = original_cols - new_cols
        
        if not cols_only_in_new and not cols_only_in_original:
            print("Standardized schemas are identical.")
        else:
            if cols_only_in_new:
                print(f"Columns found only in the new table: {sorted(list(cols_only_in_new))}")
            if cols_only_in_original:
                print(f"Columns found only in the original table (missing from new): {sorted(list(cols_only_in_original))}")

        # 2. Data Comparison (on standardized names)
        print("\n--- 2. Data Comparison ---")
        common_cols = sorted(list(new_cols.intersection(original_cols)))
        
        # Create a primary key to join the two dataframes.
        key_cols = sorted([c for c in common_cols if c.startswith('level') and c.endswith('_Key')])
        # Add the new leaf_account_key to the join keys for more precise matching.
        if 'leaf_account_key' in common_cols:
            key_cols.append('leaf_account_key')
        
        if not key_cols:
            print("Could not find key columns to perform a data comparison. Skipping.")
        else:
            print(f"Using key columns for comparison: {key_cols}")
            
            # Add suffixes to differentiate columns from the two dataframes after the join
            new_df_aliased = new_df_std.select(common_cols)
            for c in common_cols:
                new_df_aliased = new_df_aliased.withColumnRenamed(c, f"{c}_new")
                
            original_df_aliased = original_df_std.select(common_cols)
            for c in common_cols:
                original_df_aliased = original_df_aliased.withColumnRenamed(c, f"{c}_orig")
            
            # Create join keys with aliased names
            join_expr = [col(f"{c}_new") == col(f"{c}_orig") for c in key_cols]
            
            # Perform a full outer join to find rows that are in one table but not the other
            comparison_df = new_df_aliased.join(original_df_aliased, reduce(lambda a, b: a & b, join_expr), "full_outer")
            
            # Find rows only in the new table
            rows_only_in_new = comparison_df.filter(col(f"{key_cols[0]}_orig").isNull())
            if rows_only_in_new.count() > 0:
                print(f"\nFound {rows_only_in_new.count()} rows only in the new table. Sample:")
                rows_only_in_new.select([c for c in comparison_df.columns if c.endswith('_new')]).show(5, truncate=False)

            # Find rows only in the original table
            rows_only_in_original = comparison_df.filter(col(f"{key_cols[0]}_new").isNull())
            if rows_only_in_original.count() > 0:
                print(f"\nFound {rows_only_in_original.count()} rows only in the original table. Sample:")
                rows_only_in_original.select([c for c in comparison_df.columns if c.endswith('_orig')]).show(5, truncate=False)
                
            # Find rows with data mismatches
            print("\nChecking for data mismatches in common rows...")
            mismatch_conditions = []
            compare_cols = [c for c in common_cols if c not in key_cols] # Columns to check for differences
            for c in compare_cols:
                mismatch_conditions.append(
                    (col(f"{c}_new") != col(f"{c}_orig")) | \
                    (col(f"{c}_new").isNull() & col(f"{c}_orig").isNotNull()) | \
                    (col(f"{c}_new").isNotNull() & col(f"{c}_orig").isNull())
                )
            
            if mismatch_conditions:
                mismatched_rows = comparison_df.filter(reduce(lambda a, b: a | b, mismatch_conditions))
                if mismatched_rows.count() > 0:
                    print(f"Found {mismatched_rows.count()} rows with data mismatches. Sample:")
                    mismatched_rows.select(
                        [f"{c}_new" for c in key_cols] + \
                        [item for sublist in [[f"{c}_new", f"{c}_orig"] for c in compare_cols] for item in sublist]
                    ).show(5, truncate=False)
                else:
                    print("No data mismatches found in common rows.")
            else:
                print("No data columns to compare for mismatches.")

    else:
        print(f"Validation skipped: One or both tables do not exist ('{target_table}', '{original_table_name}').")

    print("\n--- Validation Finished ---")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
