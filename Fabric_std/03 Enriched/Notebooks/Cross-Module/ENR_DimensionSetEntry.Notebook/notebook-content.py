# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9376fc45-1e74-4890-acb2-f16a86c266c5",
# META       "default_lakehouse_name": "DP",
# META       "default_lakehouse_workspace_id": "3a25df0d-986f-45a8-9140-8e9d88526e86",
# META       "known_lakehouses": [
# META         {
# META           "id": "9376fc45-1e74-4890-acb2-f16a86c266c5"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Enriched entity: Dimensionset entry
# 
# ### Dependency:
# - dp_DimensionSetEntry
# 
# Reads tables from dp and saves to Enr, mode: full overwrite  <br>
# 1. Unions dimension set entry table from BC and form Nav(hand made)
# 2. Writes to ENR, mode full load

# CELL ********************

%run FM_Utility

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load

# PARAMETERS CELL ********************

target_table = "Enr.enr_dimensionsetentry"

dse = spark.read.table('DP.dp_dimensionsetentry')
dimension_names = GlobalParameters.dimension_mapping

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

found_dims = []
for dim_name in dimension_names:
    cur_dim_name = 'dse_'+dim_name[2].lower()+"Key"
    code_target = 'dse_'+dim_name[2]+"Code"
    key_target = 'dse_'+dim_name[2]+"Key"
    if cur_dim_name in dse.schema.names:
        found_dims.append(col(cur_dim_name).alias(code_target))
        found_dims.append(
            when(
                col(cur_dim_name) != lit('-1'),
                    concat_ws('_',col('source_system'),col('companyKey'),col(cur_dim_name))
            )
                .otherwise(lit('-1'))
                .alias(key_target)
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Transform data (Standard)

# CELL ********************

result = dse.select(
    col("companyKey"),
    *found_dims,
    col('dimensionSetEntryKey'),
    col('source_system')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# check duplicates 
DataCheck.check_duplicates(result,'dimensionSetEntryKey')           

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### save

# CELL ********************

result.write.mode("overwrite").option("overwriteSchema","true").format("delta").saveAsTable(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
