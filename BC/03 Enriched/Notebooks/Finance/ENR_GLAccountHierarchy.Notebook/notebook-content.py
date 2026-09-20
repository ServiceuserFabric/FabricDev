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

# # ENR_GLAccountHierarchy
#
# ## Purpose
# Builds a P&L and Balance hierarchy **directly from the GL Account chart of accounts**, as an
# alternative to `ENR_AccountSchedule` (which is driven by the Account Schedule / Financial Report
# definition). Use this when you want the Power BI P&L / Balance hierarchy to match the GL Account setup.
#
# It produces the **same output schema** as `ENR_AccountSchedule`
# (`accountScheduleName`, `level{n}_Key` / `level{n}_Name`, `leaf_account_key`,
# `incomeBalance`, `fullAccountName`) so it is a drop-in source for the same downstream model.
#
# ## Hybrid method — auto-selected per statement
# BC charts encode their structure in two parallel, client-maintained ways, and which one carries the
# hierarchy differs between the Balance Sheet and the Income Statement (and between clients):
#
#  1. **Indentation** (`Begin-Total` / `End-Total` brackets). Balance Sheets are usually richly
#     bracketed; the nesting comes from BC's `Indentation` field.
#  2. **`Total` accounts + `Totaling` ranges.** Income Statements are often *flat* (every line at
#     indentation 0) and express their structure through running-total ranges
#     (e.g. Dækningsbidrag = 10100..37000 nested inside Bruttoresultat = 10100..51800).
#
# This notebook computes both and **chooses per `incomeBalance`**:
#  - If a statement is meaningfully indented (posting paths reach depth >= `INDENT_MIN_DEPTH`)
#    -> use the **indentation** method (ancestors = enclosing `Begin-Total` headers).
#  - Otherwise (flat statement) -> use **Totaling-range containment** (ancestors = the containing
#    `Total` / `End-Total` accounts, ordered widest range -> narrowest).
#
# `accountScheduleName` is set to `incomeBalance` ("Income Statement" / "Balance Sheet"), mirroring the
# two PBI schedules in ENR_AccountSchedule.
#
# Why not G/L Account Categories? They are the Microsoft-standard mechanism, but outside the US BC does
# not pre-map them, so they are commonly unpopulated (especially on the Balance Sheet). Indentation +
# Totaling are derived from the chart structure itself, which every client maintains, so this approach
# works out of the box without extra tables or manual category setup.
#
# ## Notes / limitations
#  - Reads **raw `GLAccount`** (not DP). `Indentation` (BC field 19) is used when present; if the column
#    is absent, every statement falls back to the Totaling-range method.
#  - Indentation correctness depends on account-`No` ordering (BC's Chart-of-Accounts order); the chart
#    is read sorted by `No`.
#  - Treated as a single, company-agnostic chart (one canonical company) — matches ENR_AccountSchedule
#    and the DP note "We only use BC accounts so that we only have one set of account hierarchy".
#    Set `hierarchy_company` to pin a specific company.
#  - `leaf_account_key` is the account No as text, so non-numeric accounts (Code[20], e.g. "Z5431")
#    are fully supported. NOTE: the Totaling-range method is inherently numeric, so a non-numeric
#    account in a *flat* statement cannot be range-placed (it lands at the top level); in an *indented*
#    statement it is placed correctly by indentation. Downstream, the Power BI model's join column for
#    this key must be text (not Int64) to consume non-numeric accounts end-to-end.

# MARKDOWN ********************

#  ## Libraries

# CELL ********************

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StringType, LongType, StructType, StructField
from collections import defaultdict
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

target_table = 'Enr.enr_glAccountHierarchy'

# Pin a specific company's chart of accounts. None -> auto-pick the company with the most GL accounts.
hierarchy_company = None

# A statement is treated as "indented" (use the indentation method) when its posting paths reach at
# least this depth; otherwise it is treated as flat and the Totaling-range method is used.
INDENT_MIN_DEPTH = 2

# Force a rebuild even when the source fingerprint is unchanged (schema change / manual rebuild).
force_refresh = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Loading and Preparation
#
# Read the GL Account chart **from the Raw lakehouse** (not DP), exactly as ENR_AccountSchedule reads
# its source.

# CELL ********************

%run FM_Utility

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Loading GL Account chart of accounts from Raw...")
account_bc = FM_Utility.load_cleaned_dataframe('Raw', 'GLAccount', 'camel')

# --- Change-detection self-skip (slow-changing dimension) ---
# Fingerprint the raw source (count + max rowversion). If unchanged since the last successful
# build and not force_refresh, skip the rebuild — the existing table stays intact and any
# dependents still see success (runMultiple treats notebook.exit as success).
NOTEBOOK_KEY = "ENR_GLAccountHierarchy"
_fp = FM_Utility.source_fingerprint([account_bc])
if not force_refresh and not FM_Utility.has_changed(NOTEBOOK_KEY, _fp):
    notebookutils.notebook.exit("skipped — no source change")

has_indentation = 'indentation' in account_bc.columns
if not has_indentation:
    print("NOTE: no 'indentation' column in Raw GLAccount - every statement will use the Totaling-range method.")

select_cols = [
    col('company').cast('string').alias('company'),
    col('no').cast('string').alias('no'),
    col('name').cast('string').alias('name'),
    col('accountType').cast('string').alias('accountType'),
    col('incomeBalance').cast('string').alias('incomeBalance'),
    col('totaling').cast('string').alias('totaling'),
]
select_cols.append(
    col('indentation').cast(LongType()).alias('indentation') if has_indentation
    else lit(0).cast(LongType()).alias('indentation')
)
chart = account_bc.select(*select_cols).filter(col('no').isNotNull())

# Pick the canonical company so the chart is internally consistent for No-ordering / indentation.
if hierarchy_company is None:
    company_counts = chart.groupBy('company').count().orderBy(col('count').desc(), col('company').asc())
    hierarchy_company = company_counts.first()['company']
    print(f"Auto-selected canonical company: {hierarchy_company}")
else:
    print(f"Using pinned company: {hierarchy_company}")

chart = chart.filter(col('company') == lit(hierarchy_company)) \
             .select('no', 'name', 'accountType', 'incomeBalance', 'totaling', 'indentation') \
             .dropDuplicates(['no'])

if DataCheck:
    print("--- DataCheck: chart of accounts loaded ---")
    print(f"Distinct accounts in canonical chart: {chart.count()}")
    chart.groupBy('accountType').count().show(truncate=False)
    chart.groupBy('incomeBalance').count().show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 1: Resolve each posting's ancestor path (hybrid)
#
# Done on the driver because the chart of accounts is small metadata and the logic is sequential.
# Step A computes indentation-based paths; Step B classifies each statement; Step C computes
# Totaling-range paths for the flat statements; Step D picks the right path per posting.

# CELL ********************

# `%run FM_Utility` imports pyspark.sql.functions.{max,min,sum,round} by name into this
# notebook's namespace, shadowing the Python builtins. The driver-side logic below uses the
# builtins (max(..., default=0), sum(generator)), so restore them explicitly for this notebook.
from builtins import max, min, sum, round

# Collected once; chart-of-accounts ordering is applied PER STATEMENT in Step A (a single
# global No sort interleaves statements whose number bands/lengths overlap, which leaks
# ancestors across the P&L / Balance trees).
chart_rows = chart.collect()

MAX_CHART_ROWS = 100000  # safety guard against an unexpectedly huge / mis-targeted table
if len(chart_rows) > MAX_CHART_ROWS:
    raise ValueError(f"Chart row count ({len(chart_rows)}) exceeds the driver-side safety limit ({MAX_CHART_ROWS}).")
if len(chart_rows) == 0:
    print("⚠️ No GL accounts found for the selected company. Writing an empty fixed-schema table.")
    spark.createDataFrame([], FM_Utility.account_schedule_schema(10)) \
        .write.mode('overwrite').option('overwriteSchema', 'True').format('delta').saveAsTable(target_table)
    notebookutils.notebook.exit("No GL account hierarchy data - wrote empty table")

# --- Step A: indentation paths (ancestors = enclosing Begin-Total headers) ---
# Built INDEPENDENTLY PER STATEMENT (incomeBalance). The P&L and Balance hierarchies are
# separate trees, so an account must only ever inherit ancestors from its OWN statement. A single
# shared stack lets a Balance-Sheet Begin-Total leak onto an Income-Statement posting (and vice
# versa) whenever the two statements' account-No ranges interleave (e.g. a 5-digit Balance account
# sorting among 4-digit P&L accounts) — the original cause of cross-statement contamination.
# Within each statement, accounts are read in account-No TEXT order: that is exactly how BC sorts
# the chart of accounts, and the order the client designed the indentation against, so a non-numeric
# Code[20] No like "18820-NORWAY" still sorts next to its sibling "18820" and nests correctly.
rows_by_statement = defaultdict(list)
for r in chart_rows:
    rows_by_statement[r['incomeBalance']].append(r)

indent_path = {}      # posting no -> [(ancestor_no, ancestor_name), ...]
posting_meta = {}     # posting no -> (name, incomeBalance)

for _ib, _stmt_rows in rows_by_statement.items():
    stack = []        # fresh stack per statement — no cross-statement ancestor leakage
    for r in sorted(_stmt_rows, key=lambda row: str(row['no'])):
        depth = int(r['indentation']) if r['indentation'] is not None else 0
        acc_type = r['accountType']
        while stack and stack[-1][0] >= depth:
            stack.pop()
        if acc_type == 'Posting':
            indent_path[r['no']] = [(a_no, a_name) for (_d, a_no, a_name) in stack]
            posting_meta[r['no']] = (r['name'], r['incomeBalance'])
        if acc_type == 'Begin-Total':
            stack.append((depth, r['no'], r['name']))

# --- Step B: classify each statement as indented or flat ---
stmt_postings = defaultdict(list)
for acc_no, (_name, ib) in posting_meta.items():
    stmt_postings[ib].append(acc_no)
stmt_max_depth = {ib: max((len(indent_path[n]) for n in nos), default=0) for ib, nos in stmt_postings.items()}
indented_stmts = {ib for ib, d in stmt_max_depth.items() if d >= INDENT_MIN_DEPTH}

# --- Step C: Totaling-range branches per statement (for the flat statements) ---
def parse_ranges(totaling):
    """Parse a BC Totaling string into a list of numeric (start, end) ranges; non-numeric parts skipped."""
    ranges = []
    if totaling is None:
        return ranges
    for part in str(totaling).split('|'):
        part = part.strip()
        lo, hi = (part.split('..', 1) if '..' in part else (part, part))
        try:
            lo, hi = int(lo.strip()), int(hi.strip())
        except ValueError:
            continue
        ranges.append((lo, hi))
    return ranges

branches_by_stmt = defaultdict(list)   # incomeBalance -> [{no, name, ranges, width}, ...]
for r in chart_rows:
    if r['accountType'] in ('Total', 'End-Total'):
        rg = parse_ranges(r['totaling'])
        if rg:
            branches_by_stmt[r['incomeBalance']].append(
                {'no': r['no'], 'name': r['name'], 'ranges': rg,
                 'width': sum(hi - lo + 1 for lo, hi in rg)}
            )

def range_path(ib, acc_no_long):
    """Containing Total/End-Total accounts for an account number, ordered widest range -> narrowest."""
    if acc_no_long is None:
        return []
    conts = [b for b in branches_by_stmt.get(ib, []) if any(lo <= acc_no_long <= hi for lo, hi in b['ranges'])]
    conts.sort(key=lambda b: (-b['width'], b['no']))
    return [(b['no'], b['name']) for b in conts]

# --- Step D: pick the path per posting ---
posting_records = []
for acc_no, (name, ib) in posting_meta.items():
    if ib in indented_stmts:
        path = indent_path[acc_no]
    else:
        try:
            acc_long = int(acc_no)
        except (TypeError, ValueError):
            acc_long = None
        path = range_path(ib, acc_long)
    posting_records.append({'no': acc_no, 'name': name, 'ib': ib, 'path': path})

max_depth = max((len(rec['path']) for rec in posting_records), default=0)

if DataCheck:
    print("--- DataCheck: method chosen per statement ---")
    for ib in sorted(stmt_postings):
        method = 'indentation' if ib in indented_stmts else 'totaling-range'
        final_depth = max((len(r['path']) for r in posting_records if r['ib'] == ib), default=0)
        print(f"   {ib}: {len(stmt_postings[ib])} postings | indentation depth {stmt_max_depth[ib]} "
              f"-> method='{method}' | final path depth {final_depth}")
    print(f"Overall max path depth: {max_depth}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 2: Materialise the leaf paths back into Spark

# CELL ********************

if not posting_records:
    print("⚠️ No posting accounts found in the chart. Writing an empty fixed-schema table.")
    spark.createDataFrame([], FM_Utility.account_schedule_schema(10)) \
        .write.mode('overwrite').option('overwriteSchema', 'True').format('delta').saveAsTable(target_table)
    notebookutils.notebook.exit("No posting accounts - wrote empty table")

# leaf_account_key is the account No as-is (BC account No is Code[20], so it is a string and may be
# non-numeric, e.g. "Z5431" or "18820-NORWAY"). Keeping it as text means non-numeric accounts are
# fully supported rather than dropped by a numeric cast.
fields = [
    StructField('incomeBalance', StringType()),
    StructField('leaf_account_key', StringType()),
    StructField('fullAccountName', StringType()),
    StructField('accountName', StringType()),
]
for i in range(1, max_depth + 1):
    fields.append(StructField(f'level{i}_Key', StringType()))
    fields.append(StructField(f'level{i}_Name', StringType()))
leaf_schema = StructType(fields)

leaf_tuples = []
for rec in posting_records:
    acc_no, name, ib, path = rec['no'], rec['name'], rec['ib'], rec['path']
    row = [ib, acc_no, name, f"{acc_no} - {name}"]
    for i in range(1, max_depth + 1):
        if i <= len(path):
            row.append(str(path[i - 1][0]))  # ancestor key (account No)
            row.append(path[i - 1][1])        # ancestor name
        else:
            row.append(None)
            row.append(None)
    leaf_tuples.append(tuple(row))

leafed = spark.createDataFrame(leaf_tuples, leaf_schema)

if DataCheck:
    print("--- DataCheck: sample leaf paths ---")
    leafed.show(8, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 3: Final formatting (ragged fill-down) and write to Delta
#
# Mirrors the ENR_AccountSchedule output: the leaf account is placed at the level immediately below its
# deepest ancestor, `accountScheduleName` is set to `incomeBalance`, then written to Delta.

# CELL ********************

print("Formatting final output...")
if leafed.count() > 0:
    def _level_idx(c):
        return int(re.findall(r'\d+', c)[0])

    level_key_cols = sorted(
        [c for c in leafed.columns if c.startswith('level') and c.endswith('_Key')], key=_level_idx
    )
    account_level_num = len(level_key_cols) + 1

    temp_df = leafed \
        .withColumn(f'level{account_level_num}_Key', lit(None).cast(StringType())) \
        .withColumn(f'level{account_level_num}_Name', lit(None).cast(StringType()))

    all_level_keys = sorted(
        [c for c in temp_df.columns if c.startswith('level') and c.endswith('_Key')], key=_level_idx
    )
    all_level_names = sorted(
        [c for c in temp_df.columns if c.startswith('level') and c.endswith('_Name')], key=_level_idx
    )

    # Place the leaf account one level below its deepest ancestor (ragged hierarchy fill-down).
    for i in range(len(all_level_names) - 2, -1, -1):
        current_level_name, next_level_name = all_level_names[i], all_level_names[i + 1]
        current_level_key, next_level_key = all_level_keys[i], all_level_keys[i + 1]

        temp_df = temp_df.withColumn(
            next_level_name,
            when((col(next_level_name).isNull()) & (col(current_level_name).isNotNull()), col('accountName'))
            .otherwise(col(next_level_name))
        )
        temp_df = temp_df.withColumn(
            next_level_key,
            when((col(next_level_key).isNull()) & (col(current_level_key).isNotNull()), col('leaf_account_key').cast(StringType()))
            .otherwise(col(next_level_key))
        )

    final_df = temp_df.withColumn('accountScheduleName', col('incomeBalance')).drop('accountName')

    ordered_level_cols = []
    for i in range(1, account_level_num + 1):
        ordered_level_cols += [f'level{i}_Key', f'level{i}_Name']
    final_df = final_df.select(
        ['accountScheduleName'] + ordered_level_cols + ['leaf_account_key', 'incomeBalance', 'fullAccountName']
    )
    # Pad to the fixed level1..level10 contract so the table schema is stable across clients/runs.
    final_df = FM_Utility.pad_account_schedule_levels(final_df, 10)
else:
    print("No leaves to format - writing an empty fixed-schema table.")
    final_df = spark.createDataFrame([], FM_Utility.account_schedule_schema(10))

if DataCheck:
    print(f"--- DataCheck: final rows: {final_df.count()} ---")
    final_df.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Writing final data to {target_table}...")
final_df.write.mode('overwrite').option('overwriteSchema', 'True').format('delta').saveAsTable(target_table)

FM_Utility.commit_watermark(NOTEBOOK_KEY, _fp)
print("Script finished successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
