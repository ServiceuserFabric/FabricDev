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

# ## Curated tables for PBI — Finance Module
#
# Materialises the Finance curated layer as Delta tables in the **Cur** lakehouse.
# Unnecessary columns are dropped and column names are set to business-friendly names.
#
# These tables replace the old `[pbi].[...]` T-SQL views. The semantic model is
# Direct Lake on OneLake, which reads Delta tables directly and cannot read SQL views.
#
# **Naming.** Cur is not schema-enabled, so tables land in `dbo` and table names
# cannot contain spaces — `Finance Transactions` becomes `Finance_Transactions`.
# Column names are kept **verbatim** from the old views, so the semantic model's
# `sourceColumn` entries are unchanged. Spaces in column names require Delta
# column mapping in `name` mode, which both Direct Lake and the SQL analytics
# endpoint support.

# CELL ********************

# Column mapping must be enabled for the column names below, which contain spaces
# and '&'. Setting it as a session default applies it to every table created here.
spark.conf.set("spark.databricks.delta.properties.defaults.columnMapping.mode", "name")
spark.conf.set("spark.databricks.delta.properties.defaults.minReaderVersion", "2")
spark.conf.set("spark.databricks.delta.properties.defaults.minWriterVersion", "5")

COLUMN_MAPPING_PROPERTIES = {
    "delta.columnMapping.mode": "name",
    "delta.minReaderVersion": "2",
    "delta.minWriterVersion": "5",
}


def materialize(table_name, sql):
    """Idempotent full overwrite of Cur.<table_name> from a Spark SQL projection."""
    target = f"Cur.{table_name}"

    # A table created by an earlier run without column mapping would reject the
    # write, so upgrade it in place first. New tables inherit the session defaults.
    if spark.catalog.tableExists(target):
        props = ", ".join(f"'{k}' = '{v}'" for k, v in COLUMN_MAPPING_PROPERTIES.items())
        spark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES ({props})")

    df = spark.sql(sql)
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target))

    print(f"{target}: {spark.table(target).count():,} rows, {len(df.columns)} columns")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

materialize("Finance_Transactions", """
SELECT
    accountKey                          AS `AccountKey`,
    genBusinessPostingGroupKey          AS `GenBusinessPostingGroupKey`,
    genProductPostingGroupKey           AS `GenProductPostingGroupKey`,
    CAST('-1' AS STRING)                AS `ContactKey`,
    companyKey                          AS `CompanyKey`,


    --Created Enriched Date Missing
    --Ledger Entry No Missing

    companyCurrencyCode                 AS `Currency Code LCY`,
    amountReportingLCY                  AS `Finance Amount LCY`,
    amountReportingGCY                  AS `Finance Amount GCY`,
    --`P&L/Balance`                     AS `P&L/Balance`,
    description                         AS `Finance Description`,
    documentNo                          AS `Finance Document Number`,
    documentType                        AS `Finance Document Type`,

    -- Dates
    documentDate                        AS `Document Date`,
    CAST(postingDate AS DATE)           AS `PostingDateKey`,

    sourceNo                            AS `Finance Source Number`,
    sourceType                          AS `Finance Source Type`,
    sourceCode                          AS `Finance Source Code`,
    sourceSystem                        AS `Data Source System`,


    -- Custom Dimensions -- Modify by customer.
    dse_AfdelingKey                     AS `dse_AfdelingKey`,
    dse_CustomerContractKey             AS `dse_CustomerContractKey`,
    dse_NetProviderKey                  AS `dse_NetProviderKey`

    -- CAST('-1' AS STRING)             AS `subVendorKey`
FROM Enr.enr_glentries
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

materialize("Finance_Budgets", """
SELECT
    accountKey                          AS `AccountKey`,

    `date`                              AS `Date`,

    budgetName                          AS `Budget Name`,
    account                             AS `Account Number`,
    dataSource                          AS `Budget Data Source`,
    amountReportingLCY                  AS `Budget Amount`,
    amountReportingGCY                  AS `Budget Amount GCY`,
    companyKey                          AS `CompanyKey`,
    --amountReportingVAT                AS `Amount Incl. VAT`,
    --CASE WHEN account < 60000 THEN 'P&L' ELSE 'Balance' END AS `P&L/Balance`,

    -- Custom Dimensions -- Modify by customer.
    dse_AfdelingKey                     AS `dse_AfdelingKey`,
    dse_CustomerContractKey             AS `dse_CustomerContractKey`,
    dse_NetProviderKey                  AS `dse_NetProviderKey`
FROM Enr.enr_budget
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

materialize("Account", """
SELECT
    accountKey                          AS `AccountKey`,
    accountScheduleKey                  AS `AccountScheduleKey`,

    accountNumberAndName                AS `Account Number And Name`,
    accountNo                           AS `Account Number`,
    name                                AS `Account Name`,

    accountCategory                     AS `Account Category`,
    `P&LSignSwitch`                     AS `Account P&L Sign Switch`,
    accountType                         AS `Account Type`,
    incomeBalance                       AS `Account Statement Type`

    -- CompensationAccount              AS `CompensationAccount`
FROM Enr.enr_glaccount
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Two interchangeable sources of the P&L / Balance hierarchy, unioned:
#   * enr_accountschedule       — driven by BC Account Schedule / Financial Report lines
#   * enr_glaccounthierarchy    — built directly from the GL Account chart of accounts
# The model always filters to ONE `Account Schedule Name`, so the union is semantically safe.
# leaf_account_key is CAST to string on BOTH branches: BC account No is Code[20] and may be
# non-numeric (e.g. 'Z5431'), so without the cast UNION ALL type resolution would try to widen
# it to a numeric type and fail. Levels extended to level1..level10 (fixed schema contract).
materialize("Account_Schedule", """
SELECT
    CAST(leaf_account_key AS STRING)    AS `AccountScheduleKey`,
    CAST(leaf_account_key AS STRING)    AS `Account Number`,

    accountScheduleName                 AS `Account Schedule Name`,
    level1_Key                          AS `Level1Key`,
    level2_Key                          AS `Level2Key`,
    level3_Key                          AS `Level3Key`,
    level4_Key                          AS `Level4Key`,
    level5_Key                          AS `Level5Key`,
    level6_Key                          AS `Level6Key`,
    level7_Key                          AS `Level7Key`,
    level8_Key                          AS `Level8Key`,
    level9_Key                          AS `Level9Key`,
    level10_Key                         AS `Level10Key`,
    level1_Name                         AS `Account Schedule - Level 1`,
    level2_Name                         AS `Account Schedule - Level 2`,
    level3_Name                         AS `Account Schedule - Level 3`,
    level4_Name                         AS `Account Schedule - Level 4`,
    level5_Name                         AS `Account Schedule - Level 5`,
    level6_Name                         AS `Account Schedule - Level 6`,
    level7_Name                         AS `Account Schedule - Level 7`,
    level8_Name                         AS `Account Schedule - Level 8`,
    level9_Name                         AS `Account Schedule - Level 9`,
    level10_Name                        AS `Account Schedule - Level 10`,
    CAST(-1 AS INT)                     AS `row_id`, --deprecated not in source
    fullAccountName                     AS `Account Name`,
    incomeBalance                       AS `Account Statement Type`
FROM Enr.enr_accountschedule
UNION ALL
SELECT
    CAST(leaf_account_key AS STRING)    AS `AccountScheduleKey`,
    CAST(leaf_account_key AS STRING)    AS `Account Number`,

    accountScheduleName                 AS `Account Schedule Name`,
    level1_Key                          AS `Level1Key`,
    level2_Key                          AS `Level2Key`,
    level3_Key                          AS `Level3Key`,
    level4_Key                          AS `Level4Key`,
    level5_Key                          AS `Level5Key`,
    level6_Key                          AS `Level6Key`,
    level7_Key                          AS `Level7Key`,
    level8_Key                          AS `Level8Key`,
    level9_Key                          AS `Level9Key`,
    level10_Key                         AS `Level10Key`,
    level1_Name                         AS `Account Schedule - Level 1`,
    level2_Name                         AS `Account Schedule - Level 2`,
    level3_Name                         AS `Account Schedule - Level 3`,
    level4_Name                         AS `Account Schedule - Level 4`,
    level5_Name                         AS `Account Schedule - Level 5`,
    level6_Name                         AS `Account Schedule - Level 6`,
    level7_Name                         AS `Account Schedule - Level 7`,
    level8_Name                         AS `Account Schedule - Level 8`,
    level9_Name                         AS `Account Schedule - Level 9`,
    level10_Name                        AS `Account Schedule - Level 10`,
    CAST(-1 AS INT)                     AS `row_id`, --deprecated not in source
    fullAccountName                     AS `Account Name`,
    incomeBalance                       AS `Account Statement Type`
FROM Enr.enr_glaccounthierarchy
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
