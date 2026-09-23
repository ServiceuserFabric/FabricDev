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

# ## Curated tables for PBI — Shared Dimensions
# # Tables shared across all modules. Always kept on deployment.
# Includes common dimensions such as calendar, company, item, contacts, posting groups, and custom dimensions.
# # These tables replace the old `[pbi].[...]` T-SQL views. The semantic model is
# Direct Lake on OneLake, which reads Delta tables directly and cannot read SQL views.
# # **Naming.** Cur is not schema-enabled, so tables land in `dbo` and table names
# cannot contain spaces — `Salesperson Purchaser` becomes `Salesperson_Purchaser`.
# Column names are kept **verbatim** from the old views, so the semantic model's
# `sourceColumn` entries are unchanged. Spaces in column names require Delta
# column mapping in `name` mode, which both Direct Lake and the SQL analytics
# endpoint support.

# CELL ********************

# Column mapping must be enabled for the column names below, which contain spaces
# and '.'. Setting it as a session default applies it to every table created here.
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

# MARKDOWN ********************

# ### Calendar

# CELL ********************

# Every column below is written by ENR_Calendar (enr_calendar). This is a straight
# rename-projection — nothing is recomputed here. The old CUR_Calendar notebook,
# which recomputed WeekLabel / DayLabel / currentMonth / currentYear / last12Months /
# upToCurrentMonth against a hardcoded reporting period, is superseded by this cell.
materialize("Calendar", """
SELECT
    `Date`                              AS `CalendarKey`,
    `Date`,
    PreviousDay                         AS `Previous Day`,
    NextDay                             AS `Next Day`,
    year                                AS `Year`,
    IsoYear                             AS `Iso Year`,
    IsLeapYear                          AS `Is Leap Year`,
    Semester                            AS `Semester`,
    SemesterCaption                     AS `Semester Caption`,
    YearSemesterCaption                 AS `Year Semester Caption`,
    Quarter                             AS `Quarter`,
    QuarterCaption                      AS `Quarter Caption`,
    YearQuarterCaption                  AS `Year Quarter Caption`,
    FirstDayOfQuarter                   AS `First Day Of Quarter`,
    LastDayOfQuarter                    AS `Last Day of Quarter`,
    Month                               AS `Month`,
    MonthName                           AS `Month Name`,
    MonthAbbr                           AS `Month Abbr.`,
    FirstDayOfMonth                     AS `First Day of Month`,
    LastDayOfMonth                      AS `Last Day of Month`,
    YearMonthCaption                    AS `Year Month Caption`,
    Week                                AS `Week`,
    YearWeekCaption                     AS `Year Week Caption`,
    IsoWeek                             AS `Iso Week`,
    IsoYearWeekCaption                  AS `Iso Year Week Caption`,
    DayOfYear                           AS `Day of Year`,
    DayOfMonth                          AS `Day of Month`,
    DayOfWeek                           AS `Day of Week`,
    IsoWeekday                          AS `Iso Week Day`,
    WeekdayName                         AS `Weekday Name`,
    IsWeekend                           AS `Is Weekend`,
    WeekLabel                           AS `Week Label`,
    MonthLabel                          AS `Month Label`,
    DayLabel                            AS `Day Label`,
    currentMonth                        AS `Current Month`,
    currentYear                         AS `Current Year`,
    last12Months                        AS `Last 12 Months`,
    upToCurrentMonth                    AS `Up To Current Month`,
    yearmonthdaynum                     AS `Year Month day Number`,
    RelativeMonth                       AS `Relative Month`
FROM Enr.enr_calendar
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Item

# CELL ********************

materialize("Item", """
SELECT
    -- companyKey,
    itemNumber                          AS `Item Number`,
    description                         AS `Item Description`,
    description2                        AS `Item Description 2`,
    inventoryPostingGroup               AS `Item Inventory Posting Group`,
    itemCategoryCode                    AS `Item Category Code`,
    -- lastDateModified                 AS `Last Date Modified`,
    lastDirectCost                      AS `Item Last Direct Cost`,
    itemUnitCost                        AS `Item Unit Cost`,
    unitPrice                           AS `Item Unit Price`,
    ItemCodeAndDescription              AS `Item Code And Description`,
    -- ItemProductCodeAndDescription    AS `Item ProductCode And Description`,
    itemKey                             AS `ItemKey`
FROM Enr.enr_items
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Company

# CELL ********************

materialize("Company", """
SELECT
    companyKey                          AS `CompanyKey`,
    companyCurrency                     AS `Company Currency`,
    companyName                         AS `Company Name`,
    companyCountry                      AS `Company Country`
FROM Enr.enr_company
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Contact

# CELL ********************

materialize("Contact", """
SELECT
    contactNumber                       AS `Contact Number`,
    address                             AS `Contact Address`,
    address2                            AS `Contact Address 2`,
    countryRegionCode                   AS `Contact Country/Region Code`,
    icPartnerCode                       AS `Contact IC Partner Code`,
    name                                AS `Contact Name`,
    paymentMethodCode                   AS `Contact Payment Method Code`,
    paymentTermsCode                    AS `Contact Payment Terms Code`,
    paymentTermsId                      AS `Contact Payment Terms ID`,
    salespersonCode                     AS `Contact Default Salesperson`,
    purchaserCode                       AS `Contact Default Purchaser Code`,
    contactKey                          AS `ContactKey`
FROM Enr.enr_contact
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Customer

# CELL ********************

materialize("Customer", """
SELECT
    -- Keys
    customerKey                         AS `CustomerKey`,

    -- Descriptive fields
    `no`                                AS `Customer Number`,
    name                                AS `Customer Name`,
    customerCodeandName                 AS `Customer Code and Name`,
    address                             AS `Customer Address`,
    address2                            AS `Customer Address 2`,
    city                                AS `Customer City`,
    countryRegionCode                   AS `Customer Country`,
    paymentTermsCode                    AS `Customer Payment Terms`,
    paymentMethodCode                   AS `Customer Payment Method`,
    salespersonCode                     AS `Customer Default Salesperson`,
    email                               AS `Customer Email`
FROM Enr.enr_customer
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Salesperson / Purchaser

# CELL ********************

materialize("Salesperson_Purchaser", """
SELECT
    -- Keys
    salespersonPurchaserKey             AS `SalespersonPurchaserKey`,

    -- Descriptive fields
    code                                AS `Salesperson Purchaser Code`,
    name                                AS `Salesperson Purchaser Name`,
    email                               AS `Salesperson Purchaser Email`
FROM Enr.enr_salespersonpurchaser
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Posting Groups

# CELL ********************

materialize("Gen_Business_Posting_Group", """
SELECT
    genBusinessPostingGroupKey          AS `GenBusinessPostingGroupKey`,
    genBusinessPostingGroupsCode        AS `Gen Business Posting Group Code`,
    genBusinessPostingGroupsDescription AS `Gen Business Posting Group Description`
FROM Enr.enr_genbusinesspostinggroups
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

materialize("Gen_Product_Posting_Group", """
SELECT
    genProductPostingGroupKey           AS `GenProductPostingGroupKey`,
    genProductPostingGroupsCode         AS `Gen Product Posting Group Code`,
    genProductPostingGroupsDescription  AS `Gen Product Posting Group Description`
FROM Enr.enr_genproductpostinggroups
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Custom Dimensions

# CELL ********************

materialize("dse_Afdeling", """
SELECT
    dse_AfdelingCode                        AS `Afdeling Code`,
    dse_AfdelingName                        AS `Afdeling Name`,
    dse_AfdelingKey                         AS `dse_AfdelingKey`,
    dse_AfdelingCode_And_Name               AS `Afdeling Code And Name`,
    dse_AfdelingCode_And_Name_Reporting     AS `Afdeling Code And Name Reporting`
FROM Enr.enr_dse_afdeling
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

materialize("dse_CustomerContract", """
SELECT
    dse_CustomerContractCode                    AS `CustomerContract Code`,
    dse_CustomerContractName                    AS `CustomerContract Name`,
    dse_CustomerContractKey                     AS `dse_CustomerContractKey`,
    dse_CustomerContractCode_And_Name           AS `CustomerContract Code And Name`,
    dse_CustomerContractCode_And_Name_Reporting AS `CustomerContract Code And Name Reporting`
FROM Enr.enr_dse_customercontract
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

materialize("dse_NetProvider", """
SELECT
    dse_NetProviderCode                     AS `NetProvider Code`,
    dse_NetProviderName                     AS `NetProvider Name`,
    dse_NetProviderKey                      AS `dse_NetProviderKey`,
    dse_NetProviderCode_And_Name            AS `NetProvider Code And Name`,
    dse_NetProviderCode_And_Name_Reporting  AS `NetProvider Code And Name Reporting`
FROM Enr.enr_dse_netprovider
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
