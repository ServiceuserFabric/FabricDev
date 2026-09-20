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

# # Enriched entity: gl_budget

# MARKDOWN ********************

# Creates and saves Budget


# MARKDOWN ********************

# ##  Libraries

# CELL ********************

%run FM_Utility

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_table = f"Enr.enr_budget"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


if spark.catalog.tableExists(target_table):
   deltaTable = DeltaTable.forName(spark, 'DP.dp_budget')
   lastSourceChange = deltaTable.history(1).select("timestamp").collect()[0][0]
   deltaTable = DeltaTable.forName(spark, target_table)
   lastTargetWrite = deltaTable.history(1).select("timestamp").collect()[0][0]

   if (lastTargetWrite - lastSourceChange).total_seconds() > 300:  #5 minute sync delay so we don't write everytime
      notebookutils.notebook.exit("No updates on budget")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ### Load data

# PARAMETERS CELL ********************



# Needs to be updated to there is a selection going on towards the DP layer. 

#GL entries only contains data from BC. (it's shortcutted to DP layer directly from Raw)
gl_budget = spark.read.format("delta").table("DP.dp_budget")

#Use ENR layer so no duplicates! 
#dim_set = FM_Utility.transform_dimension_set() # Mapped version of dimension set entry
dim_set = spark.read.format("delta").table('Enr.enr_dimensionsetentry')
gl_account = spark.read.format("delta").table("Enr.enr_glaccount")
company = spark.read.format("delta").table("dp.dp_company")
dimension_names = GlobalParameters.dimension_mapping

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform data

# CELL ********************

# Join with dimension set entry using DimensionSetID
gl_budget = (gl_budget.alias("a")
    .join(
        dim_set.alias("b"),
        [
            (col("a.companyKey") == col("b.companyKey")),
            (concat_ws('_', col('a.sourceSystem'), col("a.companyKey"), col("a.dimensionSetID")) == col("b.dimensionSetEntryKey"))
        ],
        "left"
    )
    .select(
        "a.*",
        col("b.dimensionSetEntryKey").alias("dimensionSetEntryKey")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Join dimension entries & accounts to GL
gl_budget = (gl_budget.alias("a") #For dimension
                                .join(gl_account.alias('c'),expr('a.glAccount = c.accountNo AND a.companyKey = c.companyKey'),'left') 
                                .join(company.alias('comp'),expr('a.companyKey = comp.companyKey'))
                                .join(dim_set.alias('b'),expr('a.dimensionSetEntryKey = b.dimensionSetEntryKey'),'left')
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Extracting dimensionsetkey columns for select statment.
excluded_columns = ['dimensionSetEntryKey','companyKey','source_system']  # Replace with actual column names you want to exclude
columns_from_b = [col(f'b.{column}') for column in dim_set.columns if column not in excluded_columns]
print(columns_from_b)

# Select and combine all fields
result = gl_budget.select(
    # GL fields from table 'a'
  #GL fields
    col('a.budgetName'),
    col('c.accountKey'),
    col('glAccount').alias('account'),
    col('a.date'),
    col('comp.companyCurrencyCode'),
    col('a.sourceSystem').alias('dataSource'),
    col('a.companyKey'),
    col('a.amount').alias('amountAccounting'),
    # Compute 'amountReporting' column
    when(
        col('c.incomeBalance') == 'Balance Sheet',
        col('a.amount') * -1
    ).otherwise(
        col('a.amount')
    ).alias('amountReporting'),
    monotonically_increasing_id().alias('entryNo'),
    *columns_from_b,
)

result = FM_Utility.add_nullhandling(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Currency Conversion

# CELL ********************

# Define the list of columns for currency handling - these currencies will be translated into local curreny and group currency. 

tcy_columns_list = []  #No transaction currencies exists in finance.
lcy_columns_list = ['amountAccounting','amountReporting'] ## All transactions in finance are always at local currency, which is the company currencycode. 

# Get Currency Dataframe 
currency_df = FM_Utility.get_currency_conversion_df()

# Calling currency transformer adding currency columns for LCY and GCY 
result = FM_Utility.merge_currency_conversion(result, currency_df, "companyCurrencyCode", "companyCurrencyCode", "date", tcy_columns_list, lcy_columns_list, group_currency=GlobalParameters.group_currency)


result = result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Customer Specific Transformation


# CELL ********************

DataCheck.check_duplicates(result,['entryNo','companyKey','dataSource'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load 

# CELL ********************

result.write.mode("overwrite").option("overwriteSchema","true").format("delta").saveAsTable(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
