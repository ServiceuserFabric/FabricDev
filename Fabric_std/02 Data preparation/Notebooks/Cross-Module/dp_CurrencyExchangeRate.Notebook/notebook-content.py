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

# # Data Prep: CurrencyExchangeRate
# 
# Ingest data from raw navision and bc tables and combines them into table, table is writen into dp lakehouse with overwrite
# 1. transfer the nav table to same format as BC
# 2. Union the tables and write them to DP layer using overwrite

# MARKDOWN ********************

# ### Libraries

# CELL ********************


# Set the configuration for reading and writing ancient dates there is one mistake record with date 0001-01-01 ....
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run FM_Utility

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load

# CELL ********************

target_table   = "DP.dp_currencyExchangeRates"

exch_rate_bc = FM_Utility.load_cleaned_dataframe("Raw",'CurrencyExchangeRate', "camel")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Transformation

# CELL ********************

#nav date format pattern
date_format = "MMM dd yyyy hh:mma"

# BC
exch_rate_bc = exch_rate_bc.select(
    # col('companyCurrencyCode'),
    col('company'),
    when(col('startingDate')<= lit('1900-01-01'),to_date(lit('1900-01-01'))).otherwise(col('startingDate')).alias('startingDate'),
    col('adjustmentExchRateAmount'),
    col('exchangeRateAmount'),
    col('relationalAdjmtExchRateAmt'),
    col('currencyCode'),
    col('relationalCurrencyCode'),
    col('relationalExchRateAmount'),
    lit('bc').alias('dataSource')
)#.filter(expr("startingdate >= b.bcStartDate"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fix the final schema and custom filters

# CELL ********************

result = exch_rate_bc.select(
    col('currencyCode').cast('string').alias('currencyCode'),
    col('startingDate').cast('date').alias('startingDate'),
    col('exchangeRateAmount').cast('double').alias('exchangeRateAmount'),
    col('adjustmentExchRateAmount').cast('double').alias('adjustmentExchRateAmount'),
    col('relationalCurrencyCode').cast('string').alias('relationalCurrencyCode'),
    col('relationalExchRateAmount').cast('double').alias('relationalExchRateAmount'),
    col('relationalAdjmtExchRateAmt').cast('double').alias('relationalAdjmtExchRateAmt'),
    col('company'),
    # col('companyCurrencyCode').cast('string').alias('companyCurrencyCode'),
    col('dataSource').cast('string').alias('dataSource')
)
result = result.filter(col("startingDate") >= "2015-12-31")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = FM_Utility.add_company_key(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Save

# CELL ********************

result.write.mode("overwrite").option("overwriteSchema","true").format("delta").saveAsTable(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
