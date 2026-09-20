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

# # Enriched entity: Company
# Reads tables from dp and saves to Enr, mode: full overwrite  <br>
# 1. Creates full company table
# 2. Creates table that only contains "Active" companies

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

target_table = "Enr.enr_company"
target_table2 = "Enr.enr_companyNonHistoric" # This table will not include NAV companykeys of companies, which are already live in BC

company = spark.read.format("delta").table("DP.dp_company")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

company = company.select(
    col("companyKey"),
    col('CompanyCurrencyCode').alias('companyCurrency'),
    col('companyName'),
    col('CompanyCountry').alias('companyCountry')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Null handling adds a row with -1 keys for PBI
company = FM_Utility.add_nullhandling(company)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Transform data (Standard)

# CELL ********************

#Create table that only contains companies from NAV that are not precent in BC
non_historic_company = company.select(
    col("companyKey").cast('int'),
    col('companyCurrency'),
    col('companyName'),
    col('companyCountry')
)
non_historic_company=non_historic_company.orderBy("companyKey")\
.dropDuplicates(["companyName"])\
.withColumn("companyKey", col("companyKey").cast("string"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# check duplicates 

DataCheck.check_duplicates(non_historic_company,'companyKey')
DataCheck.check_duplicates(company,'companyKey')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### save

# CELL ********************

company.write.mode("overwrite").option("overwriteSchema","true").format("delta").saveAsTable(target_table)
non_historic_company.write.mode("overwrite").option("overwriteSchema","true").format("delta").saveAsTable(target_table2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
