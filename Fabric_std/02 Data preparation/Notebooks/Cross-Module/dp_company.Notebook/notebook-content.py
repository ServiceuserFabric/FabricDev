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

# ## Company
# Reads tables from raw and saves to DP layer, mode: full overwrite  <br>
# Create nav_lookup_company table in raw lakehouse that's used by other notebooks on pipeline

# MARKDOWN ********************

# ### Libraries

# CELL ********************

%run FM_Utility

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Ingestion

# CELL ********************

target_table = "DP.dp_Company"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create the company table
#companyName, companyCurrencyCode, bcStartDate, dataSource, companyKey, CompanyCountry, bcCompanyCode, navCompanyCode
data = GlobalParameters.company_data 
columns = [
'companyName',
'companyCurrencyCode',
'companyKey',
'CompanyCountry'
]

company_bc = spark.createDataFrame(data,columns)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Modeling

# CELL ********************


company_bc = company_bc.select(
'companyName',
'companyCurrencyCode',
'companyKey',
'CompanyCountry'
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load

# CELL ********************

company_bc.write.mode("overwrite").option("overwriteSchema","true").format("delta").saveAsTable(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
