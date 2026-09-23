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

# # Enriched Dimension: Salesperson/Purchaser
# 
# ### Purpose
# Create Salesperson/Purchaser dimension table from BC SalespersonPurchaser table.
# This table serves both sales and purchase fact tables — the same person can act as a salesperson, purchaser, or both.
# 
# ### Source Tables
# - Raw.SalespersonPurchaser
# 
# ### Output
# - Enr.enr_salespersonPurchaser
# - Mode: Full overwrite
# 
# ### Key Design
# - `salespersonPurchaserKey` is the single primary key on this dimension
# - Sales fact tables join via their `salesPersonKey` column
# - Purchase fact tables join via their `purchaserKey` column
# - Both map to `salespersonPurchaserKey` on this dimension

# CELL ********************

%run FM_Utility

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load

# CELL ********************

target_table = "Enr.enr_salespersonPurchaser"

df = FM_Utility.load_cleaned_dataframe('Raw', 'SalespersonPurchaser', 'camel')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform

# CELL ********************

result = df.select(
    col('company').cast('string').alias('company'),
    col('code').cast('string').alias('code'),
    col('name').cast('string').alias('name'),
    col('eMail').cast('string').alias('email'),
    col('blocked').cast('string').alias('blocked'),
    lit('bc').cast('string').alias('source_system')
)

# Add null handling and company key
result = FM_Utility.add_nullhandling(result)
result = FM_Utility.add_company_key(result)

# Primary key
result = result.withColumn(
    "salespersonPurchaserKey",
    concat_ws('_', col('source_system'), col('companyKey'), col('code'))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validate

# CELL ********************

DataCheck.check_duplicates(result, 'salespersonPurchaserKey')

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
