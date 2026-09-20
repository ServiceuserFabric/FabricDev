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

# # Enriched entity: Customer
# 
# ### Dependency
# - Company
# 
# Reads tables from dp and saves to Enr, mode: full overwrite  <br>
# 


# CELL ********************

%run FM_Utility

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load

# PARAMETERS CELL ********************


target_table = 'Enr.enr_customer'

df_Customer = FM_Utility.load_cleaned_dataframe('Raw','Customer','camel')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(df_Customer)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Transformations

# CELL ********************

result = df_Customer.select(
# Basic customer info
col('company').cast('string').alias('company'),
col('no').cast('string').alias('no'),
col('name').cast('string').alias('name'),
concat_ws(' - ', col('no'), col('name')).cast('string').alias('customerCodeandName'),

# Address information
col('address').cast('string').alias('address'),
col('address2').cast('string').alias('address2'),
col('city').cast('string').alias('city'),
col('countryRegionCode').cast('string').alias('countryRegionCode'),  

# Payment & sales information
col('paymentTermsCode').cast('string').alias('paymentTermsCode'),
col('paymentMethodCode').cast('string').alias('paymentMethodCode'),  
col('salespersonCode').cast('string').alias('salespersonCode'),      

# Contact information
col('eMail').cast('string').alias('email'),  

# Other fields
col('iCPartnerCode').cast('string').alias('icPartnerCode'),  

# Source system
lit('bc').cast('string').alias('source_system')
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add -1 row for null handling
result = FM_Utility.add_nullhandling(result)

# Add company key
result = FM_Utility.add_company_key(result)

# Generate customer and salesperson keys
result = (result
    .withColumn("customerKey", concat_ws('_', col('source_system'), col('companyKey'), col('no')))
    .withColumn("salesPersonKey", concat_ws('_', col('source_system'), col('companyKey'), col('salespersonCode')))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Check

# CELL ********************

DataCheck.check_duplicates(result,'customerKey')


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
