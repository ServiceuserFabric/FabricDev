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

# # Enriched Dimension: Vendor
# 
# ### Purpose
# Create Vendor dimension table from BC Vendor master data.
# 
# ### Source Tables
# - Raw.Vendor
# 
# ### Output
# - Enr.enr_vendor
# - Mode: Full overwrite
# 
# ### Key Design
# - `vendorKey` is the primary key on this dimension (`bc_<companyKey>_<vendorNo>`)
# - `purchaserKey` is the vendor's default purchaser, joins to `salespersonPurchaserKey` on the Salesperson/Purchaser dimension

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

target_table = 'Enr.enr_vendor'

df = FM_Utility.load_cleaned_dataframe('Raw', 'Vendor', 'camel')

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
    col('no').cast('string').alias('no'),
    col('name').cast('string').alias('name'),
    col('address').cast('string').alias('address'),
    col('address2').cast('string').alias('address2'),
    col('countryRegionCode').cast('string').alias('countryRegionCode'),
    col('paymentTermsCode').cast('string').alias('paymentTermsCode'),
    col('paymentMethodCode').cast('string').alias('paymentMethodCode'),
    col('purchaserCode').cast('string').alias('purchaserCode'),
    col('currencyCode').cast('string').alias('currencyCode'),
    col('iCPartnerCode').cast('string').alias('icPartnerCode'),
    lit('bc').cast('string').alias('source_system')
)

# Add null handling and company key
result = FM_Utility.add_nullhandling(result)
result = FM_Utility.add_company_key(result)

# Generate keys
result = (result
    .withColumn("vendorKey", concat_ws('_', col('source_system'), col('companyKey'), col('no')))
    .withColumn("purchaserKey", concat_ws('_', col('source_system'), col('companyKey'), col('purchaserCode')))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validate

# CELL ********************

DataCheck.check_duplicates(result, 'vendorKey')

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
