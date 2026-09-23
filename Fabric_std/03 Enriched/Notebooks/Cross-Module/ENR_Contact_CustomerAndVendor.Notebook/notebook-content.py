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

# # Enriched entity: Contact (Customer/Vendor)
# 
# ### Dependency
# - Company
# 
# Reads tables from dp and saves to Enr, mode: full overwrite  <br>
# 
# 1. To vendor we join nothing


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


target_table = 'Enr.enr_contact'

vendor = FM_Utility.load_cleaned_dataframe('Raw','Vendor','camel')
customer = FM_Utility.load_cleaned_dataframe('Raw','Customer','camel')
company = spark.read.format("delta").table("DP.dp_Company")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vendor = vendor.drop("CurrencyCode")
vendor = vendor.withColumn("ContactType",lit("vend"))
customer = customer.drop("City","EMail")
customer = customer.withColumn("ContactType",lit("cust"))
FM_Utility.compare_dataframe_schemas(vendor, customer,"vendor","customer")

contact = customer.unionByName(vendor,allowMissingColumns=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Transformations
# - Adding csv 

# CELL ********************

## 
#inner join active companies to accounts
contact = contact.alias('a').join(company.alias('b'),expr('a.Company = b.companyName'))

result = contact.select(
    col('b.companyKey').cast('string').alias('companyKey'),
    col('No').alias('contactNumber'),
    col('Address').alias('address'),
    col('Address2').alias('address2'),
    col('CountryRegionCode').alias('countryRegionCode'),
    col('ICPartnerCode').alias('icPartnerCode'),
    col('Name').alias('name'),
    col('PaymentMethodCode').alias('paymentMethodCode'),
    col('PaymentTermsCode').alias('paymentTermsCode'),
    col('PaymentTermsId').alias('paymentTermsId'),
    col('SalespersonCode').alias('salespersonCode'),
    col('purchaserCode').alias('purchaserCode'),
    concat_ws('_', col('b.companyKey'),col('ContactType'), col('No')).alias('contactKey'),

    when((col('SalespersonCode').isNull()) | (col('SalespersonCode') == ''), lit('-1'))
        .otherwise(concat_ws('_', lit('bc'),col('Company'), col('SalespersonCode')))
        .alias('salesPersonKey'),

    #concat_ws(' - ', col('BCCustomerKey'), col('Name')).alias('CustomerCodeAndName'),


)


#Remove nulls
result = result.na.drop(how='any',subset=["contactNumber"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Add -1 row
result = FM_Utility.add_nullhandling(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Check

# CELL ********************

# Remove duplicate keys
result = FM_Utility.add_row_number(result,['contactKey'],'contactNumber').filter(col('row_num') == 1).drop('row_num')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DataCheck.check_duplicates(result,'contactKey')


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
