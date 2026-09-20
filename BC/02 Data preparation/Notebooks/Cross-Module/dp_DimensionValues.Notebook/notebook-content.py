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

# # Enriched entity: Create Dimension Tables

# MARKDOWN ********************

# This code will iterate over ext_dimensionvalues and create simple dimensions for each dimensionsetentry combination.  '
# Define the sink table names in this input to create the enriched tables. 
# 
# 
# TODO: If this goes to DP? Should we specify schema for dimension? warehouse. fact HR, Hr --> warehouse fact_hc dim_warehouse --> specify dimension or remove all na/-1 calls
# TODO: How do we make this to work with HistoryData ?

# MARKDOWN ********************

# ##  Libraries

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

target_table = 'DP.dp_dimensionValues'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dimension_values = FM_Utility.load_cleaned_dataframe("Raw", "DimensionValue", "camel")
company = FM_Utility.load_cleaned_dataframe("Raw", "Company", "camel") 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Lookups
#dimension_values = dimension_values.join(company,expr('Company = bcCompanyKey'))
dimension_values = dimension_values.withColumn('source_system',lit('bc'))

#union = dimension_values.unionByName(dimension_values_nav, allowMissingColumns = True)

#Create the final df and force schema
result = dimension_values.select(
    col('company'),
    col('code').cast('string'),
    col('name').cast('string'),
    col('blocked').cast('string'),
    col('dimensionCode').cast('string'),
    col('dimensionId').cast('string'),
    col('dimensionValueId'),
    col('source_system')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper Code

# CELL ********************

result = FM_Utility.add_company_key(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write dimensionsetentry 
result.write.mode("overwrite").option("overwriteSchema","true").format("delta").saveAsTable(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
