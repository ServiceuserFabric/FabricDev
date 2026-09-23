# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9664b42f-75e3-44c2-a937-8a0231cc803e",
# META       "default_lakehouse_name": "Raw",
# META       "default_lakehouse_workspace_id": "3a25df0d-986f-45a8-9140-8e9d88526e86",
# META       "known_lakehouses": [
# META         {
# META           "id": "9664b42f-75e3-44c2-a937-8a0231cc803e"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Enriched entity: Create Dimension Tables and DimensionSetEntry

# MARKDOWN ********************

# This code will iterate over ext_dimensionvalues and create simple dimensions for each dimensionsetentry combination.  '
# Define the sink table names in this input to create the enriched tables. 
# 
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

# ## Parameter

# PARAMETERS CELL ********************

target_table = 'DP.dp_dimensionsetentry'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load

# CELL ********************

dimension_set_entry_bc = FM_Utility.load_cleaned_dataframe("Raw", 'DimensionSetEntry', "camel")
dimension_names = GlobalParameters.dimension_mapping

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dimension_set_entry_bc = dimension_set_entry_bc.withColumn('source_system',lit('bc'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Pivot the entry by dimension code
dimension_set_entry = dimension_set_entry_bc.groupBy(["dimensionSetID",'company','source_system']).pivot('DimensionCode').agg(first(col('dimensionValueCode')))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create the dimensions, add null column for any dimension code not present in the pivot (e.g. no entries yet)
dimension_columns = [
    col(name[0]).cast("string").alias('dse_'+name[2].lower()+'Key') if name[0] in dimension_set_entry.columns
    else lit(None).cast("string").alias('dse_'+name[2].lower()+'Key')
    for name in dimension_names
]

dimension_set_entry = FM_Utility.add_company_key(dimension_set_entry)

dse = dimension_set_entry.select(
    col('companyKey'),
    col('dimensionSetId').cast('long').alias('dimensionSetID'),
    col('source_system'),
    *dimension_columns,
    concat_ws('_', col('source_system'), col('companyKey'), col('dimensionSetId')).cast('string').alias('dimensionSetEntryKey')
)

# Fill missing values with "-1" in the identified columns
dse = dse.fillna(value="-1")#, subset=columns_to_fill) should it not just be all? 

# Apply FM_Utility.add_nullhandling
result = FM_Utility.add_nullhandling(dse)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DataCheck.check_duplicates(result,'dimensionSetEntryKey')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper Code

# CELL ********************

# Write dimensionsetentry 
result.write.mode("overwrite").option("overwriteSchema","true").format("delta").saveAsTable(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
