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

# # Enriched entity: Item
# 
# ### Dependency:
# - dp_items
# 


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

# ## Load data

# PARAMETERS CELL ********************

target_table = "Enr.enr_items"
items = spark.read.table('DP.dp_items')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = items.alias("itm").select(
 col('itm.no').alias('itemNumber'),
 col('itm.description'),
 col('itm.description2'),
 col('itm.inventoryPostingGroup'),
 col('itm.itemCategoryCode'),
#  col('itm.itemProductCode'),
#  col('itm.lastDateModified'),
 col('itm.lastDirectCost'),
 col('itm.unitCost').alias('itemUnitCost'),
 col('itm.unitPrice'),
 concat_ws(' - ', col('itm.no'), col('itm.description')).alias('ItemCodeAndDescription'),
 concat_ws('_', lit('bc'), col('itm.companyKey'), col('itm.no')).alias('itemKey')
)
result = result.na.drop(how='any',subset=["itemNumber"])

result = FM_Utility.add_nullhandling(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validate

# CELL ********************

DataCheck.check_duplicates(result,'itemKey')

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
