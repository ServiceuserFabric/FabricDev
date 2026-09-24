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

# # Data Prep: items
# 
# Ingest data from raw navision and bc tables, table is writen into dp lakehouse with overwrite
# 1. transfer the nav table to same format as BC
# 2. Union the tables and write them to DP layer using overwrite
# 


# MARKDOWN ********************

# ### Libraries

# CELL ********************

%run FM_Utility

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_table   = "DP.dp_items" 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load

# CELL ********************

items_bc =  FM_Utility.load_cleaned_dataframe("Raw",'Item', "camel")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Make the final table and enforce schema

# CELL ********************


#Final result table and schema
result = items_bc.select(
    col('no')                    .cast('string').alias('no'),
    col('description')           .cast('string').alias('description'),
    col('description2')          .cast('string').alias('description2'),
    col('inventoryPostingGroup') .cast('string').alias('inventoryPostingGroup'),
    col('itemCategoryCode')      .cast('string').alias('itemCategoryCode'),
    # col('LSCRetailProductCode')  .cast('string').alias('itemProductCode'),
    #handle old dates
    # when(col('lastDateModified') < lit('1900-01-01'),lit('1900-01-01')).otherwise(col('lastDateModified')).cast('date').alias('lastDateModified'),
    col('lastDirectCost')        .cast('double').alias('lastDirectCost'),
    col('unitCost')              .cast('double').alias('unitCost'),
    col('unitPrice')             .cast('double').alias('unitPrice'),
    col('vendorNo')              .cast('string').alias('vendorNo'),
    # col('LOVSSubVendorNo')       .cast('string').alias('subVendorNo'),
    col('company'),
    lit('bc')          .cast('string').alias('sourceSystem')
)
#display(result.filter(expr("sourceSystem='nav'")))

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

# ### Load

# CELL ********************

result.write.mode("overwrite").option("overwriteSchema","true").format("delta").saveAsTable(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
