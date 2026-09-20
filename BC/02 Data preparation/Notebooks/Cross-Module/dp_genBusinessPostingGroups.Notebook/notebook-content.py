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

# # Data Prep: genBusinessPostingGroups
# 
# Ingest data from raw navision and bc tables and combines them into table, table is writen into dp lakehouse with overwrite
# 1. write them to DP layer using overwrite

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

# ### Load

# CELL ********************

target_table   = "DP.dp_genBusinessPostingGroups"

genBusinessPostingGroups_bc = FM_Utility.load_cleaned_dataframe('Raw','GenBusinessPostingGroup','camel')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Transform to BC format and select wanted columns

# CELL ********************

#nav date format pattern
date_format = "MMM dd yyyy hh:mma"

genBusinessPostingGroups_bc = genBusinessPostingGroups_bc.select(
    'code',
    'description',
    'company',
    lit('bc').alias('sourceSystem')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Make the final table and enforce schema

# CELL ********************

result = genBusinessPostingGroups_bc.select(
    col('code')        .cast('string').alias('code'),
    col('description') .cast('string').alias('description'),
    col('sourceSystem').cast('string').alias('sourceSystem'),
    col('company')
)

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
