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

# # Data Prep: glAccount
# 
# Ingest data from bc tables and combines them into table, table is writen into dp lakehouse with overwrite
# We only use BC accounts so that we only have one set of account hierarchy. 


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

target_table   = "DP.dp_glAccounts"

account_bc = FM_Utility.load_cleaned_dataframe('Raw','GLAccount','camel')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Make the final table and enforce schema

# CELL ********************

result = account_bc.select(
    col('company'),
    # col('companyName')        .cast('string') .alias('company'),
    col('no')                 .cast('string') .alias('no'),
    col('name')               .cast('string') .alias('name'),
    col('accountType')        .cast('string') .alias('accountType'),
    col('incomeBalance')      .cast('string') .alias('incomeBalance'),
    #col('indentation')        .cast('long')   .alias('indentation'),?
    col('totaling')           .cast('string') .alias('totaling'),
    #col('accountNoIsNumeric') .cast('boolean').alias('accountNoIsNumeric'),
    #col('noNumeric')          .cast('string') .alias('noNumeric'),
    col('apiAccountType')     .cast('string') .alias('apiAccountType'),
    col('accountCategory')    .cast('string') .alias('accountCategory'),
    # special
    # col('LOVSCompensationAccount').cast('boolean').alias('CompensationAccount')
)
#display(result)

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
