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

# # Enriched entity: GL Account
# 
# ### Dependency
# - dp_company
# - dp_glAccounts
# 
# Business Area: Finance \
# This code creates the enriched account table  \
# 
# Latest changes: Code refactored to new standard and using DP, added logic to pick BC accounts based on activiation date on company table 
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


target_table = f"Enr.enr_glAccount"

# Tables
df_accounts = spark.read.format("delta").table("DP.dp_glAccounts")
company = spark.read.table('DP.dp_company')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dp_company already contains only relevant companies (bcStartDate column was removed from the table)

#inner join active companies to accounts
df_accounts = df_accounts.alias('a')#.join(company,'companyKey').select('a.*')



result = df_accounts.select(
     concat_ws('_', col('companyKey'), col('no')).alias('accountKey'),
     concat_ws('-', col('no'), col('name')).alias('accountNumberAndName'),
     col('no').alias('accountScheduleKey'), 
     "companyKey",
     col('no').alias('accountNo'), 
     col('apiAccountType'),
     col('accountCategory'),
     col('accountType'),
     col('incomeBalance'),
     #col('indentation'),
     col('name'),
     col('totaling'),
     #col('accountNoIsNumeric'),
     #col('noNumeric').alias('accountNoNumeric'),
     #P&L sign flag switch
     when(col('incomeBalance') == "Income Statement", -1)
    .when(col('incomeBalance') == "Balance Sheet", 1)
    .otherwise(None)
    .cast("int").alias('P&LSignSwitch'),
    #Specific
#     col('CompensationAccount')

     ).na.drop(how='any',subset=["accountNo"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validate

# CELL ********************

DataCheck.check_duplicates(result,'accountKey')

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
