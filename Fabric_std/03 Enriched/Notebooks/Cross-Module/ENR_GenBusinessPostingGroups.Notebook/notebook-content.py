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

# # Enriched entity: GenBusinessPostingGroups
# 
# ### Dependency
# - dp_genBusinessPostingGroups
# 
# Reads tables from dp and saves to Enr, mode: full overwrite  <br>

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

target_table = 'Enr.enr_GenBusinessPostingGroups'

business_posting_groups = spark.read.table('DP.dp_genbusinesspostinggroups')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Transformation

# CELL ********************

result = business_posting_groups.select(
    col('companyKey'), 
    col('code').alias('genBusinessPostingGroupsCode'), 
    col('description').alias('genBusinessPostingGroupsDescription'),
    concat_ws('_', col('companyKey'), col('code')).alias('genBusinessPostingGroupKey'),
    col('sourceSystem')
)

result = FM_Utility.add_nullhandling(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validate

# CELL ********************

DataCheck.check_duplicates(result,'genBusinessPostingGroupKey','sourceSystem')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Save

# CELL ********************

result.write.mode("overwrite").option("overwriteSchema","true").format("delta").saveAsTable(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
