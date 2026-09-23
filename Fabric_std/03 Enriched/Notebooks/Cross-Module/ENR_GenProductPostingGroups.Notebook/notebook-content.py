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

# # Enriched entity: GenProductPostingGroups
# 
# ### Dependency: 
# - dp_dp_genProductpostinggroups
# 
# Reads tables from dp and saves to Enr, mode: full overwrite  <br>

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

# ### Load

# PARAMETERS CELL ********************

target_table = 'Enr.enr_GenProductPostingGroups'

product_posting_groups = spark.read.table('DP.dp_genProductpostinggroups')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Transform data 

# CELL ********************

result = product_posting_groups.select(
    col('companyKey'), 
    col('code').alias('genProductPostingGroupsCode'), 
    col('description').alias('genProductPostingGroupsDescription'),
    concat_ws('_', col('companyKey'), col('code')).alias('genProductPostingGroupKey'),
    col('sourceSystem')
)

result = FM_Utility.add_nullhandling(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation

# CELL ********************

DataCheck.check_duplicates(result,'genProductPostingGroupKey','sourceSystem')

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
