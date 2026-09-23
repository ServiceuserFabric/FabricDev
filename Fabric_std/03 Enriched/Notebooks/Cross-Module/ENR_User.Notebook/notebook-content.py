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

# # Enriched entity: User
# 
# ### Dependency
# - Company
# 
# Reads tables from dp and saves to Enr, mode: full overwrite  <br>
# 


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


target_table = 'Enr.enr_user'

df_user = FM_Utility.load_cleaned_dataframe('Raw','User','camel')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Transformations

# CELL ********************

result = df_user.select(
    
    col('userSecurityID').cast('string').alias('userSecurityID'),
    col('fullName').cast('string').alias('fullName'),
    col('userName').cast('string').alias('userName'),
    lit('bc').cast('string').alias('sourceSystem')
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Add -1 row
result = (FM_Utility.add_nullhandling(result)
            .withColumn("userKey", concat_ws('_', col('sourceSystem'), col('userName')))
            )

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
