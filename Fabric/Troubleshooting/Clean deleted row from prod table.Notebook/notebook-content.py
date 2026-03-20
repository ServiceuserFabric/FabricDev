# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b1df83e8-4963-4129-b9a8-db8ced7800dd",
# META       "default_lakehouse_name": "Bronze_lakehouse",
# META       "default_lakehouse_workspace_id": "6b06974a-4346-4a38-bc5a-d42e564a6bec",
# META       "known_lakehouses": [
# META         {
# META           "id": "b1df83e8-4963-4129-b9a8-db8ced7800dd"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DELETE FROM all_data_cust_prod 
# MAGIC WHERE id IN (22038
# MAGIC ,21946
# MAGIC ,21945
# MAGIC ,21937
# MAGIC ,21936
# MAGIC ,21935
# MAGIC ,21797
# MAGIC ,21791
# MAGIC ,21783
# MAGIC ,17118
# MAGIC ,7390)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
