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
# MAGIC SELECT * FROM Bronze_lakehouse.all_data_cust_prod

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_rma = spark.sql("SELECT * FROM Bronze_lakehouse.all_data_cust_prod_rma")
df = spark.sql("SELECT * FROM Bronze_lakehouse.all_data_cust_prod")
# display(df)
print(df_rma.count())
print(df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
