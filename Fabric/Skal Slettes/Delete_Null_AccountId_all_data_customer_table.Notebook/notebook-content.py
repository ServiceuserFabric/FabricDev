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
# META     },
# META     "warehouse": {}
# META   }
# META }

# MARKDOWN ********************

# # Delete rows from dbo.Customer where account_id is NULL in Silver_lakehouse


# PARAMETERS CELL ********************

LAKEHOUSE_NAME = "Bronze_lakehouse"
TABLE_NAME = "all_data_customer_table"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
print(f"Deleting rows from {TABLE_NAME} where accounts_id IS NULL")
spark.sql(f"DELETE FROM {TABLE_NAME} WHERE accounts_id IS NULL")
print("Delete completed")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql(f"SELECT COUNT(*) AS RemainingNullAccountIds FROM {TABLE_NAME} WHERE accounts_id IS NULL"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
