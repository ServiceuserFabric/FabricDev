# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ac6fbbda-655a-466b-abfa-ba456ca9605e",
# META       "default_lakehouse_name": "Silver_lakehouse",
# META       "default_lakehouse_workspace_id": "6b06974a-4346-4a38-bc5a-d42e564a6bec",
# META       "known_lakehouses": [
# META         {
# META           "id": "ac6fbbda-655a-466b-abfa-ba456ca9605e"
# META         }
# META       ]
# META     },
# META     "warehouse": {}
# META   }
# META }

# MARKDOWN ********************

# # Delete rows from dbo.Customer where account_id is NULL in Silver_lakehouse


# PARAMETERS CELL ********************

LAKEHOUSE_NAME = "Silver_lakehouse"
TABLE_NAME = "Customer"


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
