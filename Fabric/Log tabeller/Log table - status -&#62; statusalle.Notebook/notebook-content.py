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
# META       "default_lakehouse_workspace_id": "6b06974a-4346-4a38-bc5a-d42e564a6bec"
# META     },
# META     "warehouse": {
# META       "default_warehouse": "39d339dc-5635-453e-ba9e-8734cbf0953f",
# META       "known_warehouses": [
# META         {
# META           "id": "39d339dc-5635-453e-ba9e-8734cbf0953f",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Status log table data load

# CELL ********************

spark.sql("""
DROP TABLE silver_lakehouse.statusalle
""")

spark.sql("""
CREATE OR REPLACE TABLE silver_lakehouse.statusalle
AS
SELECT * FROM silver_lakehouse.status
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
