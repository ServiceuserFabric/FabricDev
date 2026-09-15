# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "836bc8da-2d44-4096-be88-36e2f91bbf80",
# META       "default_lakehouse_name": "Golden_lakehouse",
# META       "default_lakehouse_workspace_id": "6b06974a-4346-4a38-bc5a-d42e564a6bec",
# META       "known_lakehouses": [
# META         {
# META           "id": "836bc8da-2d44-4096-be88-36e2f91bbf80"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Coerce columns to int if not already for [Kundebasen], [Salg] and [Churn]

from pyspark.sql.functions import col, to_date

df_budget = (
    spark.table("Golden_lakehouse.Budget")
    .withColumn("Dato", to_date(col("Dato"), "yyyy-MM-dd"))     # DATE
    .withColumn("Kundebasen", col("Kundebasen").cast("decimal(38,4)"))
    .withColumn("Salg", col("Salg").cast("decimal(38,2)"))
    .withColumn("Churn", col("Churn").cast("decimal(38,2)"))
)

df_budget.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Golden_lakehouse.Budget")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
