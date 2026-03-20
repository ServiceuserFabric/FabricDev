# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "dcddaf9e-0415-4d65-894b-104157b74950",
# META       "default_lakehouse_name": "Puzzel_Altibox",
# META       "default_lakehouse_workspace_id": "6b06974a-4346-4a38-bc5a-d42e564a6bec",
# META       "known_lakehouses": [
# META         {
# META           "id": "dcddaf9e-0415-4d65-894b-104157b74950"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Import package
from pyspark.sql import functions as F

# Read the Lakehouse table (Spark SQL)
df = spark.sql("SELECT * FROM dbo.agent_events")

# Equivalent to:
# SELECT rec_id, COUNT(*) cnt FROM dbo.agent_events GROUP BY rec_id HAVING COUNT(*) > 1
dupe_keys = (
    df.groupBy("rec_id")
      .agg(F.count(F.lit(1)).alias("cnt"))
      .filter(F.col("cnt") > 1)
      .orderBy(F.desc("cnt"), F.asc("rec_id"))
)

display(dupe_keys)   # Fabric notebook display


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import package
from pyspark.sql.window import Window

w = Window.partitionBy("rec_id").orderBy(F.col("dte_start").desc_nulls_last())

ranked = df.withColumn("rn", F.row_number().over(w))

to_delete_preview = ranked.filter(F.col("rn") > 1)
display(to_delete_preview)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Skriv i første omgang til ny tabel for at tjekke
deduped = ranked.filter(F.col("rn") == 1).drop("rn")

(
    deduped.write
          .mode("overwrite")
          .format("delta")
          .option("overwriteSchema", "true")
          .saveAsTable("dbo.agent_events_deduped_tmp")
)

# Then you can validate counts before replacing original
print("Original rows:", df.count())
print("Deduped rows:", deduped.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


(
    deduped.write
          .mode("overwrite")
          .format("delta")
          .option("overwriteSchema", "true")
          .saveAsTable("dbo.agent_events")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
