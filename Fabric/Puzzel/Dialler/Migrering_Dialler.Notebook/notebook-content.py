# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8c6daa7e-25e3-49f4-8f00-7ba8daf52bc3",
# META       "default_lakehouse_name": "Rapporteringer",
# META       "default_lakehouse_workspace_id": "6b06974a-4346-4a38-bc5a-d42e564a6bec",
# META       "known_lakehouses": [
# META         {
# META           "id": "8c6daa7e-25e3-49f4-8f00-7ba8daf52bc3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Delete or truncate Rapporteringer
spark.sql("TRUNCATE TABLE Rapporteringer.dbo.Migreringer")
spark.sql("TRUNCATE TABLE dbo.migrering_dialler")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

# Import from Rapporteringer.dbo.Migreringer the following columns Mobilnummer, Kundenummer, Email, Navn where max Dato
df = spark.sql("SELECT Mobilnummer, Kundenummer, Email, Navn FROM Rapporteringer.dbo.Migreringer WHERE Dato = (SELECT MAX(Dato) FROM Rapporteringer.dbo.Migreringer)")

# Change mobilnummer to add 00
from pyspark.sql.functions import concat, lit

df = df.withColumn("Mobilnummer", concat(lit("0045"), df.Mobilnummer))

# Take df and and insert or overwrite in new table called Migrering_dialler
df.write.mode("overwrite").saveAsTable("Migrering_dialler")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
