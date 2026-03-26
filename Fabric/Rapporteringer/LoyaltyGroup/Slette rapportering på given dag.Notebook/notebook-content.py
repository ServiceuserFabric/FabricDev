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

# Welcome to your new notebook
# Type here in the cell editor to add code!
#
spark.sql("""
DELETE FROM Rapporteringer.dbo.LoyaltyGroup
WHERE Udsendelsesdato = DATE '2026-03-25'
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
