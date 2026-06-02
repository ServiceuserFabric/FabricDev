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

# Sletteregel for gamle records. Vi sletter records ældre en fem år 

# Vi sletter fra agent_events 
spark.sql("""
DELETE FROM Puzzel_Altibox.dbo.agent_events
WHERE dte_start < add_months(current_date(), -60)
""")

# Vi sletter fra call_events 
spark.sql("""
DELETE FROM Puzzel_Altibox.dbo.call_events
WHERE dte_start < add_months(current_date(), -60)
""")

# Vi sletter fra vw_enqreg_total
spark.sql("""
DELETE FROM Puzzel_Altibox.dbo.vw_enqreg_total
WHERE dte_time_stamp < add_months(current_date(), -60)
""")

# Vi sletter fra enqreg_header
spark.sql("""
DELETE FROM Puzzel_Altibox.dbo.enqreg_header
WHERE dte_time_stamp < add_months(current_date(), -60)
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
