# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3ccdc5e8-f799-4ab1-87cc-80fbf06f7929",
# META       "default_lakehouse_name": "GoogleAnalytics_Altibox",
# META       "default_lakehouse_workspace_id": "6b06974a-4346-4a38-bc5a-d42e564a6bec",
# META       "known_lakehouses": [
# META         {
# META           "id": "3ccdc5e8-f799-4ab1-87cc-80fbf06f7929"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
spark.sql('DROP TABLE GoogleAnalytics_Altibox.Demographics')
spark.sql('DROP TABLE GoogleAnalytics_Altibox.Audience_name')
spark.sql('DROP TABLE GoogleAnalytics_Altibox.TrafficSource')
spark.sql('DROP TABLE GoogleAnalytics_Altibox.Engagement')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
