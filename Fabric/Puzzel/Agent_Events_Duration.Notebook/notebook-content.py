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
# META     },
# META     "warehouse": {
# META       "default_warehouse": "137ddd74-a72e-4b47-bf4d-af4d820aa79b",
# META       "known_warehouses": [
# META         {
# META           "id": "137ddd74-a72e-4b47-bf4d-af4d820aa79b",
# META           "type": "Lakewarehouse"
# META         },
# META         {
# META           "id": "e4125510-af74-9fbf-4e0b-acffe53bd032",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#__________________________________________________________________________________
#                             HEADER                                           ----  

# Script Name:                Calculation of duration of agent events.py
# Description:                A script to calculate a table upon agent_events with 
#                             the duration of each event, and the total duration of 
#                             events per agent.
# Dependencies:               
# Input:                      
# Output:                     Agent_events_duration.xlsx
# Created by:                 Mathias Liedtke
# Created date:               11/03/2026
# Last Modified by:           Author modified date
# Last Modified date:         Date modified
# Jira ticket                 KON-62
# Comments: 
#__________________________________________________________________________________

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

# Source Lakehouse table
events = spark.read.table("agent_events")

events = (
    events
    .withColumn("dte_start", F.to_timestamp("dte_start"))
    .withColumn("event_date", F.to_date("dte_start"))
)
events.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 
today = F.current_date()
now_ts = F.current_timestamp()

def end_of_day(col_date):
    return F.expr("timestampadd(microsecond, -1, date_add({0}, 1))".format(col_date))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sign in and out 
w = Window.partitionBy("agent_id").orderBy("dte_start")

sign = (
    events
    .withColumn("next_event_type", F.lead("event_type").over(w))
    .withColumn("next_dte_start", F.lead("dte_start").over(w))
    .withColumn("next_event_date", F.lead("event_date").over(w))
    .filter(F.col("event_type") == "i")
)

sign_sessions = (
    sign
    .withColumn(
        "dte_end",
        F.when(
            (F.col("next_event_type") == "o") &
            (F.col("event_date") == F.col("next_event_date")),
            F.col("next_dte_start")
        )
        .when(
            F.col("event_date") == today,
            now_ts
        )
        .otherwise(
            end_of_day("event_date")
        )
    )
    .withColumn(
        "duration_seconds",
        F.col("dte_end").cast("long") - F.col("dte_start").cast("long")
    )
    .select(
        "agent_id",
        F.col("rec_id").alias("rec_id_signed_in"),
        F.col("dte_start"),
        F.col("dte_end"),
        "profile",
        F.lit("i").alias("event_type_signed_in"),
        F.col("next_event_type").alias("event_type_signed_out"),
        "duration_seconds",
        "event_date"
    )
)

# Add columns for time 
from pyspark.sql import functions as F

sign_sessions = (
    sign_sessions
    .withColumn("dte_start_date", F.to_date("dte_start"))
    .withColumn("dte_start_time", F.date_format("dte_start", "HH:mm:ss"))
    .withColumn("dte_start_hour", F.hour("dte_start"))
    .withColumn("dte_start_minute", F.minute("dte_start"))
    .withColumn(
        "dte_start_index",
        F.col("dte_start_hour") * 100 + F.col("dte_start_minute")
    )
)

sign_sessions.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Pause duration 
pause = (
    events
    .withColumn("next_event_type", F.lead("event_type").over(w))
    .withColumn("next_dte_start", F.lead("dte_start").over(w))
    .withColumn("next_event_date", F.lead("event_date").over(w))
    .filter(F.col("event_type") == "p")
)

pause_sessions = (
    pause
    .withColumn(
        "dte_end",
        F.when(
            F.col("event_date") == F.col("next_event_date"),
            F.col("next_dte_start")
        )
        .when(F.col("event_date") == today, now_ts)
        .otherwise(end_of_day("event_date"))
    )
    .withColumn(
        "duration_seconds",
        F.col("dte_end").cast("long") - F.col("dte_start").cast("long")
    )
    .select(
        "agent_id",
        F.col("rec_id").alias("rec_id_pause_start"),
        F.col("dte_start"),
        F.col("dte_end"),
        "profile",
        F.lit("p").alias("event_type_pause_start"),
        F.col("next_event_type").alias("event_type_pause_end"),
        "duration_seconds",
        "event_date"
    )
    .withColumn("dte_start_date", F.to_date("dte_start"))
    .withColumn("dte_start_time", F.date_format("dte_start", "HH:mm:ss"))
    .withColumn("dte_start_hour", F.hour("dte_start"))
    .withColumn("dte_start_minute", F.minute("dte_start"))
    .withColumn("dte_start_index", F.col("dte_start_hour") * 100 + F.col("dte_start_minute"))
)

pause_sessions.show()
# print number of rows
print(f"Number of pause sessions: {pause_sessions.count()}")
# Print min of dte_start and max of dte_start
print(f"Min dte_start: {pause_sessions.agg(F.min('dte_start')).collect()[0][0]}, Max dte_start: {pause_sessions.agg(F.max('dte_start')).collect()[0][0]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# append into Puzzel Lakehouse
sign_sessions.write.mode("Overwrite").saveAsTable("dbo.Agent_events_signed_in_sessions")
pause_sessions.write.mode("Overwrite").saveAsTable("dbo.Agent_events_pause_durations")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
