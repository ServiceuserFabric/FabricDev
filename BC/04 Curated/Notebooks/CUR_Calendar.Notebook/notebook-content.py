# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

# Define year and month
reportingYear = 2025
reportingMonth = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run FM_Utility

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_table = f"Cur.Calendar"
calendar = spark.read.format("delta").table("Enr.enr_calendar")
reporting_period = spark.createDataFrame([(reportingYear, reportingMonth),], schema='reportingYear int, reportingMonth int')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Adding 'Current Week' Column

# CELL ********************

# Get today's date
today = current_date()

# Find the IsoWeek and Year for today's date in the calendar table
today_info = calendar.filter(col("Date") == today).select("IsoWeek", "Year").collect()

# Ensure we found a valid IsoWeek and Year
if today_info:
    current_iso_week = today_info[0]["IsoWeek"]
    current_iso_year = today_info[0]["Year"]

    # Add the new column based on matching IsoWeek and Year
    calendar = (calendar.withColumn(
        "WeekLabel",
        when((col("IsoWeek") == current_iso_week) & (col("Year") == current_iso_year), lit("Nuværende uge"))
        .otherwise(col("IsoWeek")))
        .withColumn('dayweekyearnum', concat(col("DayOfWeek"), col("IsoWeek"), col("IsoYear")))
        .withColumn("DayLabel", when(col("Date") == current_date(), lit("Nuværende dag"))
        .otherwise(col("Date")))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Adding current reporting month and year

# CELL ********************

result = calendar.crossJoin(reporting_period) \
    .withColumn("currentYear", expr("Year = reportingYear")) \
    .withColumn("currentMonth", expr("Year = reportingYear AND Month = reportingMonth")) \
    .withColumn("months_since_reporting_period", expr("(reportingYear - Year) * 12 + (reportingMonth - Month)"))\
    .withColumn("last12Months",expr("months_since_reporting_period BETWEEN 0 AND 11"))\
    .withColumn("upToCurrentMonth", expr("(Year < reportingYear) OR (Year = reportingYear AND Month <= reportingMonth)"))\
    .drop("reportingYear", "reportingMonth", "months_since_reporting_period")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# campaign_period = (
#     reporting_period
#       # shift every date forward 3 days
#     .withColumn("shifted_date", date_add(col("Date"), 3))
#       # extract ISO-week number and ISO-year from shifted_date
#     .withColumn("campaign_week", weekofyear(col("shifted_date")))
#     .withColumn("campaign_year", year(col("shifted_date")))
#       # optional: drop the intermediate column if you don’t need it
#     .drop("shifted_date")
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result.write.mode("overwrite").option("overwriteSchema","true").format("delta").saveAsTable(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
