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

# MARKDOWN ********************

# # StandardDate date type tables

# MARKDOWN ********************

# Add holidays to the calendar pr country in the bottom
# This code is from Fellowminds central repository 

# MARKDOWN ********************

# ## Shared Functions
# You must run this first!

# CELL ********************

%run FM_Utility

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_table   = "Enr.enr_calendar" 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# load packages
from itertools import chain

startdate_str = GlobalParameters.startdate 
enddate_str = GlobalParameters.enddate

# define boundaries
startdate = datetime.strptime(startdate_str, '%Y-%m-%d')
enddate = datetime.strptime(enddate_str, '%Y-%m-%d')
#enddate = (datetime.now() + timedelta(days=365 * 3)).replace(month=12, day=31)

# Add current week, day, and month labels for relative columns
today = current_date()
first_of_month = trunc(today, "month")
today_day = datetime.today().day

# Define reporting year and month 
reportingYear = GlobalParameters.reportingYear
reportingMonth = GlobalParameters.reportingMonth

# Danish → English month mapping
month_map = {
    "Januar": "January",
    "Februar": "February",
    "Marts": "March",
    "April": "April",
    "Maj": "May",
    "Juni": "June",
    "Juli": "July",
    "August": "August",
    "September": "September",
    "Oktober": "October",
    "November": "November",
    "December": "December",
}

# Create a Spark map literal for use in withColumn
month_map_expr = create_map([lit(x) for x in chain(*month_map.items())])

# define column names and its transformation rules on the Date column
column_rule_df = spark.createDataFrame([
    # Basic date columns
    ("PreviousDay", "date_sub(date, 1)"),
    ("NextDay", "date_add(date, 1)"),
    ("year", "year(date)"),
    ("IsoYear", "year(date)"),  # Simplified - full ISO year logic more complex
    ("IsLeapYear", "(year(date) % 4 = 0 AND year(date) % 100 != 0) OR (year(date) % 400 = 0)"),
    
    # Semester (half-year) columns
    ("Semester", "CASE WHEN month(date) <= 6 THEN 1 ELSE 2 END"),
    ("SemesterCaption", "CASE WHEN month(date) <= 6 THEN 'H1' ELSE 'H2' END"),
    ("YearSemesterCaption", "concat(year(date), '-', CASE WHEN month(date) <= 6 THEN 'H1' ELSE 'H2' END)"),
    
    # Quarter columns
    ("Quarter", "quarter(date)"),
    ("QuarterCaption", "concat('Q', quarter(date))"),
    ("YearQuarterCaption", "concat(year(date), '-Q', quarter(date))"),
    ("FirstDayOfQuarter", """
        CASE 
            WHEN quarter(date) = 1 THEN make_date(year(date), 1, 1)
            WHEN quarter(date) = 2 THEN make_date(year(date), 4, 1)
            WHEN quarter(date) = 3 THEN make_date(year(date), 7, 1)
            WHEN quarter(date) = 4 THEN make_date(year(date), 10, 1)
        END
    """),
    ("LastDayOfQuarter", """
        CASE 
            WHEN quarter(date) = 1 THEN make_date(year(date), 3, 31)
            WHEN quarter(date) = 2 THEN make_date(year(date), 6, 30)
            WHEN quarter(date) = 3 THEN make_date(year(date), 9, 30)
            WHEN quarter(date) = 4 THEN make_date(year(date), 12, 31)
        END
    """),
    
    # Month columns
    ("Month", "month(date)"),
    ("MonthName", """
        CASE 
            WHEN month(date) = 1 THEN 'Januar'
            WHEN month(date) = 2 THEN 'Februar'
            WHEN month(date) = 3 THEN 'Marts'
            WHEN month(date) = 4 THEN 'April'
            WHEN month(date) = 5 THEN 'Maj'
            WHEN month(date) = 6 THEN 'Juni'
            WHEN month(date) = 7 THEN 'Juli'
            WHEN month(date) = 8 THEN 'August'
            WHEN month(date) = 9 THEN 'September'
            WHEN month(date) = 10 THEN 'Oktober'
            WHEN month(date) = 11 THEN 'November'
            WHEN month(date) = 12 THEN 'December'
        END
    """),
    ("MonthAbbr", """
        CASE 
            WHEN month(date) = 1 THEN 'Jan'
            WHEN month(date) = 2 THEN 'Feb'
            WHEN month(date) = 3 THEN 'Mar'
            WHEN month(date) = 4 THEN 'Apr'
            WHEN month(date) = 5 THEN 'Maj'
            WHEN month(date) = 6 THEN 'Jun'
            WHEN month(date) = 7 THEN 'Jul'
            WHEN month(date) = 8 THEN 'Aug'
            WHEN month(date) = 9 THEN 'Sep'
            WHEN month(date) = 10 THEN 'Okt'
            WHEN month(date) = 11 THEN 'Nov'
            WHEN month(date) = 12 THEN 'Dec'
        END
    """),
    ("FirstDayOfMonth", "trunc(date, 'month')"),
    ("LastDayOfMonth", "last_day(date)"),
    ("YearMonthCaption", "date_format(date, 'yyyy-MM')"),
    
    # Week columns
    ("Week", "weekofyear(date)"),
    ("YearWeekCaption", "concat(year(date), '-W', lpad(weekofyear(date), 2, '0'))"),
    ("IsoWeek", "weekofyear(date)"),  # Simplified - true ISO week calculation is more complex
    ("IsoYearWeekCaption", "concat(year(date), '-W', lpad(weekofyear(date), 2, '0'))"),
    
    # Day columns
    ("DayOfYear", "dayofyear(date)"),
    ("DayOfMonth", "dayofmonth(date)"),
    ("DayOfWeek", "dayofweek(date)"),
    ("IsoWeekday", "dayofweek(date)"),  # Simplified - ISO uses 1=Monday
    ("WeekdayName", """
        CASE 
            WHEN dayofweek(date) = 1 THEN 'Søndag'
            WHEN dayofweek(date) = 2 THEN 'Mandag'
            WHEN dayofweek(date) = 3 THEN 'Tirsdag'
            WHEN dayofweek(date) = 4 THEN 'Onsdag'
            WHEN dayofweek(date) = 5 THEN 'Torsdag'
            WHEN dayofweek(date) = 6 THEN 'Fredag'
            WHEN dayofweek(date) = 7 THEN 'Lørdag'
        END
    """),
    ("IsWeekend", "dayofweek(date) IN (1, 7)")  # Sunday=1, Saturday=7
], ["new_column_name", "expression"])

start = int(startdate.timestamp())
stop = int(enddate.timestamp())
df = spark.range(start, stop, 60*60*24).select(
    col("id").cast("timestamp").cast("date").alias("Date")
)

# this loops over all rules defined in column_rule_df adding the new columns
for row in column_rule_df.collect():
    new_column_name = row["new_column_name"]
    expression = expr(row["expression"])
    df = df.withColumn(new_column_name, expression)

# Add English month name column
df = df.withColumn("MonthNameEnglish", month_map_expr[col("MonthName")])

# Find the IsoWeek and Year for today's date
today_info = df.filter(col("Date") == today).select("IsoWeek", "year", "Month").collect()

if today_info:
    current_iso_week = today_info[0]["IsoWeek"]
    current_iso_year = today_info[0]["year"]
    current_iso_month = today_info[0]["Month"]
    
    # # Determine fiscal month
    # fiscal_month = current_iso_month - 1 if current_iso_month > 1 else 12
    # use_fiscal_month = fiscal_month if today_day <= 30 else current_iso_month
    # first_of_month_fiscal = add_months(first_of_month, -1) if today_day <= 30 else first_of_month
    
    # Add label and calculation columns
    df = (df
        .withColumn("WeekLabel", 
            when((col("IsoWeek") == current_iso_week) & (col("year") == current_iso_year), 
                 lit("Nuværende uge"))
            .otherwise(col("IsoWeek")))
        .withColumn("dayweekyearnum", 
            concat(col("DayOfWeek"), col("IsoWeek"), col("IsoYear")))
        .withColumn("yearmonthdaynum", 
            concat(col("year"), 
                   when(col("Month") < 10, concat(lit("0"), col("Month"))).otherwise(col("Month")), 
                   when(col("DayOfMonth") < 10, concat(lit("0"), col("DayOfMonth"))).otherwise(col("DayOfMonth"))))
        .withColumn("DayLabel", 
            when(col("Date") == current_date(), lit("Nuværende dag"))
            .otherwise(col("Date")))
        .withColumn("MonthLabel", 
            when((col("Month") == current_iso_month) & (col("year") == current_iso_year), 
                 lit("Indeværende"))
            .otherwise(col("MonthNameEnglish")))
        # .withColumn("FiscalMonthLabel",
        #     when((col("Month") == use_fiscal_month) & (col("year") == current_iso_year), 
        #          lit("Indeværende"))
        #     .otherwise(col("MonthNameEnglish")))
        .withColumn("RelativeMonth",
            round(months_between(col("FirstDayOfMonth"), first_of_month, roundOff=True)).cast("int"))
        # .withColumn("FiscalRelativeMonth",
        #     round(months_between(col("FirstDayOfMonth"), first_of_month_fiscal, roundOff=True)).cast("int"))
    )

reporting_period = spark.createDataFrame(
    [(reportingYear, reportingMonth)], 
    schema='reportingYear int, reportingMonth int'
)

# Add reporting period flags
result = df.crossJoin(reporting_period) \
    .withColumn("currentYear", expr("year = reportingYear")) \
    .withColumn("currentMonth", expr("year = reportingYear AND Month = reportingMonth")) \
    .withColumn("months_since_reporting_period", 
                expr("(reportingYear - year) * 12 + (reportingMonth - Month)")) \
    .withColumn("last12Months", 
                expr("months_since_reporting_period BETWEEN 0 AND 11")) \
    .withColumn("upToCurrentMonth", 
                expr("(year < reportingYear) OR (year = reportingYear AND Month <= reportingMonth)")) \
    .drop("reportingYear", "reportingMonth", "months_since_reporting_period")



# Reorder columns to match target structure with additional columns
result = result.select(
    "Date",
    "PreviousDay",
    "NextDay",
    "year",
    "IsoYear",
    "IsLeapYear",
    "Semester",
    "SemesterCaption",
    "YearSemesterCaption",
    "Quarter",
    "QuarterCaption",
    "YearQuarterCaption",
    "FirstDayOfQuarter",
    "LastDayOfQuarter",
    "Month",
    "MonthName",
    "MonthNameEnglish",
    "MonthAbbr",
    "FirstDayOfMonth",
    "LastDayOfMonth",
    "YearMonthCaption",
    "Week",
    "YearWeekCaption",
    "IsoWeek",
    "IsoYearWeekCaption",
    "DayOfYear",
    "DayOfMonth",
    "DayOfWeek",
    "IsoWeekday",
    "WeekdayName",
    "IsWeekend",
    "WeekLabel",
    "DayLabel",
    "MonthLabel",
    #"FiscalMonthLabel",
    "dayweekyearnum",
    "yearmonthdaynum",
    "RelativeMonth",
    #"FiscalRelativeMonth",
    "currentYear",
    "currentMonth",
    "last12Months",
    "upToCurrentMonth"
)

#display(result)



# Optional: Save as Delta table
# target_table = "Cur.Calendar"
# result.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(result.filter((col("yearmonthdaynum") >= 20240101) & (col("yearmonthdaynum") < 20260201)).orderBy(col("yearmonthdaynum")))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

result.write.mode("overwrite").option("overwriteSchema","true").format("delta").saveAsTable(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
