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
# META           "id": "836bc8da-2d44-4096-be88-36e2f91bbf80"
# META         },
# META         {
# META           "id": "b1df83e8-4963-4129-b9a8-db8ced7800dd"
# META         },
# META         {
# META           "id": "dcddaf9e-0415-4d65-894b-104157b74950"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "3775f688-b7ee-4481-94ab-4e2963abb53d",
# META       "known_warehouses": [
# META         {
# META           "id": "3775f688-b7ee-4481-94ab-4e2963abb53d",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#__________________________________________________________________________________
#                             HEADER                                           ----  

# Script Name:                Servicekald.py
# Description:                A script to make lists for service calls. 
#                             There are three types of service calls: Privat, Erhverv and Migrering.

# Dependencies:               Dependencies to other scripts
# Input:                      Golden Lakehouse 
#                             Bronze Lakehouse (for contact information)
# Output:                     Servicekald_Privat_YYYYMMDD.csv
#                             Servicekald_Erhverv_YYYYMMDD.csv
#                             Servicekald_Migrering_YYYYMMDD.csv
# Created by:                 Mathias Liedtke
# Created date:               21/01/2026
# Last Modified by:           Author modified date
# Last Modified date:         Date modified
# Comments: 
#__________________________________________________________________________________

# Load Libraries ##################################################################
from datetime import datetime # For date handling
from datetime import timedelta # To adjust for number of days
import pyodbc # For database connection. Otherwise use pip install pyodbc
import pandas as pd # For data manipulation. Otherwise use pip install pandas
import os # For file path handling
from notebookutils import mssparkutils # To handle exit message for pipeline
import json # To handle exit message for pipeline



# User defined variables ###########################################################
# Define your variables here
dToday = datetime.now()
dToday = dToday.date() # - timedelta(days=1)
# For testing, set a manual date
# dToday = datetime(2026, 3, 9).date()

# Is monday
# is_monday = ((dToday + timedelta(days=1)).weekday()) == 0
# print(is_monday)

# From date. If today is monday, subtract 9 days, else subtract 8 days
dDate = dToday - timedelta(days=8)
# dFromDate = dToday - timedelta(days=8) # if is_monday else dToday - timedelta(days=7)

# Subtract 8 days from today and format as date only without time
# dTodate = dToday - timedelta(days=7)
# Ensure dFromDate is not equal to dTodate othwerwise SQL query will fail for 'between' statement
#dFromDate = dFromDate if dFromDate != dTodate else dFromDate - timedelta(days=1)
print("Todays date:", dToday)
print("Date:", dDate)
# print("Is it Monday today?:", is_monday)
# print("From date:", dFromDate)
# print("To date:", dTodate)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# set connection to golden lakehouse 
spark.catalog.setCurrentDatabase("golden_lakehouse")

# Show tables in Golden Lakehouse
spark.sql("SHOW TABLES").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data ########################################################################
## Panther ----
# Connect to Fabrics sql endpoint 

query_Golden_Lakehouse = f"""
WITH ranked AS (
    SELECT
        C.name,
        ph.accountKey,
        ph.KundeNO,
        PC.ProductMainGroup,
        GC.fullPath,
        ph.activeProvisioning,
        ROW_NUMBER() OVER (
            PARTITION BY ph.accountKey
            ORDER BY
                CASE WHEN ph.KundeNO != '' THEN 0 ELSE 1 END,
                ph.KundeNO
        ) AS rn
    FROM ProductHistory AS ph
    INNER JOIN Product_config AS PC
        ON PC.id = ph.ProduktID
    INNER JOIN Customer AS C
        ON ph.accountKey = C.accounts_id
    INNER JOIN Cust_Group AS GC
        ON GC.id = C.customerGroupKey
    WHERE
        ph.FromDate <= DATE '{dToday.isoformat()}'
        AND ph.ToDate  > DATE '{dToday.isoformat()}'
        AND ph.startDate = DATE '{dDate.isoformat()}'
        AND ph.Kunde_Status = 'Aktive'
        AND ph.NySalg = '1'
        AND (
            (PC.HovedProduktGruppe = 'Internet' AND PC.productGroup != 'Migrering')
            OR
            (PC.HovedProduktGruppe = 'TV' AND PC.ProductMainGroup = 'TVSA')
        )
        AND (
            GC.fullPath LIKE '%Erhverv%'
            OR GC.fullPath LIKE '%Privat%'
            OR GC.fullPath LIKE '%Medarbejderbredbånd%'
        )
)

SELECT
    name,
    accountKey,
    KundeNO,
    ProductMainGroup,
    fullPath,
    SUM(CAST(activeProvisioning AS INT)) AS sumActiveProvisioning
FROM ranked
WHERE
    rn = 1
    AND KundeNO IS NOT NULL
GROUP BY
    name,
    accountKey,
    KundeNO,
    ProductMainGroup,
    fullPath
HAVING
    SUM(CAST(activeProvisioning AS INT)) = 1
"""

query_Churn_Kunder = f"""
SELECT
    KundeNO
FROM ProductHistory
INNER JOIN Product_config AS PC ON PC.id = ProductHistory.ProduktID
WHERE PC.Internet_speed is not null and
    Partner IS NOT NULL  
    AND FromDate <= DATE '{dToday.isoformat()}'
    AND ToDate > DATE '{dToday.isoformat()}' and endDate >= DATE '{dToday.isoformat()}'
GROUP BY
    KundeNO
HAVING
    MAX(CASE WHEN endDate IS NOT NULL THEN 1 ELSE 0 END) = 1
AND MAX(CASE WHEN endDate IS NULL THEN 1 ELSE 0 END) = 0;"""


Golden_Lakehouse = spark.sql(query_Golden_Lakehouse)
Golden_Lakehouse.show(100, truncate=False)

Golden_Lakehouse = spark.sql(query_Golden_Lakehouse)

Golden_Lakehouse_Churn = spark.sql(query_Churn_Kunder)
Golden_Lakehouse_Churn.show(100, truncate=False)
# Print rows
print(f"Number of rows: {Golden_Lakehouse.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# set schema 
spark.catalog.setCurrentDatabase("bronze_lakehouse")

# Show tables available
spark.sql("SHOW TABLES").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Return all values from status.accountkey to filter for bronze lakehouse and use as variable for the query below

account_keys = [r["accountKey"] for r in Golden_Lakehouse.select("accountKey").distinct().collect()]

# Hvis ingen nye kunder, så afsluttes scriptet
if Golden_Lakehouse.isEmpty():
    print("No data found in Golden Lakehouse for the given criteria. Exiting script.")
    raise SystemExit(0)
# Print keys 
print("Account keys to filter for Bronze Lakehouse:", account_keys)

# Konverter til df
keys_df = Golden_Lakehouse.selectExpr("CAST(accountKey AS STRING) AS accountKey").distinct()

# Laver en midlertidig tabel, som joines på, for at få udsnit af data for nye kunder og ikke alle. 
bronze_df = spark.table("all_data_customer_table") \
    .withColumnRenamed("accounts_id", "accounts_id_raw")

# Ensure same datatype for join
bronze_df = bronze_df.selectExpr(
    "CAST(accounts_id_raw AS STRING) AS accounts_id",
    "accounts_email",
    "accounts_mobile",
    "mobile",
    "startDate",
    "customAttributes_created",
    "customAttributes_firstactivedate"
)

filtered = bronze_df.join(keys_df, bronze_df.accounts_id == keys_df.accountKey, "inner").drop("accountKey")
filtered.show(20, truncate=False)


from pyspark.sql import functions as F
# Definer migreringsdatoer for at skelne på typer af migreringer. 
migration_dates = [
    "2025-11-10",
    "2025-11-17",
    "2025-11-19",
    "2025-12-10",
    "2025-12-15"
]

result = (
    filtered
    .withColumn("startDate_d", F.to_date("startDate"))
    .withColumn(
        "Kundetype",
        F.when(
            (F.col("customAttributes_created") != "") & (F.col("startDate_d").isin(migration_dates)),
            F.lit("ON MIGRERING")
        ).when(
            (F.col("customAttributes_created") != "") & (~F.col("startDate_d").isin(migration_dates)),
            F.lit("PARTNERNET MIGRERING")
        ).otherwise(F.lit("Panther"))
    )
    .drop("startDate_d")
)

result.show(500, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import Window from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Manipulate data to prepare for dialler output  
merged = (
    Golden_Lakehouse.alias("g")
    .join(
        result.alias("b"),
        F.col("g.accountKey").cast("string") == F.col("b.accounts_id").cast("string"),
        "left"
    )
)

# =========================================
# 7) Phone cleaning rules 
# =========================================

# accounts_mobile = accounts_mobile if present, else use mobile
merged = merged.withColumn(
    "accounts_mobile",
    F.coalesce(F.col("b.accounts_mobile"), F.col("b.mobile"))
)

# Trim spaces
merged = merged.withColumn("accounts_mobile", F.trim(F.col("accounts_mobile")))

# If starts with '+', replace '+' with '00' (prefix only)
merged = merged.withColumn(
    "accounts_mobile",
    F.when(
        F.col("accounts_mobile").startswith("+"),
        F.concat(F.lit("00"), F.substring(F.col("accounts_mobile"), 2, 1000))
    ).otherwise(F.col("accounts_mobile"))
)

# If length == 8, prefix '0045'
merged = merged.withColumn(
    "accounts_mobile",
    F.when(
        F.length(F.col("accounts_mobile")) == 8,
        F.concat(F.lit("0045"), F.col("accounts_mobile"))
    ).otherwise(F.col("accounts_mobile"))
)

# Keep only rows where accounts_mobile starts with '0045'
merged = merged.filter(F.col("accounts_mobile").startswith("0045"))

# Drop duplicates on phone number deterministically:
# Keep the lowest KundeNO per phone (change orderBy if you prefer something else)
w = Window.partitionBy("accounts_mobile").orderBy(F.col("g.KundeNO").asc_nulls_last())
merged = merged.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

# Do an antijoin from Golden_Lakehouse_Churn on KundeNO to exclude churned customers
merged = merged.join(
    Golden_Lakehouse_Churn.select("KundeNO").withColumnRenamed("KundeNO", "churn_KundeNO"),
    merged["g.KundeNO"] == F.col("churn_KundeNO"),
    "left_anti"
)

Merged_Data = merged.cache()
print("Merged_Data sample:")
Merged_Data.show(20, truncate=False)


# =========================================
# 8) Split into Privat / Migrering / Erhverv
# =========================================
Privat_Data = Merged_Data.filter(
    F.col("g.fullPath").contains("Privat") & (F.col("b.Kundetype") != "PARTNERNET MIGRERING" & F.col("b.Kundetype") != "ON MIGRERING")
)

Migrering_Data = Merged_Data.filter(
    F.col("g.fullPath").contains("Privat") & (F.col("b.Kundetype") == "PARTNERNET MIGRERING" & F.col("b.Kundetype") == "ON MIGRERING")
)

Erhverv_Data = Merged_Data.filter(
    F.col("g.fullPath").contains("Erhverv")
)


# =========================================
def format_output(df):
    return df.select(
        F.col("accounts_mobile").alias("Phone_num"),
        F.col("g.KundeNO").alias("var1"),
        F.col("b.accounts_email").alias("var2"),
        F.col("g.name").alias("var3")
    )

Privat_Out    = format_output(Privat_Data)
Migrering_Out = format_output(Migrering_Data)
Erhverv_Out   = format_output(Erhverv_Data)

print("Privat_Out:")
Privat_Out.show(100, truncate=False)

print("Migrering_Out:")
Migrering_Out.show(20, truncate=False)

print("Erhverv_Out:")
Erhverv_Out.show(20, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Here we change spark df to csv

from pyspark.sql import functions as F
from notebookutils import mssparkutils
import re

def write_single_csv(df, target_dir, file_name, header=True, sep=";"):
    """
    Writes df as a single CSV file in target_dir/file_name by:
      1) writing to a temp folder
      2) finding the single part-*.csv
      3) moving/renaming it to the desired file_name
      4) cleaning temp folder
    """
    temp_dir = f"{target_dir}/_tmp_{file_name}"
    
    # 1) write to temp (single partition)
    (df.coalesce(1)
       .write.mode("overwrite")
       .option("header", str(header).lower())
       .option("sep", sep)
       .option("quote", "\"")
       .option("escape", "\"")
       .csv(temp_dir)
    )

    # 2) locate part file
    files = mssparkutils.fs.ls(temp_dir)
    part = [f.path for f in files if re.search(r"/part-.*\.csv$", f.path)]
    if not part:
        raise Exception(f"No part file found in {temp_dir}. Files: {[f.path for f in files]}")
    part_path = part[0]

    # 3) move/rename to final location
    final_path = f"{target_dir}/{file_name}"
    mssparkutils.fs.mv(part_path, final_path, True)

    # 4) cleanup temp folder (includes _SUCCESS etc.)
    mssparkutils.fs.rm(temp_dir, True)

    return final_path



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Example target in Lakehouse Files (adjust to your lakehouse)
# Load to Puzzel_Altibox Lakehouse
from datetime import datetime

run_date = datetime.now().strftime("%Y-%m-%d")  # e.g. 2026-02-25
base_dir = f"Files/Puzzel_Dialler/{run_date}"

privat_path    = write_single_csv(Privat_Out,    base_dir, "Privat_Out.csv")
migrering_path = write_single_csv(Migrering_Out, base_dir, "Migrering_Out.csv")
erhverv_path   = write_single_csv(Erhverv_Out,   base_dir, "Erhverv_Out.csv")

print(privat_path, migrering_path, erhverv_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
