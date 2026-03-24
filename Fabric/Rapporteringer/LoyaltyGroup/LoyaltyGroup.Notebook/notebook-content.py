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
# META         },
# META         {
# META           "id": "b1df83e8-4963-4129-b9a8-db8ced7800dd"
# META         },
# META         {
# META           "id": "8c6daa7e-25e3-49f4-8f00-7ba8daf52bc3"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "e4125510-af74-9fbf-4e0b-acffe53bd032",
# META       "known_warehouses": [
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

# Script Name:                oyaltyGroup
# Description:                A script to make a list for LoyaltyGroup

# Dependencies:               Dependencies to other scripts
# Input:                      Golden Lakehouse 
#                             Bronze Lakehouse (for contact information)
# Output:                     
# Created by:                 Mathias Liedtke
# Created date:               23/03/2026
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
# dToday = datetime(2026, 3, 21).date()

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
spark.catalog.setCurrentDatabase("Golden_lakehouse")

# Show tables in Golden Lakehouse
spark.sql("SHOW TABLES").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data ########################################################################
## Panther: Her henter vi oplysninger om nysalgskunder på internet ved nedenstående SQL. 
# Connect to Fabrics sql endpoint 

query_Golden_Lakehouse_Internet = f"""
SELECT 
PH.KundeNO, 
C.name,
PH.accountKey,
PH.Produkt_Status,
PH.ProduktID,
PC.selfserviceNames_da,
C.accounts_address, 
C.Zip,
C.City                                        
FROM ProductHistory AS PH
LEFT JOIN Product_config as PC on PC.id = PH.ProduktID
LEFT JOIN Customer AS C ON C.accounts_id = PH.accountKey                                        
WHERE PH.FromDate <= DATE '{dToday.isoformat()}' and PH.ToDate > DATE '{dToday.isoformat()}' and Produkt_Status = 'I brug' and PC.HovedProduktGruppe = 'Internet' and PC.productGroup != 'Migrering'
"""

Golden_Lakehouse_Internet = spark.sql(query_Golden_Lakehouse_Internet)
Golden_Lakehouse_Internet.show(100, truncate=False)
print(f"Number of rows: {Golden_Lakehouse_Internet.count()}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data ########################################################################
## Panther: Her henter vi oplysninger om nysalgskunder på tv ved nedenstående SQL. 
# Connect to Fabrics sql endpoint 

query_Golden_Lakehouse_TV = f"""
SELECT 
PH.KundeNO, 
C.name,
PH.accountKey,
PH.Produkt_Status,
PH.ProduktID,
PC.selfserviceNames_da,
C.accounts_address, 
C.Zip,
C.City                                        
FROM ProductHistory AS PH
LEFT JOIN Product_config as PC on PC.id = PH.ProduktID
LEFT JOIN Customer AS C ON C.accounts_id = PH.accountKey                                        
WHERE PH.FromDate <= DATE '{dToday.isoformat()}' and PH.ToDate > DATE '{dToday.isoformat()}' and Produkt_Status = 'I brug' and PC.HovedProduktGruppe = 'TV'
"""


Golden_Lakehouse_TV = spark.sql(query_Golden_Lakehouse_TV)
Golden_Lakehouse_TV.show(100, truncate=False)
print(f"Number of rows: {Golden_Lakehouse_TV.count()}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data ########################################################################
## Panther: Her henter vi oplysninger om produktkonfigurationen
# Connect to Fabrics sql endpoint 

query_Golden_Lakehouse_PC = f"""
SELECT *
FROM Product_config
"""


Golden_Lakehouse_PC = spark.sql(query_Golden_Lakehouse_PC)
Golden_Lakehouse_PC.show(100, truncate=False)
print(f"Number of rows: {Golden_Lakehouse_PC.count()}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Her skifter vi til at hente data fra bronze
# set schema 
spark.catalog.setCurrentDatabase("Bronze_lakehouse")

# Show tables available
spark.sql("SHOW TABLES").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data ########################################################################
## Panther: Her henter vi oplysninger om kunden
# Connect to Fabrics sql endpoint 

query_Bronze_Lakehouse_Email = f"""
SELECT 
C.accounts_id, 
C.accounts_email,
CG.name as customerGroupName, 
C.customerGroupKey, 
CG.fullPath                                         
FROM all_data_customer_table as C
left join all_data_customer_groups as CG on CG.id = C.customerGroupKey
"""


Bronze_Lakehouse_Email = spark.sql(query_Bronze_Lakehouse_Email)
Bronze_Lakehouse_Email.show(100, truncate=False)
print(f"Number of rows: {Bronze_Lakehouse_Email.count()}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data ########################################################################
## Panther: Her henter vi oplysninger om alder
# Connect to Fabrics sql endpoint 

query_Bronze_Lakehouse_Customer = f"""
SELECT
    accounts_id,
    customAttributes_birthday
FROM all_data_customer_table
WHERE try_cast(trim(customAttributes_birthday) AS DATE) IS NOT NULL
  AND try_cast(trim(customAttributes_birthday) AS DATE) > DATE '1900-01-01'
  AND customAttributes_birthday != 'N/A'
"""


Bronze_Lakehouse_Customer = spark.sql(query_Bronze_Lakehouse_Customer)
Bronze_Lakehouse_Customer.show(100, truncate=False)
print(f"Number of rows: {Bronze_Lakehouse_Customer.count()}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data ########################################################################
## Panther: Her henter vi oplysninger om teknologitype
# Connect to Fabrics sql endpoint 

query_Bronze_Lakehouse_ProvModel = f"""
SELECT accountKey,productConf_provisioningModel

  FROM all_data_cust_prod -- Nedenstående teknologityper begrænser antallet og sikrer mere korrekt join. 
  where productConf_provisioningModel in ('opennet_fiber', 'netco_wholesale_fiber', 'wholesale_coax')
"""


Bronze_Lakehouse_ProvModel = spark.sql(query_Bronze_Lakehouse_ProvModel)
Bronze_Lakehouse_ProvModel.show(100, truncate=False)
print(f"Number of rows: {Bronze_Lakehouse_ProvModel.count()}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Henter informatioer på mobilkunder
# set connection to golden warehouse
df = spark.read.synapsesql(
    "Golden_warehouse.nuu.AltiboxMobil_Nuuday"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rapporteringer_path = (
    "abfss://6b06974a-4346-4a38-bc5a-d42e564a6bec@onelake.dfs.fabric.microsoft.com/8c6daa7e-25e3-49f4-8f00-7ba8daf52bc3/Tables/dbo/LoyaltyGroup"
)

df_rapporteringer = (
    spark.read
    .format("delta")
    .load(rapporteringer_path)
)

df_rapporteringer.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Checker størrelsen på data frames for at sammenligne script i python 

print("Internet rows:", Golden_Lakehouse_Internet.count())
print("TV rows:", Golden_Lakehouse_TV.count())

# Der er flere rækker i data frames end i Python 
join_keys = ['KundeNO', 'accounts_address', 'Zip', 'City']
internet_dedup = Golden_Lakehouse_Internet.dropDuplicates(join_keys)
print(f"Number of rows: {internet_dedup.count()}")
tv_dedup       = Golden_Lakehouse_TV.dropDuplicates(join_keys)
print(f"Number of rows: {tv_dedup.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# -----------------------------
# Helper: rename non-key columns with suffix to avoid collisions after joins
# -----------------------------
def suffix_columns(df, keys, suffix):
    cols = []
    for c in df.columns:
        if c in keys:
            cols.append(F.col(c))
        else:
            cols.append(F.col(c).alias(f"{c}{suffix}"))
    return df.select(*cols)

# -----------------------------
# 1) Outer join Internet + TV on keys
# -----------------------------
join_keys = ['KundeNO', 'accounts_address', 'Zip', 'City']

internet_s = suffix_columns(Golden_Lakehouse_Internet, join_keys, "_Internet")
tv_s       = suffix_columns(Golden_Lakehouse_TV,       join_keys, "_TV")

Golden_Lakehouse_Products = internet_s.join(tv_s, on=join_keys, how="outer")

# Delete rows where KundeNO is "null"
Golden_Lakehouse_Products = Golden_Lakehouse_Products.filter(F.col("KundeNO") != "null")
Golden_Lakehouse_Products = Golden_Lakehouse_Products.filter(F.col("KundeNO").isNotNull())
# Delete duplicate rows
Golden_Lakehouse_Products = Golden_Lakehouse_Products.dropDuplicates()
# Order by accountKey_Internet
Golden_Lakehouse_Products.show(50, truncate=False)

print(f"Number of rows: {Golden_Lakehouse_Products.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------
# 2) Join emails twice (Internet accountKey + TV accountKey), then coalesce
# -----------------------------
# Select only what you need from email table to keep it clean
email_sel = Bronze_Lakehouse_Email.select(
    F.col("accounts_id").alias("accounts_id"),
    F.col("accounts_email").alias("accounts_email"),
    F.col("customerGroupName").alias("customerGroupName"),
    F.col("customerGroupKey").alias("customerGroupKey"),
    F.col("fullPath").alias("fullPath")
)

email_i = (email_sel
           .withColumnRenamed("accounts_id", "accounts_id_i")
           .withColumnRenamed("accounts_email", "accounts_email_i")
           .withColumnRenamed("customerGroupName", "customerGroupName_i")
           .withColumnRenamed("customerGroupKey", "customerGroupKey_i")
           .withColumnRenamed("fullPath", "fullPath_i"))

email_t = (email_sel
           .withColumnRenamed("accounts_id", "accounts_id_t")
           .withColumnRenamed("accounts_email", "accounts_email_t")
           .withColumnRenamed("customerGroupName", "customerGroupName_t")
           .withColumnRenamed("customerGroupKey", "customerGroupKey_t")
           .withColumnRenamed("fullPath", "fullPath_t"))

Merge_Golden_Bronze = (Golden_Lakehouse_Products
    .join(email_i, F.col("accountKey_Internet") == F.col("accounts_id_i"), "left")
    .join(email_t, F.col("accountKey_TV")       == F.col("accounts_id_t"), "left")
)

# Coalesce fields
Merge_Golden_Bronze = (Merge_Golden_Bronze
    .withColumn("Email", F.coalesce(F.col("accounts_email_i"), F.col("accounts_email_t")))
    .withColumn("name",  F.coalesce(F.col("name_TV"), F.col("name_Internet")))
    .withColumn("customerGroupName", F.coalesce(F.col("customerGroupName_t"), F.col("customerGroupName_i")))
    .withColumn("customerGroupKey",  F.coalesce(F.col("customerGroupKey_i"),  F.col("customerGroupKey_t")))
    .withColumn("fullPath",          F.coalesce(F.col("fullPath_i"),          F.col("fullPath_t")))
    .withColumn("accountKey",        F.coalesce(F.col("accountKey_Internet"), F.col("accountKey_TV")))
)

Merge_Golden_Bronze.show(20, truncate=False)
print(f"Number of rows:{Merge_Golden_Bronze.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------
# 3) Select relevant columns for df_Loyalty
# -----------------------------
df_Loyalty = Merge_Golden_Bronze.select(
    "accountKey",
    "KundeNO",
    "name",
    "Email",
    "selfserviceNames_da_Internet",
    "ProduktID_Internet",
    "ProduktID_TV",
    "selfserviceNames_da_TV",
    "Zip",
    "City",
    "customerGroupName",
    "fullPath"
)

df_Loyalty.show(100, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------
# 4) Join technology info on accountKey (Bronze_Lakehouse_Cust_Prod_All)
# -----------------------------
df_Loyalty = df_Loyalty.join(
    Bronze_Lakehouse_ProvModel,  # assumes it has accountKey + productConf_provisioningModel
    on="accountKey",
    how="left"
)

df_Loyalty.show(100, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------
# 5) Join Premium/Light + Speed from Product Config (Golden_Lakehouse_PC)
# -----------------------------
pc_sel = Golden_Lakehouse_PC.select(
    F.col("id").alias("pc_id"),
    F.col("Internet_category"),
    F.col("Internet_speed")
)

df_Loyalty = df_Loyalty.join(
    pc_sel,
    df_Loyalty["ProduktID_Internet"] == pc_sel["pc_id"],
    "left"
).drop("pc_id")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------
# 6) Filter out business/employee/association/test groups by fullPath contains
# -----------------------------
exclude_regex = r"(?i)(Erhverv|Medarbejderbredbånd|Forening|Test Kunder|Altibox Medarbejder|Fakturakontoer FB|Sponsor TV)"
df_Loyalty = df_Loyalty.filter(~F.col("fullPath").rlike(exclude_regex))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------
# 7) Join birthday + compute age (Kundealder)
# -----------------------------
cust_sel = Bronze_Lakehouse_Customer.select(
    F.col("accounts_id").alias("accounts_id"),
    F.col("customAttributes_birthday").alias("customAttributes_birthday")
)

df_Loyalty = df_Loyalty.join(
    cust_sel,
    df_Loyalty["accountKey"] == cust_sel["accounts_id"],
    "left"
).drop("accounts_id")

# Robust date parse (add formats if needed)
birthday_raw = F.trim(F.col("customAttributes_birthday"))
birthday_date = F.coalesce(
    F.to_date(birthday_raw, "yyyy-MM-dd"),
    F.to_date(birthday_raw, "dd-MM-yyyy"),
    F.to_date(birthday_raw, "MM/dd/yyyy")
)

df_Loyalty = (df_Loyalty
    .withColumn("birthday_date", birthday_date)
    .withColumn(
        "Kundealder",
        F.when(F.col("birthday_date").isNotNull() & (F.col("birthday_date") > F.lit("1900-01-01").cast("date")),
               F.floor(F.datediff(F.current_date(), F.col("birthday_date")) / F.lit(365))
        ).otherwise(F.lit(None).cast("int"))
    )
    .drop("birthday_date")
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------
# 8) Keep first name only
# -----------------------------
df_Loyalty = df_Loyalty.withColumn(
    "name",
    F.when(F.col("name").isNotNull(), F.split(F.col("name"), " ").getItem(0)).otherwise(F.col("name"))
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------
# 9) Add Produkttype_Mobil = null
# -----------------------------
df_Loyalty = df_Loyalty.withColumn("Produkttype_Mobil", F.lit(None).cast("string"))

df_Loyalty.show(100, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------
# 10) Select output columns in order + rename
# -----------------------------
df_Loyalty = df_Loyalty.select(
    F.col("KundeNO").alias("KundeID"),
    F.col("name").alias("Kundenavn"),
    F.col("Email"),
    F.col("Internet_category").alias("Produkttype_Internet"),
    F.col("productConf_provisioningModel").alias("Produkttype_Technology"),
    F.col("Internet_speed").alias("Produkttype_Speed"),
    F.col("selfserviceNames_da_TV").alias("Produkttype_TV"),
    F.col("Produkttype_Mobil"),
    F.col("Kundealder"),
    F.col("customerGroupName").alias("Netejer"),
    F.col("Zip").alias("Postnummer")
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------
# 11) Fill Produkttype_Technology if null and Produkttype_Internet not null
#     - if Netejer contains 'ON' AND not 'TVStandAlone' -> opennet_fiber
#     - if Netejer in ['AFAA','Hedens Net','GVD'] -> wholesale_coax
# -----------------------------
df_Loyalty = df_Loyalty.withColumn(
    "Produkttype_Technology",
    F.when(
        F.col("Produkttype_Internet").isNotNull() & F.col("Produkttype_Technology").isNull() &
        F.col("Netejer").isNotNull() &
        F.col("Netejer").contains("ON") &
        (~F.col("Netejer").contains("TVStandAlone")),
        F.lit("opennet_fiber")
    ).when(
        F.col("Produkttype_Internet").isNotNull() & F.col("Produkttype_Technology").isNull() &
        F.col("Netejer").isin("AFAA", "Hedens Net", "GVD"),
        F.lit("wholesale_coax")
    ).otherwise(F.col("Produkttype_Technology"))
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------
# 12) Anti-join: remove customers already in previous sample
# -----------------------------
df_Loyalty = df_Loyalty.join(
    Rapporteringer_LoyaltyGroup_Sample.select("KundeID").distinct(),
    on="KundeID",
    how="left_anti"
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------
# 13) Weighted sample of 6000 unique KundeID
#     Weight = 0.25 for customers with BOTH internet speed and TV, else 1.
#     Use exponential-race key to do weighted sampling without replacement.
# -----------------------------
df_Loyalty_unique = df_Loyalty.dropDuplicates(["KundeID"])

weight_col = F.when(
    F.col("Produkttype_Speed").isNotNull() & F.col("Produkttype_TV").isNotNull(),
    F.lit(0.25)
).otherwise(F.lit(1.0))

# Exponential race key: smaller key => more likely chosen proportional to weight
# key = -log(U) / weight  (U uniform in (0,1))
u = F.rand(seed=1)
key = (-F.log(u)) / weight_col

df_sample = (df_Loyalty_unique
    .withColumn("_wkey", key)
    .orderBy(F.col("_wkey").asc())
    .limit(6000)
    .drop("_wkey")
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------
# 14) Add todays date (dToday) into Udsendelsesdato
# -----------------------------
df_sample_table = df_sample.withColumn("Udsendelsesdato", F.lit(dToday))# -----------------------------
# 14) Add todays date (dToday) into Udsendelsesdato
# -----------------------------
df_sample_table = df_sample.withColumn("Udsendelsesdato", F.lit(dToday))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
