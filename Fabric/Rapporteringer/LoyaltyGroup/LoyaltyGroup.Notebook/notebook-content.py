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



# BEMÆRK! AI-genereret kode kan indeholde fejl eller handlinger, du ikke havde til hensigt. Gennemse koden i denne celle omhyggeligt, før du kører den.

from datetime import datetime
try:
    from dateutil.relativedelta import relativedelta
except ImportError:
    raise ImportError("Please install python-dateutil for month arithmetic")

# Set today's date
dToday = datetime.now().date()
# Subtract 3 months, then set day to 1 (beginning of month)
three_months_ago = dToday - relativedelta(months=2)
dFromDate = three_months_ago.replace(day=1)

print("dFromDate:", dFromDate)
print("Todays date:", dToday)

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
C.City,
C.mobile                                         
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
C.City,
C.mobile                                        
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

# Load data ########################################################################
## Panther: Her henter vi oplysninger om nye kunder, som ikke skal kontaktes pga. KTU
# Connect to Fabrics sql endpoint 

query_Golden_Lakehouse_NEW = f"""
SELECT ProductHistory.accountKey, ProductHistory.KundeNO as KundeID,ProductHistory.orderedDate, PC.selfserviceNames_da
--SELECT *
FROM ProductHistory
    INNER JOIN Product_config AS PC
        ON PC.id = ProductHistory.ProduktID
    INNER JOIN Customer AS C 
        ON ProductHistory.accountKey = C.accounts_id
    INNER JOIN Cust_Group AS GC 
        ON GC.id = C.customerGroupKey
WHERE 
        ProductHistory.KundeNO IS NOT NULL
    AND ProductHistory.FromDate <= DATE '{dToday.isoformat()}' AND ProductHistory.ToDate > DATE '{dToday.isoformat()}'
    AND ProductHistory.Kunde_Status = 'Aktive'
    AND ProductHistory.NySalg = '1'
    AND ProductHistory.startDate between DATE '{dFromDate.isoformat()}' and DATE '{dToday.isoformat()}'
        AND (
            (PC.HovedProduktGruppe = 'Internet' AND PC.productGroup <> 'Migrering')
            OR
            (PC.HovedProduktGruppe = 'TV' AND PC.productGroup <> 'Dump')
        )
        AND (
            GC.fullPath LIKE '%Erhverv%'
            OR GC.fullPath LIKE '%Privat%'
            OR GC.fullPath LIKE '%Medarbejderbredbånd%'
        ) AND ca_created = ''
"""


Golden_Lakehouse_NEW = spark.sql(query_Golden_Lakehouse_NEW)
Golden_Lakehouse_NEW.show(10, truncate=False)
print(f"Number of rows: {Golden_Lakehouse_NEW.count()}")



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
C.email,
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
# Connecting to a warehouse requires another specific package 
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

AltiboxMobil_Nuuday = (
    spark.read
        .option(Constants.DatabaseName, "Golden_warehouse")
        .synapsesql(
            """
            SELECT
                [Mobil nummer],
                [Kunde Navn],
                [E-Mail],
                [Kunde Adresse],
                [Kunde Postnummer],
                [Kunde By]
            FROM nuu.AltiboxMobil_Nuuday
            WHERE LastVersion = 1
              AND Opsigelsesdato IS NOT NULL
            """
        )
)

display(AltiboxMobil_Nuuday)

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
# Coalesce mobile_Internet and mobile_TV
Golden_Lakehouse_Products = Golden_Lakehouse_Products.withColumn(
    "mobile",
    F.coalesce(F.col("mobile_Internet"), F.col("mobile_TV"))
)
# Order by accountKey_Internet
Golden_Lakehouse_Products.show(10, truncate=False)

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
    F.col("email").alias("email"),
    F.col("customerGroupName").alias("customerGroupName"),
    F.col("customerGroupKey").alias("customerGroupKey"),
    F.col("fullPath").alias("fullPath")
)

email_i = (email_sel
           .withColumnRenamed("accounts_id", "accounts_id_i")
           .withColumnRenamed("email", "email_i")
           .withColumnRenamed("customerGroupName", "customerGroupName_i")
           .withColumnRenamed("customerGroupKey", "customerGroupKey_i")
           .withColumnRenamed("fullPath", "fullPath_i"))

email_t = (email_sel
           .withColumnRenamed("accounts_id", "accounts_id_t")
           .withColumnRenamed("email", "email_t")
           .withColumnRenamed("customerGroupName", "customerGroupName_t")
           .withColumnRenamed("customerGroupKey", "customerGroupKey_t")
           .withColumnRenamed("fullPath", "fullPath_t"))

Merge_Golden_Bronze = (Golden_Lakehouse_Products
    .join(email_i, F.col("accountKey_Internet") == F.col("accounts_id_i"), "left")
    .join(email_t, F.col("accountKey_TV")       == F.col("accounts_id_t"), "left")
)

# Coalesce fields
Merge_Golden_Bronze = (Merge_Golden_Bronze
    .withColumn("Email", F.coalesce(F.col("email_i"), F.col("email_t")))
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

# # -----------------------------
# 2.1 Left join Golden_Lakehouse_Products with informations on ALtiboxMobil_Nuuday
# ----------------------------

# Check format from AltiboxMobil_Nuuday.view
AltiboxMobil_Nuuday.show(3, truncate=False)
Merge_Golden_Bronze.show(3, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # -----------------------------
# 2.1 Left join Golden_Lakehouse_Products with informations on ALtiboxMobil_Nuuday
# ----------------------------

# Check format from AltiboxMobil_Nuuday.view
AltiboxMobil_Nuuday.show(3, truncate=False)
Merge_Golden_Bronze.show(3, truncate=False)

# Add column in Golden_Lakehouse_Products. It is set to 0 or 1. 0 as default and 1 if there is a match 
# when joining Golden_Lakehouse_Products['mobile', Email] with AltiboxMobil_Nuuday['Mobil nummer', 'E-mail']
from pyspark.sql import functions as F

AltiboxMobil_Nuuday = AltiboxMobil_Nuuday.withColumnRenamed("Mobil nummer", "mobile_number") \
                                       .withColumnRenamed("E-Mail", "email_mobil")

Merge_Golden_Bronze = Merge_Golden_Bronze.withColumn("Produkttype_Mobil", F.lit(0))

Merge_Golden_Bronze = Merge_Golden_Bronze.join(
    AltiboxMobil_Nuuday,
    (F.col("mobile") == F.col("mobile_number")) | (F.col("Email") == F.col("email_mobil")),
    "left")
#Merge_Golden_Bronze = Merge_Golden_Bronze.join(
#    AltiboxMobil_Nuuday,
#    (F.col("mobile") == F.col("mobile_number")),
#    "left")
Merge_Golden_Bronze = Merge_Golden_Bronze.withColumn(
    "Produkttype_Mobil",
    F.when(F.col("mobile_number").isNotNull() | F.col("email_mobil").isNotNull(), 1).otherwise(0)
)

Merge_Golden_Bronze.show(10, truncate=False)
print(f"Number of rows: {Merge_Golden_Bronze.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter and show rows where is_mobile_customer == 1
filtered = Merge_Golden_Bronze.filter(F.col("Produkttype_Mobil") == 1)
filtered.show(10, truncate=False)  # Show the matching rows

# Print the number of rows in the filtered result
row_count = filtered.count()
print(f"Number of rows: {row_count}")

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
    "Produkttype_Mobil", 
    "Zip",
    "City",
    "customerGroupName",
    "fullPath"
)

df_Loyalty.show(10, truncate=False)

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
    df_rapporteringer.select("KundeID").distinct(),
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
# 13) Anti-join: remove customers who started with Altibox within last two months, as they received KTU
# -----------------------------

df_Loyalty = df_Loyalty.join(
    Golden_Lakehouse_NEW.select("KundeID").distinct(),
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
# 14) Weighted sample of 6000 unique KundeID
#     Weight = 0.25 for customers with BOTH internet speed and TV, else 1.
#     Use exponential-race key to do weighted sampling without replacement.
# -----------------------------
df_Loyalty_unique = df_Loyalty.dropDuplicates(["KundeID"])
df_Loyalty_unique = (
    df_Loyalty_unique
        .withColumn("Email_norm", F.lower(F.col("Email")))
        .dropDuplicates(["Email_norm"])
        .drop("Email_norm")
)

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
    .limit(12000)
    .drop("_wkey")
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------
# 15) Add todays date (dToday) into Udsendelsesdato
# -----------------------------
df_sample_table_date = df_sample.withColumn("Udsendelsesdato", F.lit(dToday))
df_sample_table_date.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save to table in SQL
rapporteringer_path = (
    "abfss://6b06974a-4346-4a38-bc5a-d42e564a6bec@onelake.dfs.fabric.microsoft.com/8c6daa7e-25e3-49f4-8f00-7ba8daf52bc3/Tables/dbo/LoyaltyGroup"
)
df_sample_table_date.write.mode("append").save(rapporteringer_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# Save to csv in files by naming it with variable dToday
#df_sample.write.csv(rapporteringer_path, header=True)

#df_sample = write_single_csv(df_sample, rapporteringer_path, "Emner.csv")
from pyspark.sql import functions as F
from notebookutils import mssparkutils
import re
# Save to csv in files by naming it with variable dToday
rapporteringer_path = (
    f"abfss://6b06974a-4346-4a38-bc5a-d42e564a6bec@onelake.dfs.fabric.microsoft.com/"
    f"8c6daa7e-25e3-49f4-8f00-7ba8daf52bc3/Files/LoyaltyGroup/{dToday}_LoyaltyGroup"
)

# Write a single consolidated CSV file (non-splittable)
def write_single_csv(df, output_path, filename):
    # Repartition to 1 partition to ensure a single output file
    temp_path = output_path + "_tmp"
    (
        df.repartition(1)
        .write
        .option("header", True)
        .mode("overwrite")
        .csv(temp_path)
    )
    import re
    from notebookutils import fs
    # Find part file (use attribute access .path)
    files = fs.ls(temp_path)
    part_file = next((f.path for f in files if re.match(r".*/part-.*\.csv$", f.path)), None)
    if part_file is None:
        raise FileNotFoundError("Could not find the single output part-*.csv file.")
    target = output_path + "/" + filename
    fs.cp(part_file, target)
    fs.rm(temp_path, recurse=True)
    return df

# Call function to write single CSV file
write_single_csv(df_sample, rapporteringer_path, "Emner.csv")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Used for one time case to add data from one table into another
from pyspark.sql import functions as F

# Read source table
df_src = spark.read.table("Rapporteringer.LoyaltyGroup_old")

# Change / override a column before append
df_transformed = df_src.withColumn(
    "source_system",
    F.lit("CRM")         
)

# Append into target table
df_transformed.write \
    .mode("append") \
    .saveAsTable("Rapporteringer.TargetTable")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
