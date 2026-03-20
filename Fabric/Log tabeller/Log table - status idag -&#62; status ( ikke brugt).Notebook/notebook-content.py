# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ac6fbbda-655a-466b-abfa-ba456ca9605e",
# META       "default_lakehouse_name": "Silver_lakehouse",
# META       "default_lakehouse_workspace_id": "6b06974a-4346-4a38-bc5a-d42e564a6bec"
# META     },
# META     "warehouse": {
# META       "default_warehouse": "39d339dc-5635-453e-ba9e-8734cbf0953f",
# META       "known_warehouses": [
# META         {
# META           "id": "39d339dc-5635-453e-ba9e-8734cbf0953f",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Status log table data load

# CELL ********************

# Indlæs source view og target table som DataFrames
df_source = spark.table("silver_lakehouse.Status_I_Dag_")
df_target = spark.table("silver_lakehouse.status")

# Hent kolonnerne dynamisk fra begge schemaer
source_columns = df_source.columns
target_columns = df_target.columns

# Find de kolonner, der kun findes i kilde-tabellen (source)
missing_columns = list(set(source_columns) - set(target_columns))

# Dynamisk tilføj manglende kolonner til mål-tabellen (target)
if missing_columns:
    print(f"Tilføjer følgende manglende kolonner til 'Status': {missing_columns}")
    for col in missing_columns:
        # Find kolonnens datatype fra source schema
        dtype = [f.dataType.simpleString() for f in df_source.schema.fields if f.name == col][0]
        
        # Opret kolonnen dynamisk i mål-tabellen
        add_column_query = f"ALTER TABLE silver_lakehouse.Status ADD COLUMN {col} {dtype}"
        print(f"Executing query: {add_column_query}")
        spark.sql(add_column_query)
else:
    print("Ingen manglende kolonner blev fundet.")

# Opdater target table schema efter eventuelle ændringer
df_target = spark.table("silver_lakehouse.Status")

# **Opdater target_columns efter schema ændringer**
target_columns = df_target.columns

# Find fælles kolonner i source og target
common_columns = list(set(source_columns).intersection(target_columns))

# Konverter kolonnelisten til SQL format
columns_str = ', '.join(common_columns)

# Få antallet af rækker, der skal indsættes
row_count = df_source.count()
print(f"Forbereder indsættelse af {row_count} rækker fra 'Status_I_dag_' til 'status'...")

# Dynamisk INSERT INTO for at overføre data fra Status_I_dag_ til status
insert_query = f"""
INSERT INTO silver_lakehouse.Status({columns_str})
SELECT {columns_str}
FROM silver_lakehouse.Status_I_Dag_
"""

# Udfør INSERT INTO query
spark.sql(insert_query)

# Udskriv feedback om indsættelsen er fuldført
print(f"{row_count} rækker indsat fra 'Status_I_dag_' til 'status'.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Incremental_Status table data load

# CELL ********************

# Importer nødvendige funktioner
from pyspark.sql.functions import current_date

# Indlæs de nødvendige DataFrames
df_adi = spark.table("bronze_lakehouse.all_data_cust_prod_incremental")
df_sid = spark.table("silver_lakehouse.Status_I_dag_")

# Udfør join-operationen
df_joined = df_adi.alias("adi").join(
    df_sid.alias("sid"),
    on=df_adi["id"] == df_sid["id"],
    how="inner"
)

# Vælg de ønskede kolonner og tilføj 'log_dato' med dags dato
selected_columns = [
    "adi.id",
    "adi.accountKey",
    "adi.activeProvisioning",
    "adi.created",
    "adi.modified",
    "sid.KundeNO",
    "sid.ProduktID",
    "sid.hasBeenBilledOnce",
    "sid.Partner",
    "sid.Kunde_Status",
    "sid.Produkt_Status",
    "sid.startDate",
    "sid.endDate",
    "current_date() AS log_dato"  # Tilføj 'log_dato' kolonnen
]

df_result = df_joined.selectExpr(*selected_columns)

# Navn på mål-tabellen
target_table_name = "silver_lakehouse.Incremental_Status_Log"

# Tjek om mål-tabellen eksisterer
if spark.catalog.tableExists(target_table_name):
    print(f"Mål-tabellen '{target_table_name}' eksisterer allerede.")
    
    # Indlæs target table som DataFrame
    df_target = spark.table(target_table_name)
    
    # Hent kolonnerne dynamisk fra begge schemaer
    source_columns = df_result.columns
    target_columns = df_target.columns
    
    # Find de kolonner, der kun findes i kilde-dataene
    missing_columns = list(set(source_columns) - set(target_columns))
    
    # Dynamisk tilføj manglende kolonner til mål-tabellen
    if missing_columns:
        print(f"Tilføjer følgende manglende kolonner til '{target_table_name}': {missing_columns}")
        for col in missing_columns:
            # Find kolonnens datatype fra source schema
            dtype = [f.dataType.simpleString() for f in df_result.schema.fields if f.name == col][0]
            
            # Opret kolonnen dynamisk i mål-tabellen
            add_column_query = f"ALTER TABLE {target_table_name} ADD COLUMN ({col} {dtype})"
            print(f"Udfører query: {add_column_query}")
            spark.sql(add_column_query)
    else:
        print("Ingen manglende kolonner blev fundet.")
    
    # Opdater target table schema efter eventuelle ændringer
    df_target = spark.table(target_table_name)
    
    # Find fælles kolonner i source og target
    common_columns = list(set(source_columns).intersection(target_columns))
    
    # Konverter kolonnelisten til SQL format
    columns_str = ', '.join(common_columns)
    
    # Få antallet af rækker, der skal indsættes
    row_count = df_result.count()
    print(f"Forbereder indsættelse af {row_count} rækker til '{target_table_name}'...")
    
    # Opret en midlertidig view for df_result
    df_result.createOrReplaceTempView("temp_result")
    
    # Dynamisk INSERT INTO for at overføre data
    insert_query = f"""
    INSERT INTO {target_table_name} ({columns_str})
    SELECT {columns_str}
    FROM temp_result
    """
    
    # Udfør INSERT INTO query
    spark.sql(insert_query)
    
    # Udskriv feedback om indsættelsen er fuldført
    print(f"{row_count} rækker indsat i '{target_table_name}'.")
else:
    print(f"Mål-tabellen '{target_table_name}' eksisterer ikke. Opretter tabellen...")
    # Opret tabellen ved at skrive df_result til den
    df_result.write.mode("overwrite").saveAsTable(target_table_name)
    print(f"Tabellen '{target_table_name}' er oprettet.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
