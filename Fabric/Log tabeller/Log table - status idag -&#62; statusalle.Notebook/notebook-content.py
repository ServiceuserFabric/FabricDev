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
df_target = spark.table("silver_lakehouse.statusalle")

# Hent kolonnerne dynamisk fra begge schemaer
source_columns = df_source.columns
target_columns = df_target.columns

# Find de kolonner, der kun findes i kilde-tabellen (source)
missing_columns = list(set(source_columns) - set(target_columns))

# Dynamisk tilføj manglende kolonner til mål-tabellen (target)
if missing_columns:
    print(f"Tilføjer følgende manglende kolonner til 'statusalle': {missing_columns}")
    for col in missing_columns:
        # Find kolonnens datatype fra source schema
        dtype = [f.dataType.simpleString() for f in df_source.schema.fields if f.name == col][0]
        
        # Opret kolonnen dynamisk i mål-tabellen
        add_column_query = f"ALTER TABLE silver_lakehouse.statusalle ADD COLUMN {col} {dtype}"
        print(f"Executing query: {add_column_query}")
        spark.sql(add_column_query)
else:
    print("Ingen manglende kolonner blev fundet.")

# Opdater target table schema efter eventuelle ændringer
df_target = spark.table("silver_lakehouse.statusalle")

# **Opdater target_columns efter schema ændringer**
target_columns = df_target.columns

# Find fælles kolonner i source og target
common_columns = list(set(source_columns).intersection(target_columns))

# Konverter kolonnelisten til SQL format
columns_str = ', '.join(common_columns)

# Få antallet af rækker, der skal indsættes
row_count = df_source.count()
print(f"Forbereder indsættelse af {row_count} rækker fra 'Status_I_dag_' til 'statusalle'...")


# Dynamisk INSERT INTO for at overføre data fra Status_I_dag_ til status
insert_query = f"""
INSERT INTO silver_lakehouse.statusalle({columns_str})
SELECT {columns_str}
FROM silver_lakehouse.Status_I_Dag_ AS D
WHERE NOT EXISTS (SELECT 1 FROM silver_lakehouse.statusalle AS A
                WHERE D.id = A.id AND D.Dato = A.Dato)
"""

# Udfør INSERT INTO query
spark.sql(insert_query)

# Udskriv feedback om indsættelsen er fuldført
print(f"{row_count} rækker indsat fra 'Status_I_dag_' til 'statusalle'.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
