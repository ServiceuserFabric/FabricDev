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
# META       "default_lakehouse_workspace_id": "6b06974a-4346-4a38-bc5a-d42e564a6bec",
# META       "known_lakehouses": [
# META         {
# META           "id": "ac6fbbda-655a-466b-abfa-ba456ca9605e"
# META         },
# META         {
# META           "id": "530285db-3a0b-4483-91a4-3489702ddc72"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import lit, to_date

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

# Angiv kolonnenavnet med datoen, der skal opdateres
date_column = "Dato"  # Erstat med det korrekte kolonnenavn for dato

# Opdater kolonnen med den nye dato
df_source = df_source.withColumn(date_column, to_date(lit("2024-11-18")))

# Få antallet af rækker, der skal indsættes
row_count = df_source.count()
print(f"Forbereder indsættelse af {row_count} rækker fra 'Status_I_dag_' til 'status'...")

display(df_source)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * from status WHERE Dato = '2024-12-20'")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
