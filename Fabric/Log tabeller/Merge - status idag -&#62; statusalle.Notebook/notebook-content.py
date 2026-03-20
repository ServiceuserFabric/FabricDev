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

# # Status log table data merge

# CELL ********************

# Indlæs source view og target table som DataFrames
df_source = spark.table("silver_lakehouse.Status_I_Dag_")
df_target = spark.table("silver_lakehouse.statusalle")

# Hent kolonnerne dynamisk fra begge schemaer
source_columns = df_source.columns
target_columns = df_target.columns

# Find fælles kolonner i source og target
common_columns = list(set(source_columns).intersection(target_columns))

# Definer update set expression
update_columns = [f"target.{col_name}=source.{col_name}" for col_name in common_columns if col_name != 'id' and col_name != 'Dato']
update_set_expr = ', '.join(update_columns)

# Definer insert columns og values
insert_columns = ', '.join(common_columns)
insert_values = ', '.join([f'source.{col_name}' for col_name in common_columns])

# MERGE SQL statement
merge_sql = f"""
MERGE INTO silver_lakehouse.statusalle AS target
USING silver_lakehouse.Status_I_Dag_ AS source
ON target.id = source.id AND target.Dato = source.Dato
WHEN MATCHED THEN 
    UPDATE SET {update_set_expr}
WHEN NOT MATCHED THEN 
    INSERT ({insert_columns})
    VALUES ({insert_values})
"""

# Udfør INSERT INTO query
spark.sql(merge_sql)

# Udskriv feedback om indsættelsen er fuldført
print(f"Rækker indsat fra 'Status_I_dag_' til 'statusalle'.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
