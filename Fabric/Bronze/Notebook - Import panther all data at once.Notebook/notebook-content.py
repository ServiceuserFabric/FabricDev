# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b1df83e8-4963-4129-b9a8-db8ced7800dd",
# META       "default_lakehouse_name": "Bronze_lakehouse",
# META       "default_lakehouse_workspace_id": "6b06974a-4346-4a38-bc5a-d42e564a6bec"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Import libaries

# CELL ********************

import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Function for flattern JSON files

# CELL ********************

def flatten_df(df: DataFrame) -> DataFrame:
    # Funktion til at flade skemaet ud ved at traversere alle strukturerede felter
    def flatten_schema(schema, prefix=''):
        fields = []
        for field in schema.fields:
            name = prefix + field.name
            dtype = field.dataType
            # Hvis feltet er en StructType, kaldes funktionen rekursivt for at flade den ud
            if isinstance(dtype, StructType):
                fields += flatten_schema(dtype, prefix=name + '.')
            else:
                fields.append(name)
        return fields

    # Funktion til at vælge og aliasere kolonnerne, så punktum erstattes med understregning
    def select_columns(df, columns):
        return df.select([col(column).alias(column.replace('.', '_')) for column in columns])

    # Funktion til at eksplodere alle ArrayType felter i DataFrame
    def explode_columns(df, schema):
        for field in schema.fields:
            if isinstance(field.dataType, ArrayType):
                df = df.withColumn(field.name, explode_outer(col(field.name)))
        return df

    while True:
        # Flad skemaet ud
        schema = df.schema
        columns = flatten_schema(schema)
        df = select_columns(df, columns)
        
        # Tjek det nye skema og eksploder eventuelle ArrayType felter
        new_schema = df.schema
        if all(not isinstance(field.dataType, ArrayType) for field in new_schema.fields):
            break
        df = explode_columns(df, new_schema)
    
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cust Prod import data to lakehouse

# CELL ********************

# Læs JSON-filerne ind i en DataFrame
# Få den aktuelle dato
current_date = datetime.datetime.now().strftime("%Y-%m-%d")

# Generer stien dynamisk
path = f"Files/API_Data_cust_prod_All/{current_date}/data_page_*.json"


# Læs JSON-filerne ind i en DataFrame
df = spark.read.json(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Brug funktionen til at udfolde DataFrame
df_expanded = flatten_df(df)

print(f"Rows before deduplication:{df_expanded.count()}")

# Fjern dubletter baseret på kolonnen "id"
df_expanded = df_expanded.dropDuplicates(['id'])

print(f"Rows after deduplication:{df_expanded.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Overskriv tabellen med den udfoldede DataFrame
df_expanded.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("all_data_cust_prod")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Customer table import data to lakehouse

# CELL ********************

# Læs JSON-filerne ind i en DataFrame
# Få den aktuelle dato
current_date = datetime.datetime.now().strftime("%Y-%m-%d")

# Generer stien dynamisk
path = f"Files/API_Data_customer_table_All/{current_date}/data_page_*.json"
# path = f"Files/API_Data_customer_table_All/2025-10-09/data_page_*.json"

# Læs JSON-filerne ind i en DataFrame
df = spark.read.json(path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Brug funktionen til at udfolde DataFrame
df_expanded = flatten_df(df)

print(f"Rows before deduplication:{df_expanded.count()}")

# Fjern dubletter baseret på kolonnen "id"
# df_expanded = df_expanded.dropDuplicates(['id'])
df_expanded = df_expanded.dropDuplicates(['id', 'accounts_id'])

print(f"Rows after deduplication:{df_expanded.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Overskriv tabellen med den udfoldede DataFrame
df_expanded.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("All_data_customer_table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Product config data import data to lakehouse

# CELL ********************

# Læs JSON-filerne ind i en DataFrame
# Få den aktuelle dato
current_date = datetime.datetime.now().strftime("%Y-%m-%d")

# Generer stien dynamisk
path = f"Files/API_Data_product_config_All/{current_date}/data_page_0.json"

# Læs JSON-filerne ind i en DataFrame
df = spark.read.json(path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Brug funktionen til at udfolde DataFrame
df_expanded = flatten_df(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Overskriv tabellen med den udfoldede DataFrame
df_expanded.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("All_data_product_config")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # IO data import data to lakehouse

# CELL ********************

# Læs JSON-filerne ind i en DataFrame
# Få den aktuelle dato
current_date = datetime.datetime.now().strftime("%Y-%m-%d")

# Generer stien dynamisk
path = f"Files/API_Data_IO_All/{current_date}/data_page_*.json"

# Læs JSON-filerne ind i en DataFrame
df = spark.read.json(path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Brug funktionen til at udfolde DataFrame
df_expanded = flatten_df(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Overskriv tabellen med den udfoldede DataFrame
df_expanded.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("All_data_IO")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Customer group import data to lakehouse

# CELL ********************

# Læs JSON-filerne ind i en DataFrame
# Få den aktuelle dato
current_date = datetime.datetime.now().strftime("%Y-%m-%d")

# Generer stien dynamisk
path = f"Files/API_Data_Customer_Groups_All/{current_date}/data_page_*.json"

# Læs JSON-filerne ind i en DataFrame
df = spark.read.json(path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Brug funktionen til at udfolde DataFrame
df_expanded = flatten_df(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Overskriv tabellen med den udfoldede DataFrame
df_expanded.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("All_data_Customer_Groups")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
