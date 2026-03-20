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
path = f"Files/API_Data_cust_prod_lastModified/{current_date}/data_page_*.json"

# Læs JSON-filerne ind i en DataFrame
df = spark.read.json(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not df.isEmpty():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col


    # Hent all_data_cust_prod fra databasen
    all_data_cust_prod = spark.sql("SELECT * FROM Bronze_lakehouse.all_data_cust_prod")

    # Flad ud DataFrame (antag, at du har en funktion flatten_df)
    df_expanded = flatten_df(df)
    noDub_data_df = df_expanded.dropDuplicates(['id'])

    # Alias DataFrames for at undgå tvetydighed
    noDub_data_df = noDub_data_df.alias("source")
    all_data_cust_prod = all_data_cust_prod.alias("target")

    # Find kolonner i df_expanded som matcher all_data_cust_prod
    common_columns = [col_name for col_name in noDub_data_df.columns if col_name in all_data_cust_prod.columns]

    # Definer update set expression
    update_columns = [f"target.{col_name}=source.{col_name}" for col_name in common_columns if col_name != 'id']
    update_set_expr = ', '.join(update_columns)

    # Definer insert columns og values
    insert_columns = ', '.join(common_columns)
    insert_values = ', '.join([f'source.{col_name}' for col_name in common_columns])

    noDub_data_df.createOrReplaceTempView("noDub_data_df")

    # Antal rækker før MERGE
    initial_all_data_count = all_data_cust_prod.count()
    initial_noDub_data_count = noDub_data_df.count()

    # MERGE SQL statement
    merge_sql = f"""
    MERGE INTO Bronze_lakehouse.all_data_cust_prod AS target
    USING (SELECT * FROM noDub_data_df) AS source
    ON target.id = source.id
    WHEN MATCHED THEN 
        UPDATE SET {update_set_expr}
    WHEN NOT MATCHED THEN 
        INSERT ({insert_columns})
        VALUES ({insert_values})
    """

    # Udfør MERGE operation
    spark.sql(merge_sql)

    # Antal rækker efter MERGE
    final_all_data_count = spark.sql("SELECT * FROM Bronze_lakehouse.all_data_cust_prod").count()
    print(final_all_data_count)
    print(initial_all_data_count)

    # Beregn antal indsatte rækker
    inserted_count = final_all_data_count - initial_all_data_count

    # Beregn antal opdaterede rækker
    # Her antager vi, at alle rækker i noDub_data_df, som matcher, bliver opdateret
    updated_count = initial_noDub_data_count - inserted_count

    # Kontrol udskrifter
    print("Upsert operation udført:")
    print(f"Antal indsatte rækker: {inserted_count}")
    print(f"Antal opdaterede rækker: {updated_count}")
else:
    print("Ingen data at loade ind")


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
path = f"Files/API_Data_customer_table_lastModified/{current_date}/data_page_*.json"

# Læs JSON-filerne ind i en DataFrame
df = spark.read.json(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not df.isEmpty():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col


    # Hent all_data_customer_table fra databasen
    all_data_customer_table = spark.sql("SELECT * FROM Bronze_lakehouse.all_data_customer_table")

    # Flad ud DataFrame (antag, at du har en funktion flatten_df)
    df_expanded = flatten_df(df)
    noDub_data_df = df_expanded.dropDuplicates(['id', 'accounts_id'])

    # Alias DataFrames for at undgå tvetydighed
    noDub_data_df = noDub_data_df.alias("source")
    all_data_customer_table = all_data_customer_table.alias("target")

    # Find kolonner i df_expanded som matcher all_data_customer_table
    common_columns = [col_name for col_name in noDub_data_df.columns if col_name in all_data_customer_table.columns]

    # Definer update set expression
    update_columns = [f"target.{col_name}=source.{col_name}" for col_name in common_columns if col_name != 'id']
    update_set_expr = ', '.join(update_columns)

    # Definer insert columns og values
    insert_columns = ', '.join(common_columns)
    insert_values = ', '.join([f'source.{col_name}' for col_name in common_columns])

    noDub_data_df.createOrReplaceTempView("noDub_data_df")

    # Antal rækker før MERGE
    initial_all_data_count = all_data_customer_table.count()
    initial_noDub_data_count = noDub_data_df.count()

    # MERGE SQL statement
    merge_sql = f"""
    MERGE INTO Bronze_lakehouse.all_data_customer_table AS target
    USING (SELECT * FROM noDub_data_df) AS source
    ON target.id = source.id
    AND target.accounts_id = source.accounts_id
    WHEN MATCHED THEN 
        UPDATE SET {update_set_expr}
    WHEN NOT MATCHED THEN 
        INSERT ({insert_columns})
        VALUES ({insert_values})
    """

    # Udfør MERGE operation
    spark.sql(merge_sql)

    # Antal rækker efter MERGE
    final_all_data_count = spark.sql("SELECT * FROM Bronze_lakehouse.all_data_customer_table").count()
    print(final_all_data_count)
    print(initial_all_data_count)

    # Beregn antal indsatte rækker
    inserted_count = final_all_data_count - initial_all_data_count

    # Beregn antal opdaterede rækker
    # Her antager vi, at alle rækker i noDub_data_df, som matcher, bliver opdateret
    updated_count = initial_noDub_data_count - inserted_count

    # Kontrol udskrifter
    print("Upsert operation udført:")
    print(f"Antal indsatte rækker: {inserted_count}")
    print(f"Antal opdaterede rækker: {updated_count}")
else:
    print("ingen data at loade")

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
path = f"Files/API_Data_product_config_lastModified/{current_date}/data_page_*.json"

# Læs JSON-filerne ind i en DataFrame
df = spark.read.json(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not df.isEmpty():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    # Hent all_data_product_config fra databasen
    all_data_product_config = spark.sql("SELECT * FROM Bronze_lakehouse.all_data_product_config")

    # Flad ud DataFrame (antag, at du har en funktion flatten_df)
    df_expanded = flatten_df(df)
    noDub_data_df = df_expanded.dropDuplicates(['id'])

    # Alias DataFrames for at undgå tvetydighed
    noDub_data_df = noDub_data_df.alias("source")
    all_data_product_config = all_data_product_config.alias("target")

    # Find kolonner i df_expanded som matcher all_data_product_config
    common_columns = [col_name for col_name in noDub_data_df.columns if col_name in all_data_product_config.columns]

    # Definer update set expression
    update_columns = [f"target.{col_name}=source.{col_name}" for col_name in common_columns if col_name != 'id']
    update_set_expr = ', '.join(update_columns)

    # Definer insert columns og values
    insert_columns = ', '.join(common_columns)
    insert_values = ', '.join([f'source.{col_name}' for col_name in common_columns])

    noDub_data_df.createOrReplaceTempView("noDub_data_df")

    # Antal rækker før MERGE
    initial_all_data_count = all_data_product_config.count()
    initial_noDub_data_count = noDub_data_df.count()

    # MERGE SQL statement
    merge_sql = f"""
    MERGE INTO Bronze_lakehouse.all_data_product_config AS target
    USING (SELECT * FROM noDub_data_df) AS source
    ON target.id = source.id
    WHEN MATCHED THEN 
        UPDATE SET {update_set_expr}
    WHEN NOT MATCHED THEN 
        INSERT ({insert_columns})
        VALUES ({insert_values})
    """

    # Udfør MERGE operation
    spark.sql(merge_sql)

    # Antal rækker efter MERGE
    final_all_data_count = spark.sql("SELECT * FROM Bronze_lakehouse.all_data_product_config").count()
    print(final_all_data_count)
    print(initial_all_data_count)

    # Beregn antal indsatte rækker
    inserted_count = final_all_data_count - initial_all_data_count

    # Beregn antal opdaterede rækker
    # Her antager vi, at alle rækker i noDub_data_df, som matcher, bliver opdateret
    updated_count = initial_noDub_data_count - inserted_count

    # Kontrol udskrifter
    print("Upsert operation udført:")
    print(f"Antal indsatte rækker: {inserted_count}")
    print(f"Antal opdaterede rækker: {updated_count}")
else:
    print("Ingen data at loade")

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
path = f"Files/API_Data_IO_lastModified/{current_date}/data_page_*.json"

# Læs JSON-filerne ind i en DataFrame
df = spark.read.json(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not df.isEmpty():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    # Hent all_data_io fra databasen
    all_data_io = spark.sql("SELECT * FROM Bronze_lakehouse.all_data_io")

    # Flad ud DataFrame (antag, at du har en funktion flatten_df)
    df_expanded = flatten_df(df)
    noDub_data_df = df_expanded.dropDuplicates(['id'])

    # Alias DataFrames for at undgå tvetydighed
    noDub_data_df = noDub_data_df.alias("source")
    all_data_io = all_data_io.alias("target")

    # Find kolonner i df_expanded som matcher all_data_io
    common_columns = [col_name for col_name in noDub_data_df.columns if col_name in all_data_io.columns]

    # Definer update set expression
    update_columns = [f"target.{col_name}=source.{col_name}" for col_name in common_columns if col_name != 'id']
    update_set_expr = ', '.join(update_columns)

    # Definer insert columns og values
    insert_columns = ', '.join(common_columns)
    insert_values = ', '.join([f'source.{col_name}' for col_name in common_columns])

    noDub_data_df.createOrReplaceTempView("noDub_data_df")

    # Antal rækker før MERGE
    initial_all_data_count = all_data_io.count()
    initial_noDub_data_count = noDub_data_df.count()

    # MERGE SQL statement
    merge_sql = f"""
    MERGE INTO Bronze_lakehouse.all_data_io AS target
    USING (SELECT * FROM noDub_data_df) AS source
    ON target.id = source.id
    WHEN MATCHED THEN 
        UPDATE SET {update_set_expr}
    WHEN NOT MATCHED THEN 
        INSERT ({insert_columns})
        VALUES ({insert_values})
    """

    # Udfør MERGE operation
    spark.sql(merge_sql)

    # Antal rækker efter MERGE
    final_all_data_count = spark.sql("SELECT * FROM Bronze_lakehouse.all_data_io").count()
    print(final_all_data_count)
    print(initial_all_data_count)

    # Beregn antal indsatte rækker
    inserted_count = final_all_data_count - initial_all_data_count

    # Beregn antal opdaterede rækker
    # Her antager vi, at alle rækker i noDub_data_df, som matcher, bliver opdateret
    updated_count = initial_noDub_data_count - inserted_count

    # Kontrol udskrifter
    print("Upsert operation udført:")
    print(f"Antal indsatte rækker: {inserted_count}")
    print(f"Antal opdaterede rækker: {updated_count}")
else:
    print("ingen data at loade")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Customer group data to lakehouse

# CELL ********************

# Læs JSON-filerne ind i en DataFrame
# Få den aktuelle dato
current_date = datetime.datetime.now().strftime("%Y-%m-%d")

# Generer stien dynamisk
path = f"Files/API_Data_Customer_Groups_lastModified/{current_date}/data_page_*.json"

# Læs JSON-filerne ind i en DataFrame
df = spark.read.json(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not df.isEmpty():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    # Hent all_data_customer_groups fra databasen
    all_data_customer_groups = spark.sql("SELECT * FROM Bronze_lakehouse.all_data_customer_groups")

    # Flad ud DataFrame (antag, at du har en funktion flatten_df)
    df_expanded = flatten_df(df)
    noDub_data_df = df_expanded.dropDuplicates(['id'])

    # Alias DataFrames for at undgå tvetydighed
    noDub_data_df = noDub_data_df.alias("source")
    all_data_customer_groups = all_data_customer_groups.alias("target")

    # Find kolonner i df_expanded som matcher all_data_customer_groups
    common_columns = [col_name for col_name in noDub_data_df.columns if col_name in all_data_customer_groups.columns]

    # Definer update set expression
    update_columns = [f"target.{col_name}=source.{col_name}" for col_name in common_columns if col_name != 'id']
    update_set_expr = ', '.join(update_columns)

    # Definer insert columns og values
    insert_columns = ', '.join(common_columns)
    insert_values = ', '.join([f'source.{col_name}' for col_name in common_columns])

    noDub_data_df.createOrReplaceTempView("noDub_data_df")

    # Antal rækker før MERGE
    initial_all_data_count = all_data_customer_groups.count()
    initial_noDub_data_count = noDub_data_df.count()

    # MERGE SQL statement
    merge_sql = f"""
    MERGE INTO Bronze_lakehouse.all_data_customer_groups AS target
    USING (SELECT * FROM noDub_data_df) AS source
    ON target.id = source.id
    WHEN MATCHED THEN 
        UPDATE SET {update_set_expr}
    WHEN NOT MATCHED THEN 
        INSERT ({insert_columns})
        VALUES ({insert_values})
    """

    # Udfør MERGE operation
    spark.sql(merge_sql)

    # Antal rækker efter MERGE
    final_all_data_count = spark.sql("SELECT * FROM Bronze_lakehouse.all_data_customer_groups").count()
    print(final_all_data_count)
    print(initial_all_data_count)

    # Beregn antal indsatte rækker
    inserted_count = final_all_data_count - initial_all_data_count

    # Beregn antal opdaterede rækker
    # Her antager vi, at alle rækker i noDub_data_df, som matcher, bliver opdateret
    updated_count = initial_noDub_data_count - inserted_count

    # Kontrol udskrifter
    print("Upsert operation udført:")
    print(f"Antal indsatte rækker: {inserted_count}")
    print(f"Antal opdaterede rækker: {updated_count}")
else:
    print("ingen data at loade")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Incremental load Status idag

# CELL ********************

# Læs JSON-filerne ind i en DataFrame
# Få den aktuelle dato
current_date = datetime.datetime.now().strftime("%Y-%m-%d")

# Generer stien dynamisk
path = f"Files/API_Data_cust_prod_lastModified/{current_date}/data_page_*.json"

# Læs JSON-filerne ind i en DataFrame
df = spark.read.json(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not df.isEmpty():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    spark.sql("DELETE FROM Bronze_lakehouse.All_data_Cust_prod_incremental")

    # Hent All_data_Cust_prod_incremental fra databasen
    All_data_Cust_prod_incremental = spark.sql("SELECT * FROM Bronze_lakehouse.All_data_Cust_prod_incremental")

    # Flad ud DataFrame (antag, at du har en funktion flatten_df)
    df_expanded = flatten_df(df)
    noDub_data_df = df_expanded.dropDuplicates(['id'])

    # Alias DataFrames for at undgå tvetydighed
    noDub_data_df = noDub_data_df.alias("source")
    All_data_Cust_prod_incremental = All_data_Cust_prod_incremental.alias("target")

    # Find kolonner i df_expanded som matcher All_data_Cust_prod_incremental
    common_columns = [col_name for col_name in noDub_data_df.columns if col_name in All_data_Cust_prod_incremental.columns]

    # Definer update set expression
    update_columns = [f"target.{col_name}=source.{col_name}" for col_name in common_columns if col_name != 'id']
    update_set_expr = ', '.join(update_columns)

    # Definer insert columns og values
    insert_columns = ', '.join(common_columns)
    insert_values = ', '.join([f'source.{col_name}' for col_name in common_columns])

    noDub_data_df.createOrReplaceTempView("noDub_data_df")

    # Antal rækker før MERGE
    initial_all_data_count = All_data_Cust_prod_incremental.count()
    initial_noDub_data_count = noDub_data_df.count()

    # MERGE SQL statement
    merge_sql = f"""
    MERGE INTO Bronze_lakehouse.All_data_Cust_prod_incremental AS target
    USING (SELECT * FROM noDub_data_df) AS source
    ON target.id = source.id
    WHEN MATCHED THEN 
        UPDATE SET {update_set_expr}
    WHEN NOT MATCHED THEN 
        INSERT ({insert_columns})
        VALUES ({insert_values})
    """

    # Udfør MERGE operation
    spark.sql(merge_sql)

    # Antal rækker efter MERGE
    final_all_data_count = spark.sql("SELECT * FROM Bronze_lakehouse.All_data_Cust_prod_incremental").count()
    print(final_all_data_count)
    print(initial_all_data_count)

    # Beregn antal indsatte rækker
    inserted_count = final_all_data_count - initial_all_data_count

    # Beregn antal opdaterede rækker
    # Her antager vi, at alle rækker i noDub_data_df, som matcher, bliver opdateret
    updated_count = initial_noDub_data_count - inserted_count

    # Kontrol udskrifter
    print("Upsert operation udført:")
    print(f"Antal indsatte rækker: {inserted_count}")
    print(f"Antal opdaterede rækker: {updated_count}")
else:
    print("ingen data at loade")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
