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

# ## Libraries    

# CELL ********************

import pyspark
import pandas as pd
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (concat, col, lit, concat_ws, when,row_number, last,
                                    regexp_replace, lpad, expr,year,month,current_timestamp,to_date,first, 
                                    coalesce, min,max, sum, last_day, countDistinct, explode, 
                                    sequence,length ,udf,monotonically_increasing_id, 
                                    current_date, date_sub, weekofyear, date_format, date_add,dayofmonth, trim,
                                    from_utc_timestamp, months_between, round, trunc, add_months, create_map
                                    )
from collections import Counter
from datetime import date, datetime, timedelta
import re
from pyspark.sql.types import StringType, IntegerType, LongType, DateType, TimestampType
from pyspark.sql import functions, DataFrame
from pyspark.sql.window import Window
import sempy.fabric as fabric


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Settings and values that are used across the workspace

# CELL ********************

class GlobalParameters: 
    """
    Contains setup values. 
    use: print(GlobalParameters.bc_dimensions) 
    """

    #Default Company setup
    #companyName, companyCurrencyCode, companyKey, CompanyCountry
    company_data = [
        ('Altibox Danmark A/S','DKK','1','DK')
        ]

    #records (Technical name(col name) BC,Technical name NAV, wanted business name)
    dimension_mapping = [
        ("AFDELING","AFDELING","Afdeling"),
        ("CUSTOMERCONTRACT","CUSTOMERCONTRACT","CustomerContract"),
        ("NET PROVIDER","NET PROVIDER","NetProvider")
        ]
    
    def get_bc_dimension_keys():
        
        keys = [f"{key[1]}Key" for key in GlobalParameters.bc_dimensions]
        return keys

    #Calendar start and endpoint, and reporting year and month
    startdate = '2018-01-01'
    enddate = '2036-12-31'
    reportingYear = 2026
    reportingMonth = 9

    # ========================================
    # Finance Module Configuration
    # ========================================
    # NOTE: These parameters are ONLY used by Finance module notebooks (dp_budget, ENR_GLEntries).
    # If Finance module is not part of the deployed solution, these values are ignored.
    # IMPORTANT: Keep these parameters even if Finance is not initially deployed - this avoids
    # reconfiguration work if Finance module is added later.

    # Budget table name - leave as None to use default BC budget table, or specify custom table name
    budget_table_name = None

    # Equity account number for year-end reset posts (defined by business requirements)
    equity_account = 9098

    # Year-end range - accounts with number <= this value are P&L, above are Balance Sheet
    pl_range_end = 7999 

    # Source code pattern for year-end reset posts (usually "CLSINCOME" or "NULSTILRES")
    nulstilres_pattern = "NULSTILRES"

    # ========================================
    # End Finance Module Configuration
    # ========================================

    #Multi-environment flag: Set to True for clients with multiple BC environments (schemas in raw lakehouse)
    #Set to False for single-environment deployments (accelerator default)
    MULTI_ENVIRONMENT = False

    # ========================================
    # Currency Configuration
    # ========================================
    # Group reporting currency for GCY calculations. Consumed by FM_Utility.merge_currency_conversion
    # and FM_Utility.get_lcy_to_gcy_rate_df. Override per client deployment if the group consolidates
    # in a non-DKK currency (e.g. 'EUR', 'USD'). Must match a toCurrency value produced by
    # ENR_CurrencyExchangeRates after the Phase 0 inverse rows are appended.
    group_currency = 'DKK'

    # workspace_env = Lakehouse workspace name, Lakehouse workspace ID, {Reports workspace name: [semantic models to refresh]}
    workspace_env = [
        ('Prod - Dataplatform', '3a25df0d-986f-45a8-9140-8e9d88526e86', {'Prod - Reporting': ['PowerBiDataModel']})
    ]


    def get_workspace_enviroment():
        workspace = notebookutils.runtime.context.get('currentWorkspaceId')
        for ws in GlobalParameters.workspace_env:
            if workspace == ws[1]:
                return ws
        raise Exception('Cannot find workspace, check FM_Utility GlobalParameters.get_workspace_enviroment()')

    def get_semantic_models():
        """Returns the reports workspace and models paired to the current lakehouse workspace"""
        env_name, ws_id, models = GlobalParameters.get_workspace_enviroment()
        return models

# Global defaults for orchestrator parameters
# When the orchestrator runs a notebook, it injects datacheck=False via DAG args BEFORE cells execute.
# When running interactively, datacheck won't exist yet, so we default to True.
try:
    datacheck
except NameError:
    datacheck = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Transformer

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session

spark = SparkSession.builder.appName("CurrencyConversion").getOrCreate()


class FM_Utility:
    @staticmethod
    def get_currency_conversion_df():
        # Full path to the currency conversion Delta table
        SourceLakehouse = "Enr"
        SourceSchema = "enr_"
        SourceTableName = "currencyexchangerates"
        currency_df = spark.read.format("delta").table(f"{SourceLakehouse}.{SourceSchema}{SourceTableName}")
        
        # If needed, convert the columns to appropriate types
        currency_df = currency_df.withColumn("relationalExchRateAmount", currency_df["relationalExchRateAmount"].cast("float"))
        currency_df = currency_df.withColumnRenamed("date", "ConversionDate")
        currency_df = currency_df.withColumn("ConversionDate", currency_df["ConversionDate"].cast("date"))
        
        return currency_df

    @staticmethod
    def get_lcy_to_gcy_rate_df(date_col="date"):
        """
        Returns a DataFrame with (companyCurrency, <date_col>, gcy_rate) for direct LCY -> GCY conversion.

        Used by modules whose LCY columns already carry an LCY suffix (e.g. AR/AP: remainingAmtLCY,
        originalAmtLCY) and therefore cannot be passed through merge_currency_conversion without
        producing invalid *LCYGCY names. Callers join this df on (companyCurrency, date_col) and
        compute <col>GCY = <col>LCY / gcy_rate * 100 directly.

        Filters Enr.enr_currencyexchangerates to rows where toCurrency == GlobalParameters.group_currency.
        Requires the Phase 0 inverse rows fix in ENR_CurrencyExchangeRates so non-DKK companies
        find a matching LCY -> GCY row.
        """
        SourceLakehouse = "Enr"
        SourceSchema = "enr_"
        SourceTableName = "currencyexchangerates"
        currency_df = spark.read.format("delta").table(f"{SourceLakehouse}.{SourceSchema}{SourceTableName}")
        currency_df = currency_df.withColumn("relationalExchRateAmount", currency_df["relationalExchRateAmount"].cast("float"))
        currency_df = currency_df.withColumn("date", currency_df["date"].cast("date"))

        gcy_rates = currency_df.filter(col("toCurrency") == GlobalParameters.group_currency) \
            .select(
                col("fromCurrency").alias("companyCurrency"),
                col("date").alias(date_col),
                col("relationalExchRateAmount").alias("gcy_rate")
            )
        return gcy_rates

    @staticmethod
    def merge_currency_conversion(df, currency_df, transaction_currency_col, company_currency_col, date_col, tcy_columns_list, lcy_columns_list, group_currency=None):
        """
        Merges currency conversion rates into the given DataFrame and calculates local currency (LCY) and group currency (GCY) values.

        Args:
            df (spark.DataFrame): The input DataFrame containing transaction data.
            currency_df (spark.DataFrame): The DataFrame containing currency conversion rates.
            transaction_currency_col (str): The column name in `df` representing the transaction currency.
            company_currency_col (str): The column name in `df` representing the company currency.
            date_col (str): The column name in `df` representing the date of the transaction.
            tcy_columns_list (list of str): List of column names in `df` that are in transaction currency and need to be converted to LCY and GCY.
            lcy_columns_list (list of str): List of column names in `df` that are already in LCY and need to be converted to GCY.
            group_currency (str, optional): The group currency to convert LCY values to. If None, resolved from GlobalParameters.group_currency so client-level overrides are honoured.

        Returns:
            spark.DataFrame: The DataFrame with additional columns for LCY and GCY values, and renamed original columns.
        """
        # Resolve group_currency from GlobalParameters when caller didn't pass one explicitly.
        # Single source of truth — no literal default in the signature, no USD fallback in the body.
        if group_currency is None:
            group_currency = GlobalParameters.group_currency

        # Alias the datasets to avoid ambiguity
        df_alias = df.alias("df")
        currency_df_alias = currency_df.alias("currency_df")

        # First join: transaction currency to company currency for columns that are not already in LCY
        df_currency_lcy = df_alias.join(currency_df_alias,
                                        (col("df." + transaction_currency_col) == col("currency_df.toCurrency")) &
                                        (col("df." + company_currency_col) == col("currency_df.FromCurrency")) &
                                        (col("df." + date_col) == col("currency_df.ConversionDate")),
                                        how="left")
        
        # Handle exchange rate for identical currencies
        df_currency_lcy = df_currency_lcy.withColumn(
            "relationalExchRateAmount",
            when(col(f"df.{transaction_currency_col}") == col(f"df.{company_currency_col}"), lit(100))
            .otherwise(col("currency_df.relationalExchRateAmount"))
        )

        # Calculate LCY values for columns that are not already in LCY (TCY columns)
        for col_name in tcy_columns_list:
            df_currency_lcy = df_currency_lcy.withColumn(f"{col_name}LCY", col(f"df.{col_name}") / (100 / col("relationalExchRateAmount")))

        df_currency_lcy = df_currency_lcy.withColumnRenamed("relationalExchRateAmount", "relationalExchRateAmountTransToLCY") \
                                         .withColumnRenamed("ConversionDate", "ConversionDateLCY")

        # Second join: company currency to group currency. group_currency is guaranteed
        # non-None by the resolution at the top of the function.
        currency_df_filtered = currency_df.filter(col("toCurrency") == group_currency)

        currency_df_filtered_alias = currency_df_filtered.alias("currency_df_filtered")

        df_currency_gcy = df_currency_lcy.alias("df_currency_lcy").join(currency_df_filtered_alias,
                                                                        (col("df_currency_lcy." + company_currency_col) == col("currency_df_filtered.FromCurrency")) &
                                                                        (col("df_currency_lcy." + date_col) == col("currency_df_filtered.ConversionDate")),
                                                                        how="left")

        # Handle exchange rate for identical currencies
        df_currency_gcy = df_currency_gcy.withColumn(
            "relationalExchRateAmount",
            when(col(f"df_currency_lcy.{company_currency_col}") == lit(group_currency), lit(100))
            .otherwise(col("currency_df_filtered.relationalExchRateAmount"))
        )

        # Calculate GCY values for columns converted from TCY to LCY
        for col_name in tcy_columns_list:
            df_currency_gcy = df_currency_gcy.withColumn(
                f"{col_name}GCY",
                (col(f"{col_name}LCY") / col("relationalExchRateAmount"))*100
            )
        
        # Directly convert LCY columns to GCY without expecting a LCY intermediate step
        for col_name in lcy_columns_list:
            df_currency_gcy = df_currency_gcy.withColumn(
                f"{col_name}GCY",
                (col(f"{col_name}") / col("relationalExchRateAmount"))*100
            )

        # Rename original TCY columns to have a "TCY" suffix
        for col_name in tcy_columns_list:
            df_currency_gcy = df_currency_gcy.withColumnRenamed(col_name, f"{col_name}TCY")

        # Rename original LCY columns to have a "LCY" suffix
        for col_name in lcy_columns_list:
            df_currency_gcy = df_currency_gcy.withColumnRenamed(col_name, f"{col_name}LCY")

        # Always drop the following columns from the dataframe
        df_currency_gcy = df_currency_gcy.drop('fromCurrency')\
                            .drop('toCurrency')\
                            .drop('relationalExchRateAmountTransToLCY')\
                            .drop('relationalExchRateAmount')\
                            .drop('ConversionDate')\
                            .drop('ConversionDateLCY')
        return df_currency_gcy

    @staticmethod
    def list_methods():
        """Lists all available static methods in FM_Utility with descriptions."""
        methods = [func for func in dir(FM_Utility) if callable(getattr(FM_Utility, func)) and not func.startswith("__")]
        print("\n🔹 Available static methods in FM_Utility:\n")
        for method in methods:
            doc = getattr(FM_Utility, method).__doc__
            description = doc.strip() if doc else "No description available."
            print(f"  - ✅**{method}**: {description}\n")


    @staticmethod
    def list_lakehouse_tables(lakehouse_name):
        """returns a list of tables in a lakehouse, given the name of the lakehouse, auto resolves to current workspace."""
        workspace_id = fabric.get_workspace_id()
        lakehouse_id = mssparkutils.lakehouse.get(lakehouse_name)["id"]
        path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/"

        files = mssparkutils.fs.ls(path)
        names = []
        for file in files:
            names.append(file.name)
        return names

    @staticmethod
    def list_lakehouse_schemas(lakehouse_name):
        """Returns a list of schema names in a lakehouse."""
        workspace_id = fabric.get_workspace_id()
        lakehouse_id = mssparkutils.lakehouse.get(lakehouse_name)["id"]
        path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/"

        entries = mssparkutils.fs.ls(path)
        schemas = []
        for entry in entries:
            # Schemas are directories that contain subdirectories (tables), not Delta tables themselves
            # We check if the entry is a directory and contains sub-items (tables)
            try:
                sub_items = mssparkutils.fs.ls(entry.path)
                # If the entry has sub-items that look like Delta tables, it's a schema folder
                if any(not item.name.startswith("_") for item in sub_items):
                    schemas.append(entry.name)
            except:
                pass
        return schemas

    @staticmethod
    def remove_column_number(df, camelcase=None):
        """
        Removes column numbers from BC tables, for example Name-1 becomes Name
        Also transforms $Company to Company
        If camelcase is not empty, all column names are updated to start with a lowercase letter.
        Returns a PySpark dataframe
        """
        def _rename(column):
            name = "Company" if column == '$Company' else re.sub(r"-\d+$", "", column)
            if camelcase != '':
                name = name[0].lower() + name[1:] if name else name
            return name

        new_names = [_rename(c) for c in df.columns]
        return df.toDF(*new_names)

    @staticmethod
    def _load_multi_environment(lakehouse_name, table_name, camelcase="camel"):
        """
        Loads a table from all schemas in a lakehouse, adds an 'environment' column 
        derived from the schema name, and unions all DataFrames together.
        Used when GlobalParameters.MULTI_ENVIRONMENT = True.
        """
        schemas = FM_Utility.list_lakehouse_schemas(lakehouse_name)
        dfs = []

        for schema in schemas:
            # List tables in this schema
            try:
                workspace_id = fabric.get_workspace_id()
                lakehouse_id = mssparkutils.lakehouse.get(lakehouse_name)["id"]
                schema_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{schema}/"
                schema_tables = [item.name for item in mssparkutils.fs.ls(schema_path)]
            except:
                continue

            # Find matching table (with or without BC suffix)
            matched_table = None
            for t in schema_tables:
                if t == table_name or t.startswith(table_name + "-") or t.startswith(table_name):
                    matched_table = t
                    break

            if matched_table:
                df = spark.read.format("delta").load(f"{schema_path}{matched_table}")
                df = FM_Utility.remove_column_number(df, camelcase)
                df = df.withColumn("environment", lit(schema))
                dfs.append(df)
                print(f"  ✅ Loaded {matched_table} from schema: {schema}")

        if not dfs:
            raise Exception(f"Table '{table_name}' not found in any schema of lakehouse '{lakehouse_name}'")

        # Union all DataFrames using unionByName to handle schema differences
        result = dfs[0]
        for df in dfs[1:]:
            result = result.unionByName(df, allowMissingColumns=True)

        print(f"  📦 Combined {len(dfs)} environment(s)")
        return result

    @staticmethod
    def load_cleaned_dataframe(lakehouse_name, table_name, camelcase="camel"):
        """
        Loads a table into a dataframe, given lakehouse name and table name.
        The table name can be without the "-XXX" suffix found on BC tables.
        The columns of the returned dataframe is also cleaned of the same suffixes.
        
        If GlobalParameters.MULTI_ENVIRONMENT is True, discovers all schemas in the lakehouse
        containing the target table, reads from each, adds an 'environment' column, and unions them.
        """
        if GlobalParameters.MULTI_ENVIRONMENT:
            return FM_Utility._load_multi_environment(lakehouse_name, table_name, camelcase)

        # Original single-environment logic
        _table_name = table_name

        if not "-" in table_name:
            tables = FM_Utility.list_lakehouse_tables(lakehouse_name)
            for table in tables:
                if table.startswith(table_name):
                    _table_name = table
                    break

        df = spark.read.table(f"{lakehouse_name}.`{_table_name}`")
        df = FM_Utility.remove_column_number(df, camelcase)

        return df

    @staticmethod
    def add_nullhandling(df):
        """
        Add a new row with -1 in columns ending with 'Key' and matching data type values in other columns.

        Args:
            df (DataFrame): The input DataFrame.

        Returns:
            DataFrame: The DataFrame with a new row added.
        """
        # Get the schema of the DataFrame
        schema = df.schema

        # Create a new row with -1 in columns ending with 'Key' and matching data type values in others
        new_row_data = []
        for field in schema.fields:
            if field.name.endswith("Key") or field.name.endswith("Id"):
                new_row_data.append(-1)
            elif isinstance(field.dataType, StringType):
                new_row_data.append('unknown')
            elif isinstance(field.dataType, IntegerType):
                new_row_data.append(0)
            elif isinstance(field.dataType, LongType):
                new_row_data.append(0)  # Assuming 0 as a default value for LongType, adjust as needed
            elif isinstance(field.dataType, DateType):
                 new_row_data.append(date(2022, 1, 1))   # Default date value
            else:
                new_row_data.append(None)  # Handle other data types as per your requirement

        # Create a DataFrame for the new row
        new_row_df = df.sparkSession.createDataFrame([Row(*new_row_data)], schema)

        # Union the new row DataFrame with the original DataFrame
        df = df.union(new_row_df)

        return df

    @staticmethod
    def replace_blank_with_minus_one(df, columns):
        
        """ Replace blank or null values with -1 for specified columns"""

        for col_name in columns:
            df = df.withColumn(col_name, when(col(col_name).isNull() | (col(col_name) == ""), -1).otherwise(col(col_name)))
        return df


    @staticmethod
    def add_company_key(df, company_table="DP.dp_Company", source="bc"):
        """
        Add companyKey column to DataFrame by looking up the 'company' column
        against the DP.dp_Company lookup table.

        Args:
            df (DataFrame): Input DataFrame. Must contain a 'company' column.
            company_table (str): Lookup table containing companyKey and companyName.

        Returns:
            DataFrame: DataFrame with new 'companyKey' column.
        """
        # Load lookup table
        company_df = spark.read.table(company_table).select(
            col("companyName").alias("company"),
            "companyKey"
        )
        
        # Join on company name
        df_with_key = (df.join(company_df, on="company", how="left")
                        #.drop("companyCode")
                        .drop("company")
                      )
        
        return df_with_key



    @staticmethod
    def drop_columns_with_pattern(df, pattern, exceptions=None):
        
        """ Function to drop columns matching a pattern (i.e "df0" to "df9"), with exceptions """
        
        if exceptions is None:
            exceptions = []
        
        # Get a list of columns that match the pattern and are not in the exceptions list
        columns_to_drop = [
            col_name for col_name in df.columns 
            if re.match(pattern, col_name) and col_name not in exceptions
        ]
        
        # Drop the columns from the DataFrame
        df = df.drop(*columns_to_drop)
        
        return df, columns_to_drop
    
    @staticmethod
    def reorder_columns(df: DataFrame, first_columns: list) -> DataFrame:
        
        """ df = reorder_columns(df, first_columns=['companyKey', 'transactionCurrencyCode'])"""
        
        # Get the list of all columns in the DataFrame
        all_columns = df.columns

        # Define the categories based on the conditions you mentioned
        second_columns = [col for col in all_columns if 'Amount' in col]
        third_columns = [col for col in all_columns if 'Quantity' in col]
        fourth_columns = [col for col in all_columns if 'Price' in col]
        fifth_columns = [col for col in all_columns if 'Cost' in col]
        sixth_columns = [col for col in all_columns if 'Document' in col]  
        seventh_columns = [col for col in all_columns if 'Date' in col]
        eighth_columns = [col for col in all_columns if 'Key' in col]

        # Identify the remaining columns
        used_columns = set(first_columns + second_columns + third_columns + fourth_columns +
                        fifth_columns + sixth_columns + seventh_columns + eighth_columns)
        ninth_columns = [col for col in all_columns if col not in used_columns]

        # Combine all the lists to create the final order
        final_order = (first_columns + 
                    second_columns + 
                    third_columns + 
                    fourth_columns + 
                    fifth_columns + 
                    sixth_columns + 
                    seventh_columns + 
                    eighth_columns + 
                    ninth_columns)

        # Reorder the DataFrame columns
        df = df.select(final_order)
        
        return df

    @staticmethod
    def rename_column(col_name, table_name):
    
        """ Needs explination"""
        
        # Rule 1: Capitalize the first letter
        col_name = col_name[0].upper() + col_name[1:]

        # New Rule: Rename columns ending with 'No' to end with 'Number'
        if col_name.endswith("No"):
            col_name = col_name[:-2] + "Number"
        
        # Rule 2: Prefix with table name unless it ends with "Key" or already starts with the table name
        if not (col_name.endswith("Key") or col_name.startswith(table_name)):
            col_name = f"{table_name}_{col_name}"
        
        # Rule 3: Add underscores before each capital letter that is followed by a lowercase letter, unless if it's a key column
        if "Key" not in col_name:
            col_name = re.sub(r'([a-z])([A-Z])', r'\1_\2', col_name)
        
        return col_name

    @staticmethod

    def generate_rename_code(df_rename, table_name):

        """ What is this???"""
        
        # Generate the withColumnRenamed statements
        rename_statements = []
        for col_name in df_rename.columns:
            new_col_name = FM_Utility.rename_column(col_name, table_name)
            if new_col_name != col_name:
                rename_statements.append(f'.withColumnRenamed("{col_name}", "{new_col_name}")')

        # Sort the rename statements alphabetically by the original column name
        rename_statements.sort()

        # Generate the rename code
        rename_code = "df_rename = df_rename" + " \\\n       ".join(rename_statements)
        
        return rename_code

    @staticmethod
    def table_exists(spark, table_name):
        try:
            spark.sql(f"DESCRIBE TABLE {table_name}")
            return True
        except Exception:
            return False

    @staticmethod
    def create_select_statement(df, table_name,first_columns, drop_cols =[],categories = ['Key', 'Amount', 'Quantity', 'Price', 'Cost', 'Document', 'Date']): 
        """ Generates a SQL-like SELECT statement for renaming and reordering columns in a DataFrame.
        Parameters:
        df (DataFrame): The input DataFrame.
        table_name (str): The name of the table to prefix column names.
        first_columns (list): List of columns to appear first in the SELECT statement.
        categories (list, optional): List of categories to order the remaining columns. Default is ['Key', 'Amount', 'Quantity', 'Price', 'Cost', 'Document', 'Date'].
        drop_cols (list, optional): List of columns to drop from the DataFrame. Default is an empty list.
        Returns:
        str: A string representing the SELECT statement with renamed and reordered columns.
        Notes:
        - Columns specified in `drop_cols` are removed from the DataFrame.
        - Columns are renamed according to specific rules:
            - Capitalize the first letter.
            - Rename columns ending with 'No' to end with 'Number'.
            - Prefix with table name unless it ends with "Key" or already starts with the table name.
            - Add underscores before each capital letter that is followed by a lowercase letter, unless it's a key column.
        - The columns are ordered such that `first_columns` appear first, followed by columns matching the `categories`, and then any remaining columns.
        """
        #drops columns not used
        if len(drop_cols)>0:
            df = df.drop(*drop_cols)

        # Get the list of all columns in the DataFrame
        all_columns = df.columns
        # Find the length of the longest word
        max_length = max(len(col) for col in all_columns)

        def _rename_column(col_name, table_name):
        
            """Function to rename columns"""
            if col_name.startswith('is'):
                return col_name
            # Rule 1: Capitalize the first letter
            col_name = col_name[0].upper() + col_name[1:]

            # New Rule: Rename columns ending with 'No' to end with 'Number'
            if col_name.endswith("No"):
                col_name = col_name[:-2] + "Number"
            
            # Rule 2: Prefix with table name unless it ends with "Key" or already starts with the table name
            if not (col_name.endswith("Key") or col_name.startswith(table_name)):
                col_name = f"{table_name}_{col_name}"
            
            # Rule 3: Add underscores before each capital letter that is followed by a lowercase letter, unless if it's a key column
            if "Key" not in col_name:
                col_name = re.sub(r'([a-z])([A-Z])', r'\1_\2', col_name)
            
            return col_name
        ## Figure out the order of columns
        key_columns = []
        order = first_columns
        remaining_columns = [col for col in all_columns if col not in order]
        #special case for keys
        # Order the columns based on categories and add to order list
        for category in categories:
            print(category)
            #special case for keys
            if category == 'Key':
                print('here')
                key_columns += [col for col in remaining_columns if category in col]
            order += [col for col in remaining_columns if category.lower() in col.lower()]
            remaining_columns = [col for col in remaining_columns if col not in order]
            

        #add rest of cols
        order += remaining_columns

        # Move key columns to end
        order = [col for col in order if col not in key_columns]
        order += key_columns

        #print(key_columns)
        ##### column rename

        rename_statements = []
        for col_name in order:
            spacing = ' '*(max_length - len(col_name))
            new_col_name = _rename_column(col_name, table_name)
            rename_statements.append(f'col("{col_name}"){spacing}.alias("{new_col_name}"),')

        # Sort the rename statements alphabetically by the original column name
    

        # Generate the rename code
        rename_code = "df_rename = df.select(\n\t" + "\n\t".join(rename_statements)[:-1] + '\n)'
        
        return rename_code

    @staticmethod
    def add_row_number(df, partition_by_cols, order_by_col, new_col_name = 'row_num'):
        """
        Add a row number column to a PySpark DataFrame based on partition and ordering.

        Parameters:
        - df: PySpark DataFrame
        - partition_by_cols: List of columns to partition by
        - order_by_col: Column to order by
        - new_col_name: Name for the new row number column

        Returns:
        - PySpark DataFrame with the new row number column added
        """
        window_spec = Window.partitionBy(*partition_by_cols).orderBy(order_by_col)
        return df.withColumn(new_col_name, row_number().over(window_spec))
   
    
    @staticmethod
    def transform_dimension_set():
        """
        Transforms the 'Enr.enr_dimensionsetentry' table by applying NAV to BC mappings
        for varekatCode, afdelingCode, and butikKey. Returns a DataFrame with only the original columns.
        """
        # Step 1: Read the original dim_set table
        dim_set = spark.read.format("delta").table("Enr.enr_dimensionsetentry")

        # Step 2: Capture the original column structure
        original_columns = dim_set.columns

        # Step 3: Read mapping tables
        varekategori_map = spark.read.table("Raw.csv_mappingvarekategori") \
            .withColumn("VarekategoriKodeBC", lpad(col("VarekategoriKodeBC").cast("string"), 2, "0"))

        division_map = spark.read.table("Raw.csv_mappingvarekategoridivision")
        afdeling_map = spark.read.table("Raw.csv_mappingafdeling")

        # Step 4: Split NAV and non-NAV rows
        nav_dim_set = dim_set.filter(col("source_system") == "nav")
        non_nav_dim_set = dim_set.filter(col("source_system") != "nav")

        # Step 5: Join mappings onto NAV rows
        join = nav_dim_set.alias('a') \
            .join(varekategori_map.alias('b'), col("a.varekatCode") == col('b.VaregruppeKodeNAV'), "left") \
            .join(division_map.alias('c'), col('b.VarekategoriKodeBC') == col('c.VarekategoriKode'), "left") \
            .join(afdeling_map.alias('d'), col('a.afdelingCode') == col('d.navAfdeling'), "left")

        # Step 6: Apply transformations
        nav_dim_set = (
            join
            .withColumn(
                "varekatKey",
                concat_ws("_", lit("bc"), col("companyKey"), col("VarekategoriKodeBC"))
            )
            .withColumn(
                "afdelingKey",
                concat_ws(
                    "_",
                    lit("bc"),
                    col("companyKey"),
                    when(col("Division").isNotNull(), col("Division")).otherwise(col("bcAfdeling"))
                )
            )
            .withColumn(
                "varekatCode",
                when(col("VarekategoriKodeBC").isNotNull(), col("VarekategoriKodeBC")).otherwise(col("varekatCode"))
            )
            .withColumn(
                "afdelingCode",
                when(col("Division").isNotNull(), col("Division")).otherwise(col("bcAfdeling"))
            )
            .withColumn(
                "butikKey",
                regexp_replace(col("butikKey"), "nav", "bc")
            )
            .select(*original_columns)
        )

        # Step 7: Union NAV and non-NAV rows
        final_df = nav_dim_set.unionByName(non_nav_dim_set)

        return final_df

    # --- Function to compare DataFrame schemas ---
    @staticmethod
    def compare_dataframe_schemas(df1: DataFrame, df2: DataFrame, df1_name: str = "DataFrame 1", df2_name: str = "DataFrame 2"):
        """
        Compares the schemas of two PySpark DataFrames and prints the differences.

        Args:
            df1: The first PySpark DataFrame.
            df2: The second PySpark DataFrame.
            df1_name: A descriptive name for the first DataFrame (optional).
            df2_name: A descriptive name for the second DataFrame (optional).

        Returns:
            A dictionary containing the differences:
            {
                "only_in_df1": set of fields only in df1,
                "only_in_df2": set of fields only in df2,
                "common_fields": set of fields common to both,
                "type_mismatches": dict mapping field name to tuple (type_in_df1, type_in_df2)
                                for common fields with different types.
            }
        """
        if not isinstance(df1, DataFrame) or not isinstance(df2, DataFrame):
            raise TypeError("Inputs must be PySpark DataFrame objects.")

        schema1 = df1.schema
        schema2 = df2.schema

        fields1 = {field.name: field.dataType for field in schema1.fields}
        fields2 = {field.name: field.dataType for field in schema2.fields}

        field_names1 = set(fields1.keys())
        field_names2 = set(fields2.keys())

        only_in_1 = field_names1 - field_names2
        only_in_2 = field_names2 - field_names1
        common = field_names1.intersection(field_names2)

        type_mismatches = {}
        for field_name in common:
            # Compare string representations of types, as direct object comparison might fail
            # for complex types if their internal structure differs slightly but they are compatible.
            # For strict equality, use: fields1[field_name] != fields2[field_name]
            if str(fields1[field_name]) != str(fields2[field_name]):
                type_mismatches[field_name] = (fields1[field_name], fields2[field_name])

        print("-" * 30)
        print(f"Schema Comparison: '{df1_name}' vs '{df2_name}'")
        print("-" * 30)

        print(f"\nFields present only in '{df1_name}':")
        if only_in_1:
            # Sort for consistent output
            for field in sorted(list(only_in_1)):
                print(f"- {field} (Type: {fields1[field]})")
        else:
            print("- None")

        print(f"\nFields present only in '{df2_name}':")
        if only_in_2:
            # Sort for consistent output
            for field in sorted(list(only_in_2)):
                print(f"- {field} (Type: {fields2[field]})")
        else:
            print("- None")

        print(f"\nCommon fields ({len(common)}):")
        # You can uncomment the next line to list all common fields if desired
        # if common: print(f"- {sorted(list(common))}")
        # else: print("- None")

        common_fields_with_types = {
            field: {
                df1_name: fields1[field],
                df2_name: fields2[field]
            }
            for field in common
        }
        print(f"\nCommon fields ({len(common_fields_with_types)}):")

        print(f"\nCommon fields with type mismatches:")
        if type_mismatches:
            # Sort for consistent output
            for field in sorted(type_mismatches.keys()):
                types = type_mismatches[field]
                print(f"- {field}: '{df1_name}' type = {types[0]}, '{df2_name}' type = {types[1]}")
        else:
            print("- None")

        print("-" * 30)

        return {
            "only_in_df1": only_in_1,
            "only_in_df2": only_in_2,
            "common_fields": common,
            "type_mismatches": type_mismatches,
            "common_fields_with_types": common_fields_with_types
        }

    @staticmethod
    def select_dim_set_columns(dim_set, excluded_columns=None, include_all_except=None):
        """
        Build a list of columns from dim_set based on exclusion and filtering rules.
        Standard only returns Key columns. Can be set to return more. 
        
        Args:
            dim_set (DataFrame or list): The source DataFrame (only if it's still a DataFrame).
            excluded_columns (list, optional): List of columns to exclude (default is ['dimensionSetEntryKey', 'companyKey']).
            include_all_except (list, optional): If provided, include all columns except these, ignoring 'Key' filtering.
            
        Returns:
            List of column expressions.
        """
        if not isinstance(dim_set, DataFrame):
            return dim_set  # already processed
        
        if excluded_columns is None:
            excluded_columns = ['dimensionSetEntryKey', 'companyKey']

        if include_all_except:
            # Show everything except the columns in include_all_except
            valid_columns = [c for c in dim_set.columns if c not in include_all_except]
        else:
            # Default: show columns ending with "Key" and not excluded
            valid_columns = [c for c in dim_set.columns if c not in excluded_columns and c.endswith("Key")]
        
        return [col(f'dim_set.{c}') for c in valid_columns]

    @staticmethod
    def account_schedule_schema(k=10):
        """Canonical, empty-safe schema for the account-schedule / GL-hierarchy tables.

        A FIXED set of level columns (level1..levelK, Key+Name) plus the base columns,
        all StringType. Used so ENR_AccountSchedule and ENR_GLAccountHierarchy ALWAYS
        write a stable, correctly-typed table — even with no source data — rather than
        exiting early and leaving a curated view's FROM clause pointing at a missing
        object. K must match the curated view's level1..levelK and the Power BI model.
        """
        from pyspark.sql.types import StructType, StructField, StringType
        fields = [StructField('accountScheduleName', StringType())]
        for i in range(1, k + 1):
            fields.append(StructField(f'level{i}_Key', StringType()))
            fields.append(StructField(f'level{i}_Name', StringType()))
        fields += [
            StructField('leaf_account_key', StringType()),
            StructField('incomeBalance', StringType()),
            StructField('fullAccountName', StringType()),
        ]
        return StructType(fields)

    @staticmethod
    def pad_account_schedule_levels(df, k=10):
        """Pad a built account-schedule DataFrame up to the fixed level1..levelK schema.

        Adds any missing level{i}_Key / level{i}_Name as NULL, casts every level and
        base column to string, and returns columns in the canonical order produced by
        account_schedule_schema(k). Raises if the DataFrame already has a level deeper
        than K — a loud failure beats silently dropping a hierarchy level.
        """
        from pyspark.sql.functions import col, lit
        from pyspark.sql.types import StringType
        import re
        import builtins

        deepest = 0
        for c in df.columns:
            m = re.match(r'level(\d+)_(Key|Name)$', c)
            if m:
                # builtins.max: the module-level `from pyspark.sql.functions import *`
                # shadows the builtin max with the Spark column function (one arg only).
                deepest = builtins.max(deepest, int(m.group(1)))
        if deepest > k:
            raise ValueError(
                f"DataFrame has level{deepest} which exceeds the fixed K={k}. Increase K in "
                f"account_schedule_schema and the curated view / Power BI model before proceeding."
            )

        out = df
        for i in range(1, k + 1):
            for suffix in ('Key', 'Name'):
                name = f'level{i}_{suffix}'
                out = (out.withColumn(name, lit(None).cast(StringType()))
                       if name not in out.columns
                       else out.withColumn(name, col(name).cast(StringType())))

        for base in ('accountScheduleName', 'leaf_account_key', 'incomeBalance', 'fullAccountName'):
            out = (out.withColumn(base, lit(None).cast(StringType()))
                   if base not in out.columns
                   else out.withColumn(base, col(base).cast(StringType())))

        ordered = ['accountScheduleName']
        for i in range(1, k + 1):
            ordered += [f'level{i}_Key', f'level{i}_Name']
        ordered += ['leaf_account_key', 'incomeBalance', 'fullAccountName']
        return out.select(*ordered)

    # --- Change-detection: source fingerprint + watermark control table ---
    # Lets slow-changing dimension notebooks self-skip when their raw source is unchanged.
    # Fingerprint = count(*) + max(rowversion) per raw input — deliberately NOT Delta
    # DESCRIBE HISTORY, because ingestion may rewrite the raw file each export with no BC
    # change, which would produce false "changed" results.

    @staticmethod
    def _ensure_watermark_table():
        """Create the ETL watermark control table if it does not exist. Idempotent and
        race-safe (CREATE TABLE IF NOT EXISTS) so parallel runMultiple notebooks are fine."""
        tbl = "Enr.enr_etl_watermark"
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {tbl} (
                    sourceKey STRING,
                    fingerprint STRING,
                    updatedAt TIMESTAMP
                ) USING delta
            """)
        except Exception as e:
            print(f"watermark table ensure skipped/raced: {e}")
        return tbl

    @staticmethod
    def source_fingerprint(inputs, rowversion_candidates=('timestamp', 'systemModifiedAt')):
        """Compute a change-detection fingerprint over one or more source DataFrames.

        For each input: count(*) plus max() of the first available BC rowversion-style
        column (default 'timestamp', then 'systemModifiedAt'). If no such column is present,
        falls back to count-only with a warning (catches row-count changes, misses in-place
        edits). Returns a single comparable string. Accepts a single DataFrame or a list."""
        from pyspark.sql.functions import max as _max, col as _col
        if not isinstance(inputs, (list, tuple)):
            inputs = [inputs]
        parts = []
        for i, df in enumerate(inputs):
            cnt = df.count()
            rv_col = next((c for c in rowversion_candidates if c in df.columns), None)
            if rv_col is None:
                print(f"⚠️ source_fingerprint: input {i} has no rowversion column "
                      f"{rowversion_candidates}; using count only.")
                parts.append(f"cnt={cnt};max=NA")
            else:
                mx = df.agg(_max(_col(rv_col)).cast('string')).collect()[0][0]
                parts.append(f"cnt={cnt};{rv_col}max={mx}")
        return " | ".join(parts)

    @staticmethod
    def has_changed(notebook_key, fingerprint):
        """True if the stored watermark for notebook_key differs from fingerprint (or none
        exists yet -> first run -> treat as changed so the table is always built once)."""
        tbl = FM_Utility._ensure_watermark_table()
        row = spark.sql(
            f"SELECT fingerprint FROM {tbl} WHERE sourceKey = '{notebook_key}'"
        ).collect()
        if not row:
            return True
        return row[0]['fingerprint'] != fingerprint

    @staticmethod
    def commit_watermark(notebook_key, fingerprint):
        """Upsert the watermark row for notebook_key with the current fingerprint + timestamp.
        Call only after a successful write so a failed build never records a clean state."""
        tbl = FM_Utility._ensure_watermark_table()
        safe_fp = fingerprint.replace("'", "''")
        spark.sql(f"DELETE FROM {tbl} WHERE sourceKey = '{notebook_key}'")
        spark.sql(
            f"INSERT INTO {tbl} (sourceKey, fingerprint, updatedAt) "
            f"VALUES ('{notebook_key}', '{safe_fp}', current_timestamp())"
        )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## DataValidation

# CELL ********************

from pyspark.testing import assertDataFrameEqual
from pyspark.testing import assertSchemaEqual
from pyspark.errors import PySparkAssertionError
class FMtest:
    @staticmethod
    def testing_dataframe_similarity(df1,df2): 
        c1 = df1.count()
        c2 = df2.count()

        if c1 != c2:
            print(f"Rows counts don't match df1:{c1} df2: {c2}")
        #please have df ordered by something!
        try: 
            assertSchemaEqual(df1.schema,df2.schema)
        except PySparkAssertionError as e:
            #errors = spark.createDataFrame(e.data, schema=["Actual", "Expected"])
            print(e)
            return 0
        #Full comparison
        try:
            assertDataFrameEqual(df1, df2)
        except PySparkAssertionError as e:
            if len(e.message)>1000000:
                error = e.message.partition('\n')
                print(error[0])
                print('Data frame too large to print')
                #print(e.message[0:100000],'... Output truncated difference table too  big')
            else:print(e)
            return 0
        print("Df's are the same")
        return 1
        
        
    @staticmethod
    def create_or_alter_table(spark, qualified_table_name, df):
        if FM_Utility.table_exists(spark, qualified_table_name):
            # Alter table properties if the table exists
            try:
                spark.sql(f"""
                    ALTER TABLE {qualified_table_name}
                    SET TBLPROPERTIES (
                        'delta.columnMapping.mode' = 'name',
                        'delta.minReaderVersion' = '2',
                        'delta.minWriterVersion' = '5'
                    )
                """)
                print(f"Table properties for {qualified_table_name} were successfully updated.")
            except Exception as e:
                print(f"Failed to alter table properties for {qualified_table_name}: {e}")
        else:
            print(f"Table {qualified_table_name} does not exist. Creating table.")
            # Create the table if it does not exist
            try:
                # Define your table schema here
                schema = ", ".join([f"{field.name} {field.dataType.simpleString()}" for field in df.schema])
                
                # Create the table
                spark.sql(f"""
                    CREATE TABLE {qualified_table_name} ({schema})
                    USING delta
                """)
                print(f"Table {qualified_table_name} was successfully created.")
                
                # Alter table properties for the newly created table
                spark.sql(f"""
                    ALTER TABLE {qualified_table_name}
                    SET TBLPROPERTIES (
                        'delta.columnMapping.mode' = 'name',
                        'delta.minReaderVersion' = '2',
                        'delta.minWriterVersion' = '5'
                    )
                """)
                print(f"Table properties for {qualified_table_name} were successfully updated.")
            except Exception as e:
                print(f"Failed to create or alter table {qualified_table_name}: {e}")
                

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class DataCheck:

    import inspect

    @staticmethod
    def list_methods():
        """Lists all available static methods in DataCheck with descriptions."""
        methods = [
            (name, func) for name, func in inspect.getmembers(DataCheck, predicate=inspect.isfunction)
            if not name.startswith("__")
        ]
        
        print("\n🔹 Available static methods in DataCheck:\n")
        for name, func in methods:
            doc = func.__doc__
            description = doc.strip() if doc else "No description available."
            print(f"  - ✅**{name}**: {description}\n")


    @staticmethod
    def check_duplicates(df, *group_cols):
        """
        Check for duplicates in the group_cols columns. Example call:

            check_duplicates(df, "GeneralLedgerID", "CompanyName")
        
        Checks for duplicates by columns "GeneralLedgerID", "CompanyName".

        Args: 
            df (spark.DataFrame): Input dataframe to check for duplicates
            *group_cols (str): Column names to check
        
        Return:
            spark.DataFrame: Duplicates found.
        """
        dfDuplicates = df.groupBy(*group_cols).count().filter(col("count") > 1)

        # Show the duplicates if any, and raise an error
        columns = " and ".join((f"'{gc}'" for gc in group_cols))
        if dfDuplicates.count() > 0:
            print(f"Duplicates found in the {columns} column:")
            dfDuplicates.show()
            raise ValueError(f"Duplicate values found in the {columns} column. Stopping further execution.")
        else:
            print(f"No duplicates found in the {columns} column.")

        # Check the row count in the DataFrame NOT this enviroment has huge tables and we don't do this just for fun. 
        #print(f"Number of rows in the DataFrame: {df.count()}")
        return dfDuplicates

    @staticmethod
    def compare_dataframe_to_table(new_df, existing_table):
        """
        Compare the schema and data of a DataFrame (new_df) against an existing Delta table.

        Parameters:
        - new_df: DataFrame to compare (new data)
        - existing_table: String, name of the existing Delta table

        Returns:
        - A dictionary with schema and data comparison results
        """

        # Step 1: Read the schema of the existing table (if it exists)
        try:
            existing_df = spark.read.table(existing_table)
            existing_schema = set((field.name, field.dataType.simpleString()) for field in existing_df.schema.fields)
        except AnalysisException:
            existing_df = None
            existing_schema = None  # Table does not exist

        # Step 2: Extract schema from the new DataFrame (new_df)
        new_schema = set((field.name, field.dataType.simpleString()) for field in new_df.schema.fields)

        # Step 3: Compare schemas
        schema_mismatch = False
        schema_diff = None

        if existing_schema is None:
            print(f"⚠️ Table `{existing_table}` does not exist. Proceeding with write.")
        elif existing_schema != new_schema:
            schema_mismatch = True
            schema_diff = new_schema.symmetric_difference(existing_schema)
            print(f"⚠️ Schema Mismatch Detected in `{existing_table}`:\n {schema_diff}")
        else:
            print(f"✅ Schema Matches for `{existing_table}`.")

        # Step 4: Compare data differences (if table exists)
        data_changes_detected = False
        if existing_df is not None:
            data_diff = new_df.exceptAll(existing_df)

            if data_diff.count() > 0:
                data_changes_detected = True
                print(f"⚠️ Data Changes Detected in `{existing_table}`!")
            else:
                print(f"✅ No Data Changes Detected in `{existing_table}`.")

        return {
            "schema_mismatch": schema_mismatch,
            "schema_diff": schema_diff,
            "data_changes_detected": data_changes_detected
        }



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## EnrichTransformations

# CELL ********************

# enrich_transformer

class EnrichTransformations:


    @staticmethod
    def _handle_transaction_currency(df, lineCurrency, companyCurrency):
        if lineCurrency in df.columns and companyCurrency in df.columns:
            df = df.withColumn(
                "transactionCurrencyCode",
                when(
                    col(lineCurrency).isNull() | (col(lineCurrency) == ""),
                    col(companyCurrency)
                ).otherwise(col(lineCurrency))
            )
        return df


    @staticmethod
    #Generate Key columns from the input. 
    def _generate_keys(df, keyColumns, companyKey="companyKey"):
        if all(colName in df.columns for colName in keyColumns):
            for colName in keyColumns:
                newColName = colName + "Key"
                df = df.withColumn(newColName, concat_ws('_', col(companyKey), col(colName)))
        return df

    @staticmethod
    def display_joined_table(result_df, original_df):
        """
        Compares result and original DataFrames:
        - Prints row counts
        - Warns if extra rows are created
        - Shows duplicate columns by name
        """

        print(f"Initial original_df count: {original_df.count()}")
        final_count = result_df.count()
        print(f"Final result_df count: {final_count}")

        if final_count > original_df.count():
            print(f"⚠️ Join created {final_count - original_df.count()} extra rows. This may indicate a one-to-many relationship or duplicates.")

        # Find and show duplicate columns
        duplicates = EnrichTransformations._find_duplicate_column_names(result_df)

        if duplicates:
            print("\n⚡ Duplicate field names detected:")
            for field, count in duplicates.items():
                print(f"   - Field '{field}' appears {count} times")
        else:
            print("\n✅ No duplicate field names detected.")

    @staticmethod
    def _find_duplicate_column_names(df):
        """
        Counts field names ignoring struct prefixes.
        """
        field_names = [col.split(".")[-1] for col in df.columns]
        counts = Counter(field_names)
        duplicates = {field: count for field, count in counts.items() if count > 1}
        return duplicates


    @staticmethod
    def _handle_transaction_currency(df,lineCurrency,companyCurrency):
        lineCurrency = col(lineCurrency)
        companyCurrency = col(companyCurrency)
        df = df.withColumn(
            "transactionCurrencyCode",
            when(
                lineCurrency.isNull() | (lineCurrency == ""),
                companyCurrency
            ).otherwise(lineCurrency)
            
        )
        print("transactionCurrencyCode created")
        return df

    @staticmethod
    def _generate_keys(df, keyColumns, companyKey="companyKey", sourceSystem=None):
        for colName in keyColumns:
            newColName = colName + "Key"
            key_parts = []

            if sourceSystem:  # only add if it's provided
                key_parts.append(lit(sourceSystem))

            key_parts += [col(companyKey), col(colName)]

            df = df.withColumn(newColName, concat_ws('_', *key_parts))
            df = df.drop(colName)
            print(f"Key Created: {newColName}, column {colName} dropped")

        return df



    @staticmethod
    def _replace_blank_with_minus_one(df):
        """ Replace blank or null values with appropriate defaults based on data type"""
        
        
        for field in df.schema.fields:
            col_name = field.name
            if isinstance(field.dataType, (DateType, TimestampType)):
                # Replace null dates with 1900-01-01
                df = df.withColumn(col_name, when(col(col_name).isNull(), to_date(lit('1900-01-01'))).otherwise(col(col_name)))
            elif col_name.endswith('Key'):
                # Replace null or blank strings with -1 for Key columns
                df = df.withColumn(col_name, when(col(col_name).isNull() | (col(col_name) == ""), -1).otherwise(col(col_name)))
        return df

    @classmethod
    def frameTransform(cls, df, keyColumns=None):
        """Apply enriched processing transformations."""
        df = df.transform(cls._handle_transaction_currency)


        
        if keyColumns:
            df = df \
                .transform(lambda d: cls._generate_keys(d, keyColumns)) \
                .transform(cls._replace_blank_with_minus_one)

        return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Workspace Functions

# CELL ********************

class workspace:
    
    @staticmethod
    def refresh_lakehouse_endpoint(lakehouse_name):
        """
        Refreshes the SQL endpoint for the specified lakehouse to reflect schema changes.
        
        Args:
            lakehouse_name (str): The name of the lakehouse to refresh.
        
        Returns:
            None
        """
        try:
            fabric.refresh_sql_endpoint(lakehouse=lakehouse_name)
            print(f"Successfully refreshed SQL endpoint for lakehouse: {lakehouse_name}")
        except Exception as e:
            print(f"Failed to refresh SQL endpoint for lakehouse: {lakehouse_name}. Error: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Orchestration

# CELL ********************

class Orchestration:
    """Helpers for the Master orchestrator: an interactive notebook picker and a
    run dispatcher. Keeps the orchestrator notebook itself down to two calls —
    build_run_picker(DAG) to render the picker, then run(DAG, picker) to execute."""

    @staticmethod
    def build_run_picker(dag):
        """Render an accordion of checkboxes grouped by each activity's 'module'.
        Returns {notebook_name: Checkbox}. Leave all unchecked for a full run."""
        import ipywidgets as widgets
        from IPython.display import display
        from collections import OrderedDict

        modules = OrderedDict()
        for a in dag["activities"]:
            modules.setdefault(a.get("module", "Other"), []).append(a)

        checkboxes = {}
        sections, titles = [], []
        for mod, acts in modules.items():
            boxes = []
            for a in acts:
                cb = widgets.Checkbox(value=False, description=a["name"], indent=False)
                checkboxes[a["name"]] = cb
                boxes.append(cb)
            sections.append(widgets.VBox(boxes))
            titles.append(f"{mod}  ({len(acts)})")

        picker = widgets.Accordion(children=sections, selected_index=None)
        for i, t in enumerate(titles):
            picker.set_title(i, t)
        display(picker)
        print("Check notebooks to run ONLY those. Leave all unchecked for a full run.")
        return checkboxes

    @staticmethod
    def run(dag, checkboxes=None, run_multiple_args=None):
        """If any checkbox is ticked, run only those notebooks via runMultiple,
        with each selected activity's dependencies pruned to the selected set.
        Otherwise run the full DAG. Pass checkboxes=None (e.g. when the picker
        cell is frozen/skipped) to force a full run. Always goes through
        runMultiple so the orchestrator's root lakehouse context
        (useRootDefaultLakehouse on each activity) is propagated to every child
        notebook."""
        if run_multiple_args is None:
            run_multiple_args = {"displayDAGViaGraphviz": True, "DAGLayout": "spectral"}

        selected = {name for name, cb in checkboxes.items() if cb.value} if checkboxes else set()
        if not selected:
            return notebookutils.notebook.runMultiple(dag, run_multiple_args)

        print(f"Targeted run ({len(selected)} notebooks): {sorted(selected)}")
        filtered = []
        for a in dag["activities"]:
            if a["name"] in selected:
                a = dict(a)
                a["dependencies"] = [d for d in a.get("dependencies", []) if d in selected]
                filtered.append(a)
        filtered_dag = dict(dag)
        filtered_dag["activities"] = filtered
        return notebookutils.notebook.runMultiple(filtered_dag, run_multiple_args)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
