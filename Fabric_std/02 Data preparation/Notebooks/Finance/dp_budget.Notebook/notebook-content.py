# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9664b42f-75e3-44c2-a937-8a0231cc803e",
# META       "default_lakehouse_name": "DP",
# META       "default_lakehouse_workspace_id": "3a25df0d-986f-45a8-9140-8e9d88526e86",
# META       "known_lakehouses": [
# META         {
# META           "id": "9664b42f-75e3-44c2-a937-8a0231cc803e"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Data Prep: budget
# 
# Ingest data from bc tables, table is writen into dp lakehouse with overwrite
# 1.  write them to DP layer using overwrite
# 
# 
# TODO if there is large volume of data make history loads passive and do incremental load for maybe last 3 mths? (partition month and override specific folder)

# MARKDOWN ********************

# ### Libraries

# CELL ********************

%run FM_Utility

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_table   = "DP.dp_budget" 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load

# CELL ********************


#company_nav = spark.read.table('Raw_dataplatform.nav_lookup_company')


if GlobalParameters.budget_table_name:
    budget_bc = FM_Utility.load_cleaned_dataframe("Raw",GlobalParameters.budget_table_name)
else:
    budget_bc = FM_Utility.load_cleaned_dataframe("Raw","GLBudgetEntry")


name_bc = FM_Utility.load_cleaned_dataframe("Raw","GLBudgetName")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Wrangle manual budget input file

# CELL ********************

budget_bc = (
        budget_bc.select(
            lit(-1).alias("EntryNo"),
            col("BudgetName").alias("Name"),
            col('GlAccountNo').alias('glAccountNo'),
            col("Date").alias("Date"),
            regexp_replace(col("Amount"),",",".").alias("Amount"),
            col("DimensionSetID").alias("DimensionSetID"),
            col("company")
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

name_bc = name_bc.select('Name','Description','Company')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Transform nav tables to be same format as BC

# CELL ********************

# BC
budget_bc = budget_bc.alias('a').join(name_bc.alias('b'),expr("a.Name = b.Name and a.Company = b.Company"),"left")

budget_bc = budget_bc.select(
    col('EntryNo'),
    coalesce(col('b.Description'),col("a.Name")).alias('budgetName'),
    col('a.GLAccountNo').alias('glAccountNo'),
    col('Date').alias('date'),
    col('Amount').alias('amount'),
    col("DimensionSetID"),
    col('a.Company'),
    lit('bc').alias('sourceSystem')
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Make the final table and enforce schema

# CELL ********************

#Final result table and schema
result = budget_bc.select(             
    col('company')             .cast('string').alias('company'),
    col('budgetName')             .cast('string') .alias('budgetName'),            
    col('entryNo')                .cast('long')  .alias('lineNo'),
    col('glAccountNo')            .cast('string').alias('glAccount'),
    col('amount')                 .cast('double').alias('amount'),
    when(col('date') < lit('1900-01-01'),lit('1900-01-01')).otherwise(col('date'))
                                  .cast('date')  .alias('date'),
    col('dimensionSetID')         .cast('long')  .alias('dimensionSetID'),
    col('sourceSystem')           .cast('string').alias('sourceSystem'),

)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Resolve company key

# CELL ********************

result = FM_Utility.add_company_key(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load

# CELL ********************

result.write.mode("overwrite").option("overwriteSchema","true").format("delta").saveAsTable(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
