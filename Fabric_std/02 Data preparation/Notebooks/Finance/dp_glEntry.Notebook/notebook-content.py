# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9664b42f-75e3-44c2-a937-8a0231cc803e",
# META       "default_lakehouse_name": "Raw",
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

# # Data Prep: glEntry
# 
# Ingest data from raw navision and bc tables, table is writen into dp lakehouse with overwrite
# 1. transfer the nav table to same format as BC
# 2. Union the tables and write them to DP layer using overwrite
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

# PARAMETERS CELL ********************

incremental_window = 60
run_incremental = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load

# CELL ********************

target_table   = "DP.dp_glentries" 

glentry_bc = FM_Utility.load_cleaned_dataframe("Raw",'GLEntry', "camel")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Transform nav tables to be same format as BC

# CELL ********************

#Rename BC columns and select wanted columns
# glentry_bc = glentry_bc.join(company,expr('Company = bcCompanyKey'))

glentry_bc = glentry_bc.select(
col('company').alias('company'),
col('EntryNo').alias('entryNo'),
col('GLAccountNo').alias('glAccountNo'),
col('PostingDate').alias('postingDate'),
col('DocumentType').alias('documentType'),
col('DocumentNo').alias('documentNo'),
col('Description').alias('description'),
col('Amount').alias('amount'),
col('SourceCode').alias('sourceCode'),
col('PriorYearEntry').alias('priorYearEntry'),
col('Quantity').alias('quantity'),
col('BusinessUnitCode').alias('businessUnitCode'),
col('ReasonCode').alias('reasonCode'),
col('GenBusPostingGroup').alias('genBusPostingGroup'),
col('GenProdPostingGroup').alias('genProdPostingGroup'),
col('DocumentDate').alias('documentDate'),
col('SourceType').alias('sourceType'),
col('SourceNo').alias('sourceNo'),
col('DimensionSetID').alias('dimensionSetID'),
col('systemModifiedAt').cast("date").alias('systemModifiedDate'),
lit('bc').alias('sourceSystem')
)#.filter(expr(f"postingDate >= '{cut_off_date}'"))
#display(glentry_bc)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Make the final table and enforce schema

# CELL ********************

#Final result table and schema
result = (glentry_bc.select(             
    col('company')             .cast('string').alias('company'),            
    col('entryNo')                .cast('string').alias('entryNo'),
    col('glAccountNo')            .cast('string').alias('glAccount'),
    col('amount')                 .cast('double').alias('amount'),
    col('documentType')           .cast('string').alias('documentType'),
    col('documentNo')             .cast('string').alias('documentNo'),
    col('description')            .cast('string').alias('description'),
    when(col('documentDate') < lit('1900-01-01'),lit('1900-01-01')).otherwise(col('documentDate'))
                                  .cast('date')  .alias('documentDate'),
    col('sourceCode')             .cast('string').alias('sourceCode'),
    col('priorYearEntry')         .cast('boolean').alias('priorYearEntry'),
    col('quantity')               .cast('double').alias('quantity'),
    col('reasonCode')             .cast('string').alias('reasonCode'),
    col('genBusPostingGroup')     .cast('string').alias('genBusPostingGroup'),
    col('genProdPostingGroup')    .cast('string').alias('genProdPostingGroup'),
  
    #Handle old dates
    when(col('postingDate')  < lit('1900-01-01'),lit('1900-01-01')).otherwise(col('postingDate'))
                                  .cast('date')  .alias('postingDate'),
    col('sourceType')             .cast('string').alias('sourceType'),
    col('sourceNo')               .cast('string').alias('sourceNo'),
    col('dimensionSetID')         .cast('int')   .alias('dimensionSetID'),
    col('sourceSystem')           .cast('string').alias('sourceSystem')
)# for partitions and system fields
.withColumn('year',year(col('postingDate')))
.withColumn('month',month(col('postingDate')))
.withColumn("dp_processed_at", current_timestamp()) # Now this is a separate, valid operation
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load

# CELL ********************

result = FM_Utility.add_company_key(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#figure out the partitial load. 
partition_columns = ["year","month"]

if run_incremental: 
    #The tables are partitioned by month, take window time and from the first day of the month write to table. 
    incremental_start = (datetime.today().date() - timedelta(days=incremental_window)).strftime("%Y-%m-01")

    result = result.filter(expr(f"postingDate >= '{incremental_start}'"))
    # Reference: https://www.red-gate.com/simple-talk/blogs/dynamic-partitioning-and-a-simple-incremental-load/
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    result.write \
        .mode("overwrite") \
        .format("delta") \
        .option("mergeSchema","true") \
        .partitionBy(partition_columns) \
        .saveAsTable(target_table)
    notebookutils.notebook.exit('Write succesful with incremental write.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result.write.mode("overwrite").partitionBy(partition_columns).option("overwriteSchema","true").format("delta").saveAsTable(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
