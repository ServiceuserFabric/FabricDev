# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9376fc45-1e74-4890-acb2-f16a86c266c5",
# META       "default_lakehouse_name": "Enr",
# META       "default_lakehouse_workspace_id": "3a25df0d-986f-45a8-9140-8e9d88526e86",
# META       "known_lakehouses": [
# META         {
# META           "id": "9376fc45-1e74-4890-acb2-f16a86c266c5"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Enriched entity: gl_entries

# MARKDOWN ********************

# Creates and Saves GL_Entry / FactFinance table to the enriched Layer \
# Added: Proper Conversion, dynamic dimsetkey. Reviewed code - minor alias changes. \
# Outstanding: Closed period from Raw layer. 

# MARKDOWN ********************

# ##  Libraries


# CELL ********************

%run ./FM_Utility

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

datacheck = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

incremental_window =  20
run_incremental = False


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Load data

# CELL ********************

target_table = f"Enr.enr_glEntries"

# Needs to be updated to there is a selection going on towards the DP layer. 

#GL entries only contains data from BC. (it's shortcutted to DP layer directly from Raw)
gl_entries = spark.read.format("delta").table("DP.dp_glEntries")

#Use ENR layer so no duplicates! 
gl_account = spark.read.format("delta").table("Enr.enr_glaccount")
company = spark.read.format("delta").table("DP.dp_company")
gen_business_posting_groups = spark.read.format("delta").table('Enr.enr_genbusinesspostinggroups')
gen_product_posting_groups = spark.read.format("delta").table('Enr.enr_genproductpostinggroups')
dim_set = spark.read.format("delta").table('Enr.enr_dimensionsetentry')


# Defining dummy variables to create readability - imported from data transformer. 
pl_range_end = GlobalParameters.pl_range_end
equity_account = GlobalParameters.equity_account
# Altnertively use IncomeBalance Classifier from Account Table when joining. 
#pl_range_end = 59999 

nulstilres_pattern = GlobalParameters.nulstilres_pattern 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Transform data

# CELL ********************

# We add sourcesystem to the dimensionsetId.
# If we use BC data we make a key Source_BC_DimId key. If it is not BC data then the SourceSystem and key gets joined - the key coming from the birdge table already having the CompanyKey in it. 
gl_entries = (
    gl_entries.alias('a')
    .withColumn(
        'dimensionSetID',
        concat_ws('_', col('a.sourceSystem'), col('a.companyKey'), col('a.dimensionSetID'))

    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

#Join dimension entries & accounts to GL
gl_entries = (gl_entries #For BC dimension
                                .join(gl_account.alias('c'),expr('a.glAccount = c.accountNo AND a.companyKey = c.companyKey' ),'left') 
                                .join(company.alias('comp'),expr('a.companyKey =comp.companyKey'))                               
                                .join(gen_business_posting_groups.alias('bpg'),expr("""a.companyKey = bpg.companyKey 
                                                                                      AND a.genBusPostingGroup = bpg.genBusinessPostingGroupsCode 
                                                                                      AND a.sourceSystem = bpg.sourceSystem"""),'left' )\
                                .join(gen_product_posting_groups.alias('ppg') ,expr("""a.companyKey = ppg.companyKey 
                                                                                      AND a.genProdPostingGroup = ppg.genProductPostingGroupsCode 
                                                                                      AND a.sourceSystem = ppg.sourceSystem"""),'left')
                              #Configure the dimension set to  pick up from either BC or Nav
                                .join(dim_set.alias('b'),expr('dimensionSetID = b.dimensionSetEntryKey'),'left')
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ## Join Date - Selection

# CELL ********************


# Extracting dimensionsetkey columns for select statment.
excluded_columns = ['dimensionSetEntryKey','companyKey','source_system']  # Replace with actual column names you want to exclude
columns_from_b = [col(f'b.{column}') for column in dim_set.columns if column not in excluded_columns]


# Select and combine all fields
result = gl_entries.select(
    # GL fields from table 'a'
  #GL fields
    col('c.accountKey'),
    col('c.accountNo').alias('account'),
    col('comp.companyCurrencyCode'),
    col('a.sourceSystem'),
    col('a.companyKey'),
    
    col('a.amount').alias('amountAccounting'),
    # Compute 'amountReporting' column
    col('c.incomeBalance'),
    when(
        col('c.incomeBalance') != 'Balance Sheet',
        col('a.amount') * -1
    ).otherwise(
        col('a.amount')
    ).alias('amountReporting'),
    #col('a.gLAccountNo'),
    col('a.entryNo'),
    #col('a.balAccountType'),
    col('a.description'),
    col('dimensionSetID'),
    col('a.documentDate'),
    col('a.documentNo'),
    col('a.documentType'),
    #col('a.externalDocumentNo'),
    #col('a.genPostingType'),
    col('a.postingDate'),
    col('a.quantity'),
    #col('a.reasonCode'),
    #col('a.reversed'),
    col('a.sourceCode'),
    col('a.sourceNo'),
    col('a.sourceType'),
    #col('a.systemCreatedAt'),
    col('bpg.genBusinessPostingGroupKey'),
    col('ppg.genProductPostingGroupKey'),
  
    # Adding all dynamic keys from dimensionsetentry
    *columns_from_b,

    # Account fields from table 'c'
    #col('c.incomeBalance')
    col('year'),
    col('month')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Equity Calc

# MARKDOWN ********************

# ### Identify And Tag year Res posts
# 
# Create new equity entries matching 'result' schema
# Only for unfinished years - where accounts hasnt been closed do we poste the result from the P&L down to an equity account to make sure that the balance is correct.
# 


# MARKDOWN ********************

# Step 1: Flag closing entries.
# 
# Step 2: Calculate last closing date.
# 
# Step 3: Filter for unclosed P&L.
# 
# Step 4: Aggregate retained earnings per month.
# 
# Step 5: Create BI rows to equity.
# 
# Step 6: Drop original closing entries (NULSTILRES).
# 
# Step 7: Union synthetic retained earnings rows.
# 
# → Result:
# Closed years: equity entries only from BC 

# CELL ********************

# --------------------------------------------------------------------------
# FIND LAST CLOSING DATE: Identify the last "nulstilres" on the 'result' df
# --------------------------------------------------------------------------
# The combination of is_nulstilres_post and last closing date decides if it should be moved and when we should calc the result from 

# Use a window function to find the latest posting date of a closing entry.
window_spec = Window.partitionBy("companyKey", "sourceSystem")

# Define the conditions for an entry to be a P/L year-end closing entry.
# INSERT NUMERIC TEST ON ACCOUNT!!!
is_pl = col("account").cast("int") <= lit(pl_range_end)
is_nulstilres_post = col("sourceCode").like(nulstilres_pattern) & is_pl

# Add the 'last_closing_date' column to the dataset
result_with_closing_date = result.withColumn(
    "last_closing_date",
    max(when(is_nulstilres_post, col("postingDate"))).over(window_spec)
)


# --------------------------------------------------------------------------
# SUMMARIZE P/L: Calculate the total P/L for all unclosed periods
# --------------------------------------------------------------------------
# Filter for P/L entries that were posted AFTER the last closing date.
unclosed_pl_entries = result_with_closing_date.filter(
    is_pl & (col("last_closing_date").isNull() | (col("postingDate") > col("last_closing_date")))
)

# Group by company and month to sum up the P&L amount.
pl_summary = (
    unclosed_pl_entries
    .groupBy(
        col("companyKey"),
        year("postingDate").alias("year"),
        month("postingDate").alias("month"),
        col("sourceSystem")
    )
    .agg(
        sum("amountAccounting").alias("retained_earnings_amount"),
        first("companyCurrencyCode", ignorenulls=True).alias("companyCurrencyCode")
    )
)


# Month-end posting date (EOM)
retained_earnings_entries_with_date = pl_summary.withColumn(
    "postingDate",
    last_day(expr("make_date(year, month, 1)"))
)

# Fetch details for the equity account to populate incomeBalance, etc.
equity_account_details = (
    gl_account
    .filter(col("accountNo") == lit(equity_account))
    .select("accountKey", "incomeBalance")
    .first()
)
if equity_account_details is None:
    raise ValueError(f"Equity account {equity_account} not found.")

# Create new entries with a schema that exactly matches the 'result' DataFrame
retained_earnings_entries = retained_earnings_entries_with_date.select(
    lit(equity_account_details['accountKey']).alias("accountKey"),
    lit(equity_account).alias("account"),
    # If you really want to force DKK, keep the next line; otherwise use the carried code:
    # lit("DKK").alias("companyCurrencyCode"),retained_earnings_entries_with_date
    col("companyCurrencyCode"),
    col("companyKey"),
    col("retained_earnings_amount").alias("amountAccounting"),
    col("retained_earnings_amount").alias("amountReporting"),
    lit(equity_account_details['incomeBalance']).alias("incomeBalance"),
    # Build stable entryNo (no lit(concat_ws(...)))
    concat_ws("-", lit("RESULTCALC-OPEN-YEAR"),
              col("year").cast("string"),
              lpad(col("month").cast("string"), 2, "0")).alias("entryNo"),
    lit("Automatisk beregnet resultat for periode").alias("description"),
    lit(-1).cast("int").alias("dimensionSetID"),
    col("postingDate").alias("documentDate"),
    col("sourceSystem"),
    lit("RESULTAT").alias("documentNo"),
    lit("BI-CALC").alias("documentType"),
    col("postingDate"),
    lit(0.0).alias("quantity"),
    lit("AUTO").alias("sourceCode"),
    lit(None).cast("string").alias("sourceNo"),
    lit(None).cast("string").alias("sourceType"),
    lit("-1").alias("genBusinessPostingGroupKey"),
    lit("-1").alias("genProductPostingGroupKey"),
    col("year"),
    col("month")
)


if datacheck:
    FM_Utility.compare_dataframe_schemas(result_with_closing_date, retained_earnings_entries)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if datacheck: display(retained_earnings_entries)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Union the result
# First, remove the original "nulstilres" entries from our main dataset.
result = result_with_closing_date.filter(
    ~(col("sourceCode").like(nulstilres_pattern) & is_pl) | col("sourceCode").isNull()
).drop("last_closing_date")

# Ensure column order matches for a clean union

result = result.unionByName(retained_earnings_entries,allowMissingColumns=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Currency Conversion

# CELL ********************

# Define the list of columns for currency handling - these currencies will be translated into local curreny and group currency. 

tcy_columns_list = []  #No transaction currencies exists in finance.
lcy_columns_list = ['amountAccounting','amountReporting'] ## All transactions in finance are always at local currency, which is the company currencycode. 

# Get Currency Dataframe 
currency_df = FM_Utility.get_currency_conversion_df()

# Calling currency transformer adding currency columns for LCY and GCY 
result = FM_Utility.merge_currency_conversion(result, currency_df, "companyCurrencyCode", "companyCurrencyCode", "postingDate", tcy_columns_list, lcy_columns_list, group_currency=GlobalParameters.group_currency)


result =result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# # Write

# CELL ********************

#DataCheck.check_duplicates(result,['entryNo','companyKey','sourceSystem'])

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

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
result.write.mode("overwrite").option("overwriteSchema","true").partitionBy(partition_columns).format("delta").saveAsTable(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
