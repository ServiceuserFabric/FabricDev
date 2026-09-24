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

# # Enriched entity: CurrencyExchangeRates
# ## Important
# This code does not work on several currency tables, which is often the case when managing multiple companies. Therefore, there needs to be a method for selecting which currency conversions are being used.
# 
# In this example, we use the following script for each company's base currency pair. For instance, one company uses DKK, another uses EUR, and another uses BRL. Based on this, we create our currency conversion logic for the group.
# 
# However, if there are more companies with different conversion rates, a selection of which currency pair to use must be made.
# 
# Make sure that the raw table **<u>only have one basecurrency / currency</u>** pair to avoid overlapping currency

# MARKDOWN ********************

# ##  Libraries

# CELL ********************

%run FM_Utility

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load

# PARAMETERS CELL ********************

target_table = "Enr.enr_CurrencyExchangeRates"

exh_rates = spark.read.table('DP.dp_currencyexchangerates')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


exh_rates = exh_rates.select(
    col("companyKey").cast('string'),
    col('startingDate'),
    col('adjustmentExchRateAmount'),
    col("exchangeRateAmount"),
    col("currencyCode"),
    col("relationalCurrencyCode"),
    col("relationalExchRateAmount")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Enrich each rate row with the LCY of its owning company. BC's relationalCurrencyCode is
# typically blank in the source (BC implicitly uses the company's LCY), so we look it up from
# GlobalParameters.company_data via companyKey and COALESCE it with any explicit value the source
# did populate. Done BEFORE dedup so the dedup partition can include fromCurrency.
company_lcy_lookup = spark.createDataFrame(
    [(c[2], c[1]) for c in GlobalParameters.company_data],
    ["companyKey", "companyCurrencyCode"]
)
exh_rates = exh_rates.join(company_lcy_lookup, on="companyKey", how="left")
exh_rates = exh_rates.withColumn(
    "fromCurrency",
    coalesce(col("relationalCurrencyCode"), col("companyCurrencyCode"))
)

# Normalize the rate to per-100 LCY. BC stores rates with varying denominators in
# ExchangeRateAmount (commonly 100, but some rows are entered as 1). Downstream conversion in
# merge_currency_conversion hardcodes the per-100 assumption (LCY / rate * 100), so we rescale
# here once: rate * (100 / ExchangeRateAmount). Filter out exchangeRateAmount=0 first to avoid
# divide-by-zero on malformed source rows (any rate quoted as 0:N has no defined direction).
exh_rates = exh_rates.filter(col("exchangeRateAmount") != 0)
exh_rates = exh_rates.withColumn(
    "relationalExchRateAmount",
    col("relationalExchRateAmount") * (lit(100.0) / col("exchangeRateAmount"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Partition includes fromCurrency so multi-LCY clients keep both DKK and SEK (etc.) rates for the
#same foreign currency on the same date instead of silently dropping one of them.
exh_rates = FM_Utility.add_row_number(exh_rates, ['startingDate', 'currencyCode', 'fromCurrency'], 'companyKey')
exh_rates = exh_rates.filter(col('row_num')==1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform data (Standard)

# CELL ********************

# Checks if there are more currencypairs from the source system. If this is the case, filter out the unneeded currency pairs in customer specific code

def clean_data(df):
    # Create a 'CurrencyPair' column by concatenating 'companyCurrencyCode' and 'currencyCode'
    df = df.withColumn("CurrencyPair", col("currencyCode"))
    
    # Get distinct count of 'companyKey' for each 'CurrencyPair'
    df_counts = df.groupBy('CurrencyPair').agg(countDistinct('companyKey').alias('companyKey_count'))
    
    # Check for any CurrencyPair with more than 1 distinct companyKey
    error_df = df_counts.filter(col('companyKey_count') > 1)
    
    if error_df.count() > 0:
        print("⚠️ Warning: More than 1 distinct companyKey found per CurrencyPair:")
        display(error_df)
    
    # Drop any null values and sort by 'CurrencyPair'
    df_clean = df_counts.dropna().sort('CurrencyPair')
    
    return 0


clean_data(exh_rates)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = exh_rates

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# if we ever want non sql way: https://datastud.dev/posts/time-series-resample

# CELL ********************

df.createOrReplaceTempView("currencyExchangeRates")

# fromCurrency was resolved upstream (COALESCE of relationalCurrencyCode and the company's LCY).
# Every window function partitions by (currencyCode, fromCurrency) so multi-LCY clients get a
# parallel time-series per LCY-foreign pair — no interleaving of DKK and SEK rates inside the
# same EUR partition, no broken date ranges, no overlap false-positives.
df2 = spark.sql("""
    WITH ranked_data AS (
        SELECT
            currencyCode,
            fromCurrency,
            startingDate,
            relationalExchRateAmount,
            ROW_NUMBER() OVER (PARTITION BY currencyCode, fromCurrency ORDER BY startingDate) AS row_num
        FROM
            currencyExchangeRates
    ),
    date_ranges AS (
        SELECT
            currencyCode,
            fromCurrency,
            relationalExchRateAmount,
            startingDate AS startDate,
            date_sub(LEAD(startingDate, 1, '2030-12-31') OVER (PARTITION BY currencyCode, fromCurrency ORDER BY startingDate), 1) AS endDate
        FROM
            ranked_data
    ),
    date_sequence AS (
        SELECT
            currencyCode,
            fromCurrency,
            relationalExchRateAmount,
            startDate,
            endDate,
            EXPLODE(SEQUENCE(startDate, endDate)) AS date
        FROM
            date_ranges
    ),
    overlapping_dates AS (
        SELECT
            currencyCode,
            fromCurrency,
            relationalExchRateAmount,
            date,
            LAG(date, 1) OVER (PARTITION BY currencyCode, fromCurrency ORDER BY date) AS prev_date
        FROM
            date_sequence
    )
SELECT
    fromCurrency as FromCurrency,
    currencyCode as toCurrency,
    relationalExchRateAmount,
    date,
    prev_date,
    CASE WHEN prev_date IS NOT NULL AND date <= prev_date THEN 'Overlap' ELSE 'No Overlap' END AS overlap_status
FROM
    overlapping_dates
ORDER BY
    fromCurrency,
    currencyCode,
    date
""")

# Select specific columns
df2_output_to_write = df2.select("fromCurrency", "toCurrency", "relationalExchRateAmount", "date")

# Append inverse rows so LCY -> GCY joins resolve in both directions, but only where the base
# doesn't already supply a direct rate for that pair. An explicit source rate always wins over
# a derived inverse — this also handles two failure modes:
#   1. Self-rate rows (e.g. BC exports SEK -> SEK = 100): their inverse has the identical key
#      and would create a duplicate.
#   2. Reciprocal rates from different companies (DKK -> SEK from company 1 AND SEK -> DKK from
#      company 2): both directions are in the base, so we don't want inverses overwriting either.
inverse_candidates = df2_output_to_write.select(
    col("toCurrency").alias("fromCurrency"),
    col("fromCurrency").alias("toCurrency"),
    (lit(100.0) / col("relationalExchRateAmount")).alias("relationalExchRateAmount"),
    col("date")
)
base_keys = df2_output_to_write.select("fromCurrency", "toCurrency", "date")
inverse_rows = inverse_candidates.join(
    base_keys, on=["fromCurrency", "toCurrency", "date"], how="left_anti"
)
df = df2_output_to_write.unionByName(inverse_rows)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df_sorted = df.orderBy(col("date").asc())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#DataQuality
DataCheck.check_duplicates(df,"date","FromCurrency","toCurrency")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load 

# CELL ********************

df.write.mode("overwrite").option("overwriteSchema","true").format("delta").saveAsTable(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
