# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d75a272e-b5f9-4822-859f-51c084058231",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "dc072922-4ffb-4424-868c-28087b02ecba",
# META       "known_lakehouses": [
# META         {
# META           "id": "d75a272e-b5f9-4822-859f-51c084058231"
# META         },
# META         {
# META           "id": "4b77bc3c-8482-4301-8454-b7b97ada8da8"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### nb_silver_crypto_transform
# 
# **Layer:** Silver — Cleansing & Enrichment  
# **Source:** `lh_bronze_dev` → Delta Table `raw_crypto_prices`  
# **Destination:** `lh_silver` → Delta Table `silver_crypto_prices`  
# **Schedule:** Daily (after Bronze ingestion)  
# 
# This notebook reads raw crypto data from the Bronze layer, applies data quality checks,
# standardizes types, removes duplicates, and calculates derived business metrics.
# 
# Only unprocessed ingestion dates are loaded (incremental pattern), ensuring the pipeline
# is safe to re-run without duplicating records in Silver.


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Tables in Fabric Lakehouse are stored under the default 'dbo' schema folder.
# The path must include '/dbo/' to correctly resolve the Delta table location.
bronze_abfs = notebookutils.lakehouse.get("lh_bronze")["properties"]["abfsPath"]

SOURCE_PATH       = f"{bronze_abfs}/Tables/dbo/raw_crypto_prices"
DESTINATION_TABLE = "silver_crypto_prices"

print(f"Source path: {SOURCE_PATH}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Incremental Load from Bronze

# CELL ********************

# Read Bronze data using the ABFS path instead of table name,
# since the source lives in a secondary (non-default) lakehouse.
df_bronze = spark.read.format("delta").load(SOURCE_PATH)

if spark.catalog.tableExists(DESTINATION_TABLE):
    processed_dates = (
        spark.read.table(DESTINATION_TABLE)
        .select("ingestion_date")
        .distinct()
    )
    df_new = df_bronze.join(processed_dates, on="ingestion_date", how="left_anti")
else:
    df_new = df_bronze

record_count = df_new.count()
print(f"New records to process: {record_count}")

if record_count == 0:
    print("Silver is already up to date. Nothing to process.")
    notebookutils.notebook.exit("UP_TO_DATE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Deduplication and Null Handling

# CELL ********************

# Deduplicate by (id, ingestion_date) as a safety net against Bronze reruns.
# Nulls in supply fields are expected — some coins have no max supply (e.g. ETH).
# We fill them with 0.0 to avoid silent failures in downstream aggregations.
df_clean = (
    df_new
    .dropDuplicates(["id", "ingestion_date"])
    .fillna(0.0, subset=["total_supply", "max_supply",
                          "price_change_percentage_7d_in_currency",
                          "price_change_percentage_30d_in_currency"])
)

print(f"Records after deduplication: {df_clean.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Type Casting and Standardization

# CELL ********************

# last_updated comes from Bronze as a raw ISO string from the API.
# We cast it to TimestampType here so Silver has proper time-aware columns
# ready for time intelligence in Gold and the Semantic Model.
df_typed = (
    df_clean
    .withColumn("last_updated", F.to_timestamp("last_updated"))
    .withColumn("symbol", F.upper(F.col("symbol")))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Derived Business Metrics

# CELL ********************

# These metrics are calculated in Silver rather than Gold because they are
# coin-level indicators derived directly from raw fields — not aggregations.
# Gold will use them as inputs for more complex dimensional calculations.

window_by_date = Window.partitionBy("ingestion_date")

df_enriched = (
    df_typed
    # How close the current price is to the all-time high (100% = at ATH)
    .withColumn("price_vs_ath_pct",
        F.round((F.col("current_price") / F.col("ath")) * 100, 2))

    # Liquidity indicator — high ratio suggests elevated trading activity
    .withColumn("volume_to_market_cap_ratio",
        F.round(F.col("total_volume") / F.col("market_cap"), 4))

    # Market dominance — each coin's share of total market cap on that day
    .withColumn("total_market_cap_day",
        F.sum("market_cap").over(window_by_date))
    .withColumn("market_dominance_pct",
        F.round((F.col("market_cap") / F.col("total_market_cap_day")) * 100, 4))
    .drop("total_market_cap_day")

    # Size category based on market cap — standard industry classification
    .withColumn("market_cap_category",
        F.when(F.col("market_cap") >= 10_000_000_000, "Large Cap")
         .when(F.col("market_cap") >= 1_000_000_000,  "Mid Cap")
         .when(F.col("market_cap") >= 100_000_000,    "Small Cap")
         .otherwise("Micro Cap"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write to Silver Delta Table

# CELL ********************

# We use append mode here since incremental filtering in Cell 2 already
# guarantees no duplicate ingestion_dates will be written.
(
    df_enriched.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(DESTINATION_TABLE)
)

print(f"{df_enriched.count()} records written to '{DESTINATION_TABLE}'")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation

# CELL ********************

# Spot-check: row counts and key derived metrics per ingestion date.
spark.sql(f"""
    SELECT
        ingestion_date,
        COUNT(*)                        AS total_coins,
        ROUND(SUM(market_cap) / 1e9, 2) AS total_market_cap_usd_bn,
        ROUND(AVG(price_vs_ath_pct), 2) AS avg_price_vs_ath_pct,
        COUNT(DISTINCT market_cap_category) AS cap_categories
    FROM {DESTINATION_TABLE}
    GROUP BY ingestion_date
    ORDER BY ingestion_date DESC
""").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 
