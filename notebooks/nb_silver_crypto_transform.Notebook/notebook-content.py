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
# **Layer:** Silver — Cleansing, Enrichment & SCD Type 2
# **Source:** `lh_bronze` → Delta Table `raw_crypto_prices`
# **Destination:** `lh_silver` → Delta Table `silver_crypto_prices`
# **Schedule:** Daily (after Bronze ingestion)
#
# This notebook reads raw crypto data from the Bronze layer, applies data quality checks,
# standardizes types, and calculates derived business metrics.
#
# Records are written using **SCD Type 2** (Slowly Changing Dimension Type 2):
# each day a new version of every coin is inserted, and the previous current row
# is expired by setting `valid_to` and `is_current = False`.
# This preserves full price history while enabling efficient point-in-time queries.


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

bronze_abfs = notebookutils.lakehouse.get("lh_bronze")["properties"]["abfsPath"]

SOURCE_PATH       = f"{bronze_abfs}/Tables/raw_crypto_prices"
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

# Skip dates already present in Silver (checked against valid_from, which equals
# ingestion_date for the version inserted on that day).
df_bronze = spark.read.format("delta").load(SOURCE_PATH)

if spark.catalog.tableExists(DESTINATION_TABLE):
    processed_dates = (
        spark.read.table(DESTINATION_TABLE)
        .select(F.col("valid_from").alias("ingestion_date"))
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

# ### SCD Type 2 — Add Validity Columns

# CELL ********************

# Each version of a coin row carries three SCD Type 2 control columns:
#   valid_from  — the ingestion_date this version became active
#   valid_to    — the ingestion_date the next version replaced it (NULL = still current)
#   is_current  — True for the latest version, False for all historical ones
#
# Point-in-time query:  WHERE valid_from <= '<date>' AND (valid_to > '<date>' OR valid_to IS NULL)
# Latest state query:   WHERE is_current = True
df_scd = (
    df_enriched
    .withColumn("valid_from", F.col("ingestion_date").cast(DateType()))
    .withColumn("valid_to",   F.lit(None).cast(DateType()))
    .withColumn("is_current", F.lit(True))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write to Silver — SCD Type 2 Merge

# CELL ********************

if not spark.catalog.tableExists(DESTINATION_TABLE):
    # Initial load — no previous versions to expire
    (df_scd.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(DESTINATION_TABLE))
    print(f"{df_scd.count()} records written (initial load)")
else:
    dt = DeltaTable.forName(spark, DESTINATION_TABLE)

    # Step 1 — Expire the current row for every coin that has a new version incoming.
    # Sets valid_to = today's ingestion_date and flips is_current to False.
    dt.alias("target").merge(
        df_scd.alias("source"),
        "target.id = source.id AND target.is_current = true"
    ).whenMatchedUpdate(set={
        "valid_to":   "source.valid_from",
        "is_current": "false",
    }).execute()

    # Step 2 — Insert the new current versions.
    (df_scd.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(DESTINATION_TABLE))

    print(f"{df_scd.count()} new versions written")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation

# CELL ********************

# Current snapshot: one row per coin with is_current = True
print("=== Current snapshot (is_current = True) ===")
spark.sql(f"""
    SELECT
        ingestion_date,
        COUNT(*)                        AS total_coins,
        ROUND(SUM(market_cap) / 1e9, 2) AS total_market_cap_usd_bn,
        ROUND(AVG(price_vs_ath_pct), 2) AS avg_price_vs_ath_pct,
        COUNT(DISTINCT market_cap_category) AS cap_categories
    FROM {DESTINATION_TABLE}
    WHERE is_current = true
    GROUP BY ingestion_date
    ORDER BY ingestion_date DESC
""").show()

# History: total versions per coin confirms SCD Type 2 is accumulating correctly
print("=== Version history per coin (top 10 by versions) ===")
spark.sql(f"""
    SELECT id, name, COUNT(*) AS versions,
           MIN(valid_from) AS first_seen,
           MAX(valid_from) AS last_seen
    FROM {DESTINATION_TABLE}
    GROUP BY id, name
    ORDER BY versions DESC
    LIMIT 10
""").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
