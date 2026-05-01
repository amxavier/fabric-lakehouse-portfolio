# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4b77bc3c-8482-4301-8454-b7b97ada8da8",
# META       "default_lakehouse_name": "lh_bronze_dev",
# META       "default_lakehouse_workspace_id": "dc072922-4ffb-4424-868c-28087b02ecba",
# META       "known_lakehouses": [
# META         {
# META           "id": "4b77bc3c-8482-4301-8454-b7b97ada8da8"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### nb_bronze_coingecko_ingestion
# 
# **Layer:** Bronze — Raw Ingestion  
# **Source:** CoinGecko Public API (`/coins/markets`)  
# **Destination:** `lh_bronze_dev` → Delta Table `raw_crypto_prices`  
# **Schedule:** Daily  
# 
# This notebook ingests the top 100 cryptocurrencies by market cap from the CoinGecko 
# public API and stores the raw payload as a Delta table in the Bronze Lakehouse.
# 
# No transformations are applied at this stage — data is preserved in its original 
# structure to ensure full lineage traceability. Metadata columns (`ingestion_ts`, 
# `ingestion_date`, `source`) are added to support auditing and idempotency control.


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

# Standard library and PySpark imports needed for HTTP requests,
# timestamp handling, schema definition, and DataFrame operations.
import requests
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, IntegerType, TimestampType, DateType
)

# CoinGecko public endpoint — no API key required for the markets endpoint.
# We fetch the top 100 coins by market cap in USD, including 7d and 30d
# price change percentages to enrich the raw payload from the start.
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
TABLE_NAME    = "raw_crypto_prices"
INGESTION_TS  = datetime.now(timezone.utc)

PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 100,
    "page": 1,
    "sparkline": False,
    "price_change_percentage": "7d,30d"
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Fetch Data from API

# CELL ********************

# We use raise_for_status() to immediately surface any HTTP errors
# (4xx/5xx) as exceptions, so the notebook fails fast and visibly
# rather than silently producing an empty or malformed dataset.
response = requests.get(COINGECKO_URL, params=PARAMS, timeout=30)
response.raise_for_status()
data = response.json()

print(f"Records fetched: {len(data)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Define Schema and Build DataFrame

# CELL ********************

import pandas as pd

# Explicit column selection and type mapping using pandas as an intermediary.
# The CoinGecko API returns inconsistent numeric types across coins (e.g., an integer
# price for BTC, a float for altcoins), which causes PySparkTypeError when enforcing
# a strict schema directly. Pandas handles mixed-type coercion gracefully before
# the data is handed off to Spark.
column_types = {
    "id":                                      "str",
    "symbol":                                  "str",
    "name":                                    "str",
    "current_price":                           "float64",
    "market_cap":                              "Int64",
    "market_cap_rank":                         "Int32",
    "total_volume":                            "Int64",
    "high_24h":                                "float64",
    "low_24h":                                 "float64",
    "price_change_24h":                        "float64",
    "price_change_percentage_24h":             "float64",
    "price_change_percentage_7d_in_currency":  "float64",
    "price_change_percentage_30d_in_currency": "float64",
    "circulating_supply":                      "float64",
    "total_supply":                            "float64",
    "max_supply":                              "float64",
    "ath":                                     "float64",
    "last_updated":                            "str",
}

pdf = pd.DataFrame(data)[list(column_types.keys())]

# Safe type casting: numeric fields must be converted to float first before
# casting to integer types, since the API may return values like 0.0 or 1.0
# that pandas cannot directly cast from float64 to Int64.
for col, dtype in column_types.items():
    if dtype in ("Int64", "Int32"):
        pdf[col] = pd.to_numeric(pdf[col], errors="coerce").round(0).astype(dtype)
    else:
        pdf[col] = pdf[col].astype(dtype)


df = spark.createDataFrame(pdf)

# Metadata columns are added here — not in Silver — because they describe
# the ingestion event itself: when it happened and where the data came from.
# This ensures full lineage traceability from the raw layer onwards.
df = (df
    .withColumn("ingestion_ts",   F.lit(INGESTION_TS.isoformat()).cast(TimestampType()))
    .withColumn("ingestion_date", F.lit(INGESTION_TS.date().isoformat()).cast(DateType()))
    .withColumn("source",         F.lit("coingecko_markets_api"))
)

print("Schema:")
df.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Idempotency Check and Write to Delta

# CELL ********************

# Idempotency ensures that re-running this notebook on the same day
# does not produce duplicate records in the Bronze table.
# We check for existing records with today's ingestion_date before writing,
# which is a safer and simpler alternative to using MERGE for raw ingestion.
already_ingested = False

if spark.catalog.tableExists(TABLE_NAME):
    count = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {TABLE_NAME}
        WHERE ingestion_date = '{INGESTION_TS.date().isoformat()}'
    """).collect()[0]["n"]
    already_ingested = count > 0

if already_ingested:
    print(f"Data for {INGESTION_TS.date()} already ingested. Skipping.")
else:
    # mergeSchema allows the table to evolve if CoinGecko adds new fields
    # in the future, without breaking the pipeline.
    (df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(TABLE_NAME)
    )
    print(f"{df.count()} records written to '{TABLE_NAME}'")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation

# CELL ********************

# Quick row count per ingestion_date to confirm data landed correctly
# and to spot any gaps or duplicate runs at a glance.
spark.sql(f"""
    SELECT ingestion_date, COUNT(*) AS total_records
    FROM {TABLE_NAME}
    GROUP BY ingestion_date
    ORDER BY ingestion_date DESC
""").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
