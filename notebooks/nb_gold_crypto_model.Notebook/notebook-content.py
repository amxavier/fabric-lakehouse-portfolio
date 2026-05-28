# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "de113525-d92d-4596-8d5e-7de1cb332a5a",
# META       "default_lakehouse_name": "lh_gold",
# META       "default_lakehouse_workspace_id": "dc072922-4ffb-4424-868c-28087b02ecba",
# META       "known_lakehouses": [
# META         {
# META           "id": "de113525-d92d-4596-8d5e-7de1cb332a5a"
# META         },
# META         {
# META           "id": "d75a272e-b5f9-4822-859f-51c084058231"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Introduction 
# ### nb_gold_crypto_model
# 
# **Layer:** Gold — Dimensional Modeling (Star Schema)  
# **Source:** `lh_silver_dev` → Delta Table `silver_crypto_prices`  
# **Destination:** `lh_gold` → Delta Tables `dim_coin`, `dim_date`, `fact_prices`  
# **Schedule:** Daily (after Silver transformation)  
# 
# This notebook builds the Star Schema for the semantic layer:
# - `dim_coin` — coin attributes (slowly changing, Type 1)
# - `dim_date` — date dimension with time intelligence attributes
# - `fact_prices` — daily price metrics per coin, referencing both dimensions
# 
# The model is designed to be consumed directly by Power BI as a Semantic Model,
# with relationships, hierarchies and DAX measures applied on top.


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Resolve Silver lakehouse ABFS path to read across lakehouses reliably.
silver_abfs = notebookutils.lakehouse.get("lh_silver")["properties"]["abfsPath"]

SOURCE_PATH = f"{silver_abfs}/Tables/dbo/silver_crypto_prices"

DIM_COIN  = "dim_coin"
DIM_DATE  = "dim_date"
FACT_TABLE = "fact_prices"

print(f"Source: {SOURCE_PATH}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load Silver Data

# CELL ********************

# Read the full Silver table — Gold rebuilds dimensions via SCD Type 1 (overwrite
# latest attributes) and appends only new dates to the fact table.
df_silver = spark.read.format("delta").load(SOURCE_PATH)

print(f"Silver records loaded: {df_silver.count()}")
df_silver.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build dim_coin

# CELL ********************

# dim_coin stores the latest known attributes for each coin (SCD Type 1).
# We use the most recent ingestion_date to ensure the dimension always reflects
# current metadata, overwriting previous values on each run.
df_dim_coin = (
    df_silver
    .orderBy(F.col("ingestion_date").desc())
    .dropDuplicates(["id"])
    .select(
        F.col("id")             .alias("coin_id"),
        F.col("symbol"),
        F.col("name"),
        F.col("market_cap_rank").alias("current_rank"),
        F.col("market_cap_category"),
        F.col("ath"),
        F.col("circulating_supply"),
        F.col("max_supply"),
        F.col("total_supply"),
    )
    .orderBy("current_rank")
)

# Overwrite ensures dim_coin always reflects the latest coin attributes.
(
    df_dim_coin.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(DIM_COIN)
)

print(f"dim_coin: {df_dim_coin.count()} coins loaded")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build dim_date

# CELL ********************

# dim_date is generated from the distinct dates present in Silver.
# Time intelligence attributes (year, quarter, month, week, is_weekend) are
# added here so Power BI DAX measures can use them without extra calculations.
df_dates = df_silver.select("ingestion_date").distinct()

df_dim_date = (
    df_dates
    .withColumn("date_id",      F.date_format("ingestion_date", "yyyyMMdd").cast("int"))
    .withColumn("year",         F.year("ingestion_date"))
    .withColumn("quarter",      F.quarter("ingestion_date"))
    .withColumn("month",        F.month("ingestion_date"))
    .withColumn("month_name",   F.date_format("ingestion_date", "MMMM"))
    .withColumn("week_of_year", F.weekofyear("ingestion_date"))
    .withColumn("day_of_week",  F.dayofweek("ingestion_date"))
    .withColumn("day_name",     F.date_format("ingestion_date", "EEEE"))
    .withColumn("is_weekend",   F.dayofweek("ingestion_date").isin([1, 7]))
    .orderBy("ingestion_date")
)

(
    df_dim_date.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(DIM_DATE)
)

print(f"dim_date: {df_dim_date.count()} dates loaded")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build fact_prices

# CELL ********************

# fact_prices is the central fact table — one row per coin per day.
# It references dim_coin and dim_date via surrogate keys (coin_id, date_id),
# and contains all numeric metrics used in Power BI measures.
df_dim_date_keys = spark.read.table(f"dbo.{DIM_DATE}").select("ingestion_date", "date_id")

df_fact = (
    df_silver
    .join(df_dim_date_keys, on="ingestion_date", how="left")
    .select(
        F.col("id")                                       .alias("coin_id"),
        F.col("date_id"),
        F.col("ingestion_date"),
        F.col("current_price"),
        F.col("market_cap"),
        F.col("total_volume"),
        F.col("high_24h"),
        F.col("low_24h"),
        F.col("price_change_24h"),
        F.col("price_change_percentage_24h"),
        F.col("price_change_percentage_7d_in_currency")   .alias("price_change_pct_7d"),
        F.col("price_change_percentage_30d_in_currency")  .alias("price_change_pct_30d"),
        F.col("price_vs_ath_pct"),
        F.col("volume_to_market_cap_ratio"),
        F.col("market_dominance_pct"),
    )
)

# Append only new dates not yet in the fact table.
if spark.catalog.tableExists(FACT_TABLE):
    processed_dates = (
        spark.read.table(f"dbo.{FACT_TABLE}")
        .select("ingestion_date").distinct()
    )
    df_fact_new = df_fact.join(processed_dates, on="ingestion_date", how="left_anti")
else:
    df_fact_new = df_fact

(
    df_fact_new.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(FACT_TABLE)
)

print(f"fact_prices: {df_fact_new.count()} records written")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation

# CELL ********************

# Cross-check: join fact with both dimensions to validate referential integrity.
spark.sql(f"""
    SELECT
        f.ingestion_date,
        dd.year,
        dd.month_name,
        COUNT(DISTINCT f.coin_id)               AS total_coins,
        ROUND(SUM(f.market_cap) / 1e9, 2)       AS market_cap_usd_bn,
        ROUND(AVG(f.price_vs_ath_pct), 2)        AS avg_vs_ath_pct,
        ROUND(AVG(f.market_dominance_pct), 4)    AS avg_dominance_pct
    FROM dbo.{FACT_TABLE}     f
    JOIN dbo.{DIM_COIN}       d  ON f.coin_id = d.coin_id
    JOIN dbo.{DIM_DATE}       dd ON f.date_id = dd.date_id
    GROUP BY f.ingestion_date, dd.year, dd.month_name
    ORDER BY f.ingestion_date DESC
""").show()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
