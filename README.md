# microsoft-fabric-medallion-lakehouse

End-to-end Data Engineering project built on **Microsoft Fabric**, implementing the **Medallion Architecture** (Bronze / Silver / Gold) with automated CI/CD via **GitHub Actions** and a **Power BI** analytical dashboard.

---

## Architecture Overview

CoinGecko Public API
│
▼
┌─────────────────┐
│   Bronze Layer  │  Raw ingestion → Delta Table (raw_crypto_prices)
│  lh_bronze_dev  │  Idempotent daily load · Schema enforcement · Lineage metadata
└────────┬────────┘
│
▼
┌─────────────────┐
│   Silver Layer  │  Cleansing · Type casting · Deduplication
│  lh_silver_dev  │  Derived metrics: price_vs_ath_pct · market_dominance · cap_category
└────────┬────────┘
│
▼
┌─────────────────┐
│   Gold Layer    │  Star Schema: fact_prices · dim_coin · dim_date
│  lh_gold_dev    │  Optimized for Power BI Semantic Model consumption
└────────┬────────┘
│
▼
┌─────────────────────────┐
│  Power BI Semantic Model │  DAX measures · Relationships · Time intelligence
│  sm_crypto_medallion     │
└────────┬────────────────┘
│
▼
┌──────────────────┐
│  Power BI Report │  rpt_crypto_dashboard
│  rpt_crypto_*    │  Market cap · ATH analysis · Dominance by category
└──────────────────┘



---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Platform | Microsoft Fabric |
| Storage | OneLake (Delta Lake) |
| Processing | Apache Spark (PySpark) |
| Orchestration | Fabric Data Pipeline |
| Source | CoinGecko Public API |
| Semantic Layer | Power BI Semantic Model + DAX |
| Visualization | Power BI Report |
| CI/CD | GitHub Actions |
| Version Control | Git (Fabric Git Integration) |

---

## Project Structure

├── .github/
│   └── workflows/
│       ├── ci.yml                  # Validates Fabric artifact structure on every push
│       ├── cd_deploy.yml           # Promotes DEV → PRD via Fabric Deployment Pipeline API
│       └── schedule_ingestion.yml  # Triggers daily Bronze→Silver→Gold pipeline at 6am UTC
│
├── lh_bronze_dev.Lakehouse/        # Bronze Lakehouse metadata
├── lh_silver_dev.Lakehouse/        # Silver Lakehouse metadata
├── lh_gold_dev.Lakehouse/          # Gold Lakehouse metadata
│
├── nb_bronze_coingecko_ingestion.Notebook/   # Ingests top 100 coins from CoinGecko API
├── nb_silver_crypto_transform.Notebook/      # Cleanses and enriches Bronze data
├── nb_gold_crypto_model.Notebook/            # Builds Star Schema (fact + dims)
│
├── config/
│   └── params.json                 # Pipeline parameters
└── docs/
└── arquitetura/                # Architecture documentation



---

## Data Pipeline

### Bronze — Raw Ingestion
- Fetches top 100 cryptocurrencies by market cap from CoinGecko (`/coins/markets`)
- Stores raw payload as Delta table with ingestion metadata
- Idempotency check prevents duplicate daily loads
- Schema explicitly defined to enforce data contracts with downstream layers

### Silver — Cleansing & Enrichment
- Incremental load (only unprocessed `ingestion_date` values)
- Type casting, deduplication, null handling
- Derived business metrics:
  - `price_vs_ath_pct` — how close current price is to all-time high
  - `volume_to_market_cap_ratio` — liquidity indicator
  - `market_dominance_pct` — each coin's share of total daily market cap
  - `market_cap_category` — Large / Mid / Small / Micro Cap classification

### Gold — Dimensional Modeling (Star Schema)

fact_prices ──── dim_coin
│
└────────── dim_date



| Table | Type | Description |
|---|---|---|
| `fact_prices` | Fact | Daily price metrics per coin |
| `dim_coin` | Dimension | Coin attributes (SCD Type 1) |
| `dim_date` | Dimension | Date with time intelligence attributes |

---

## Power BI Semantic Model

**DAX Measures:**
- `Total Market Cap (USD)` — total market capitalization
- `Total Volume 24h (USD)` — total trading volume
- `Avg Price vs ATH (%)` — average distance from all-time high
- `Top Coin` — highest market cap coin
- `Avg Price Change 7d (%)` — average weekly price change
- `Large Cap Dominance (%)` — large cap share of total market

---

## CI/CD Pipeline

Push to dev branch
│
▼
ci.yml — Validates all Fabric artifacts exist
│
▼
Pull Request dev → main
│
▼
cd_deploy.yml — Authenticates with Azure AD (Service Principal)
— Calls Fabric Deployment Pipeline API
— Promotes DEV workspace → PRD workspace
│
▼
schedule_ingestion.yml — Runs daily at 6am UTC
— Triggers pl_medallion_orchestration
— Keeps Bronze→Silver→Gold data fresh



### Environments

| Environment | Workspace | Git Branch |
|---|---|---|
| Development | `lakehouses_dev` | `dev` |
| Production | `lakehouses_prd` | `main` |

---

## Setup

### Prerequisites
- Microsoft Fabric workspace (Trial or capacity)
- Azure AD Service Principal with Fabric Member access
- GitHub repository with Actions enabled

### GitHub Secrets Required

| Secret | Description |
|---|---|
| `AZURE_CLIENT_ID` | Service Principal Application ID |
| `AZURE_TENANT_ID` | Azure AD Tenant ID |
| `AZURE_CLIENT_SECRET` | Service Principal Client Secret |
| `FABRIC_WORKSPACE_ID_DEV` | DEV workspace ID |
| `FABRIC_DEPLOYMENT_PIPELINE_ID` | Deployment Pipeline ID |
| `FABRIC_PIPELINE_ID` | Data Pipeline object ID |

---

## Author

**Andrelino Xavier**
Data Engineer · Brazil
[GitHub](https://github.com/amxavier)
