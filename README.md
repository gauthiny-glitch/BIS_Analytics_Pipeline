# BIS Analytics Pipeline

A portfolio project demonstrating a production-style data engineering pipeline built for the analytical environment of a central bank treasury.

The pipeline ingests synthetic flat-file extracts from a deal booking system (modelled on Openlink Findur), transforms them through a layered dbt DAG, and delivers a star-schema data mart with SDR-denominated metrics and a maturity ladder — the two key outputs a Senior Business Data Analyst would produce for BIS Treasury.

---

## Business Context

The Bank for International Settlements (BIS) manages roughly SDR 182 billion in currency deposits on behalf of ~180 central bank members (Annual Report 2024-25, p. 101). The treasury's core analytical needs are:

1. **Portfolio exposure** — notional outstanding by counterparty, currency, product, and portfolio, all converted to SDR for comparability across the basket (USD, EUR, GBP, JPY, CNY).
2. **Maturity profile** — how much liquidity is locked up and over what horizon, sliced into standard time buckets (overnight → >2 years).

This project builds both outputs end-to-end using open-source tooling that maps directly to the BIS production stack (Microsoft Fabric OneLake + SQL Analytics Endpoint + dbt Core).

---

## Repository Structure

```
BIS_Project_2026/
│
├── bis_analytics/              # dbt project
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml     # declares OneLake source tables
│   │   │   ├── stg_deals.sql
│   │   │   └── stg_fx_rates.sql
│   │   ├── intermediate/
│   │   │   └── int_deals_with_sdr.sql
│   │   └── mart/
│   │       ├── dim_counterparty.sql
│   │       ├── dim_currency.sql
│   │       ├── dim_date.sql
│   │       ├── dim_product.sql
│   │       ├── fct_currency_deposits.sql
│   │       └── mart_maturity_ladder.sql
│   ├── macros/
│   ├── tests/
│   └── analyses/
│
├── data/                       # gitignored — generated locally
│   └── .gitkeep
│
├── docs/                       # study guide and portfolio materials
│   ├── BIS_SelfStudy_Guide.html
│   ├── BIS_SelfStudy_Guide.md
│   └── BIS_Portfolio_Deck.pptx
│
├── scripts/
│   └── generate_data.py        # synthetic data generator
│
├── .gitignore
└── README.md
```

---

## Pipeline DAG

```
src_deals.csv          src_fx_rates.csv
      │                       │
  [OneLake]              [OneLake]
      │                       │
  stg_deals            stg_fx_rates
      └──────────┬────────────┘
                 │
        int_deals_with_sdr
                 │
        ┌────────┴────────┐
        │                 │
    [dims]           fct_currency_deposits
  dim_counterparty        │
  dim_currency            │
  dim_date           mart_maturity_ladder
  dim_product
```

All models materialise as **views** on the Microsoft Fabric SQL Analytics Endpoint (Delta/Parquet, read-only DDL — no CREATE TABLE).

---

## Data Models

| Layer | Model | Rows (approx.) | Key Output |
|---|---|---|---|
| Staging | `stg_deals` | ~500 | Cleaned deal book with typed columns |
| Staging | `stg_fx_rates` | ~240 | Monthly SDR/unit rates Apr 2023 → Mar 2025 |
| Intermediate | `int_deals_with_sdr` | ~500 | Deals joined to SDR rates, `nominal_sdr` calculated |
| Mart — Dims | `dim_counterparty` | ~57 | Central bank members with region |
| Mart — Dims | `dim_currency` | 10 | Currency attributes, SDR basket flag |
| Mart — Dims | `dim_date` | ~730 | Calendar spine with BIS fiscal year (Apr–Mar) |
| Mart — Dims | `dim_product` | 4 | Deposit product types |
| Mart — Fact | `fct_currency_deposits` | ~500 | Star-schema fact table |
| Mart | `mart_maturity_ladder` | ~71 | CROSS JOIN deals × 24 month-ends → 7 time buckets |

---

## Getting Started

### 1. Generate synthetic source data

```bash
pip install pandas numpy python-dateutil
python scripts/generate_data.py
```

This writes `data/src_deals.csv` (~500 rows) and `data/src_fx_rates.csv` (~240 rows).

### 2. Load to OneLake (Microsoft Fabric)

Upload both CSVs to a OneLake Lakehouse via the Fabric UI or the Azure Storage SDK. The dbt `sources.yml` expects them registered as tables in a schema named `raw`.

### 3. Configure dbt profile

Create `~/.dbt/profiles.yml` with your Fabric SQL Analytics Endpoint connection string. The project name is `bis_analytics`.

```yaml
bis_analytics:
  target: dev
  outputs:
    dev:
      type: fabric
      driver: "ODBC Driver 18 for SQL Server"
      server: <your-workspace>.datawarehouse.fabric.microsoft.com
      port: 1433
      database: <your-lakehouse>
      schema: dbt_dev
      authentication: CLI
```

### 4. Run the pipeline

```bash
cd bis_analytics
dbt deps
dbt run
dbt test
```

---

## Documentation

| File | Description |
|---|---|
| `docs/BIS_SelfStudy_Guide.html` | Full technical reference — all 9 SQL models with annotated I/O tables, star schema diagram, SDR mechanics, 32-term glossary, 10-step rebuild recipe |
| `docs/BIS_SelfStudy_Guide.md` | Markdown version of the same guide |
| `docs/BIS_Portfolio_Deck.pptx` | 9-slide portfolio presentation summarising the project for a hiring audience |

---

## Technical Stack

| Tool | Role |
|---|---|
| Microsoft Fabric OneLake | Cloud data lake (Delta/Parquet) |
| Fabric SQL Analytics Endpoint | Read-only T-SQL query layer over Delta |
| dbt Core | Transformation DAG, Jinja-SQL, lineage |
| T-SQL / Views | All models; no `CREATE TABLE` DDL |
| Python + pandas | Synthetic data generation |
| Power BI | Downstream reporting layer |

---

## Key Design Decisions

**Views over tables.** The Fabric SQL Analytics Endpoint does not support `CREATE TABLE` — all dbt models are `materialized = 'view'`. This is standard for this stack and aligns with BIS's Fabric deployment.

**SDR conversion in the intermediate layer.** `nominal_sdr` is calculated once in `int_deals_with_sdr` (joining each deal to the end-of-month SDR rate for its trade month) rather than in every downstream model. This avoids duplicating join logic and keeps the fact table clean.

**Maturity ladder via CROSS JOIN.** `mart_maturity_ladder` generates its as-of date spine with a CROSS JOIN against a hardcoded 24-month date list rather than depending on `dim_date`, keeping it self-contained and runnable without the full star schema.

**Weighted counterparty sampling.** The data generator samples from 57 central bank members with weights calibrated to approximate the Asia-Pacific ~61% / Americas ~9% / Europe ~8% / Middle East ~7% / Africa ~6% / International ~9% distribution shown in BIS Annual Report 2024-25, Graph 4.3 (p. 106).

---

## Author

Portfolio project for BIS Senior Business Data Analyst application, 2025–26.  
Built to demonstrate end-to-end data engineering skills in the BIS Fabric/dbt production stack.
