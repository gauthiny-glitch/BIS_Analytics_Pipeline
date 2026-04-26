# BIS Analytics Pipeline — Self-Study Guide

> **Role:** Senior Business Data Analyst · **Institution:** Bank for International Settlements  
> **Stack:** MS Fabric · dbt Core · Power BI · T-SQL  
> **Data period:** Apr 2023 – Mar 2025 (FY2023-24 + FY2024-25)  
> **Models:** 2 staging · 1 intermediate · 5 mart · 1 analytics mart

---

## Table of Contents

1. [BIS Institutional Overview](#1-bis-institutional-overview)
2. [Tools & Prerequisites](#2-tools--prerequisites)
3. [Project Tools vs BIS Production](#3-project-tools-vs-bis-production)
4. [Data Format Flow](#4-data-format-flow)
5. [Star Schema Design](#5-star-schema-design)
6. [SQL Function Reference](#6-sql-function-reference)
7. [The dbt Pipeline](#7-the-dbt-pipeline)
   - [Layer 0 — Raw Sources](#layer-0--raw-sources)
   - [Layer 1 — stg_deals](#layer-1--stg_deals)
   - [Layer 2 — stg_fx_rates](#layer-2--stg_fx_rates)
   - [Layer 3 — int_deals_with_sdr](#layer-3--int_deals_with_sdr)
   - [Layer 4 — dim_counterparty](#layer-4--dim_counterparty)
   - [Layer 5 — dim_currency](#layer-5--dim_currency)
   - [Layer 6 — dim_product](#layer-6--dim_product)
   - [Layer 7 — dim_date](#layer-7--dim_date)
   - [Layer 8 — fct_currency_deposits](#layer-8--fct_currency_deposits)
   - [Layer 9 — mart_maturity_ladder](#layer-9--mart_maturity_ladder)
8. [Business Metrics Rationale](#8-business-metrics-rationale)
9. [End-to-End Recipe](#9-end-to-end-recipe)
10. [Lexique / Glossary](#10-lexique--glossary)

---

## 1. BIS Institutional Overview

The **Bank for International Settlements (BIS)** is an international organisation founded in 1930, headquartered in Basel, Switzerland. It is owned by 63 central banks representing ~95% of world GDP. Often called *"the central bank of central banks"* — its primary customers are central banks and official sector institutions.

### What the BIS does

The BIS operates across three main areas:

- **Banking services** — Takes deposits from ~200 central bank customers and invests the proceeds in high-quality assets. Total assets: **SDR 431.3 billion** at end-March 2025. *(Annual Report p.102)*
- **Research & Statistics** — Publishes global financial data (BIS statistics), coordinates regulatory standards (Basel Accords), produces economic research via BIS Working Papers.
- **Meeting services** — Hosts meetings of central bank governors and international financial forums (FSB, BCBS, CPMI, IAIS).

### Unit of account — the SDR

The BIS reports all financial figures in **Special Drawing Rights (SDR)**. The SDR is a basket of five currencies defined by the IMF: **USD, EUR, GBP, JPY, CNY**. It serves as the Bank's unit of account, or "numeraire". *(Annual Report p.103)*

In the pipeline, all deal amounts are converted from their original currency into SDR using a monthly exchange rate table (`src_fx_rates`). This conversion happens in `int_deals_with_sdr`.

### Currency deposit products

| Product code | Full name | Description |
|---|---|---|
| `FIXBIS_DEP` | FIXBIS | Fixed-rate BIS deposit. Fixed term, fixed interest rate. Most common product. |
| `MTI_DEP` | MTI | Medium-term instrument. Longer tenor (typically 1–10 years), fixed rate. |
| `SIGHT_DEP` | Sight | Overnight/on-demand deposit. No fixed maturity — redeemable at any time. |
| `NOTICE_DEP` | Notice | Deposit redeemable after a fixed advance notice period. |

*(Annual Report p.105)*

### BIS fiscal year

The BIS financial year runs from **1 April to 31 March**. *(Annual Report p.101)*

- FY2023-24 = 1 April 2023 → 31 March 2024
- FY2024-25 = 1 April 2024 → 31 March 2025

This is why `dim_date.fiscal_year` uses April as the start of FQ1, not January.

### Geographic distribution of deposits

| Region | Share | Key central banks |
|---|---|---|
| Asia-Pacific | **61%** | People's Bank of China, Bank of Japan, Reserve Bank of India |
| International institutions | 9% | IMF, ECB and similar |
| Americas | 9% | Federal Reserve, Banco de Brasil |
| Europe | 8% | Deutsche Bundesbank, Banque de France |
| Middle East | 7% | Saudi Arabia, UAE central banks |
| Africa | 6% | South African Reserve Bank, Central Bank of Nigeria |

*(Annual Report p.105–106, Graph 4.3)*

### Key financial figures (FY2024-25)

| Metric | Value | Source |
|---|---|---|
| Total assets (end-March 2025) | SDR 431.3 billion | p.102 |
| Currency deposits all-time high | SDR 355.3 billion (30 Dec 2024) | p.103 |
| Average currency deposits FY2024-25 | SDR 314.1 billion | p.104 |
| Number of customers | ~200 | p.104 |

### Source system — Openlink Findur

The BIS booking system for Banking Department deals is **Openlink Findur** — an enterprise treasury and trading platform used by banks and central banks worldwide. The CSV files (`src_deals.csv`, `src_fx_rates.csv`) in this project simulate a flat file extract from Findur. Deal IDs follow the format `FIN-YYYYMM-NNNNNN`.

---

## 2. Tools & Prerequisites

### Software to install locally

| Tool | Version | Purpose | Install |
|---|---|---|---|
| **Python** | 3.11+ | dbt runs on Python; data generation scripts | python.org |
| **dbt Core + dbt-fabric** | 1.8+ | SQL transformation framework + Fabric adapter | `pip install dbt-fabric` |
| **Azure CLI** | latest | Authenticates dbt to Fabric via Entra ID token | aka.ms/installazurecliwindows |
| **Power BI Desktop** | latest | Report authoring and semantic model editing | Microsoft Store |
| **VS Code** | latest | SQL + YAML editing | code.visualstudio.com |

### Cloud services required

| Service | Purpose |
|---|---|
| **Microsoft Fabric workspace** | Hosts Lakehouses, SQL Analytics Endpoint, semantic model |
| **Fabric Lakehouse (bis_raw)** | Raw data storage — Delta/Parquet files on OneLake |
| **Fabric Lakehouse (bis_analytics)** | Transformed views (dbt output) |
| **Microsoft Entra ID tenant** | Authentication for dbt → Fabric connection |

> **Important — Fabric SQL Analytics Endpoint limitation:** The endpoint is *read-only for DDL*. You cannot run `CREATE TABLE`. dbt models must be materialised as **views**. If you need physical Delta tables, use a PySpark notebook instead.

### Authentication recipe

```bash
# Re-authenticate every ~1 hour
az login --tenant 04f991f3-0023-4b95-ab0d-bebbdbc8df14 --allow-no-subscriptions
# Press 1 to confirm tenant

# Run all mart models
cd C:\Users\sosa_\Downloads\BIS_Project_2026\bis_analytics
dbt run --select mart
```

---

## 3. Project Tools vs BIS Production

| Layer | This project (portfolio) | BIS production (inferred) |
|---|---|---|
| Source system | Python-generated CSV (simulating Findur export) | Openlink Findur — live deal extract via scheduled job |
| Raw ingestion | PySpark notebook — CSV → Delta table in OneLake | ADF pipeline or Fabric Data Factory |
| Raw storage | MS Fabric Lakehouse (bis_raw, dbo schema, Delta format) | MS Fabric Lakehouse or Azure Data Lake Storage Gen2 |
| Transformation | dbt Core (open source, CLI) | Likely dbt Core or dbt Cloud; or SSMS stored procedures |
| Transformation target | MS Fabric SQL Analytics Endpoint (views only) | Same — or Fabric Warehouse (supports DDL) |
| Semantic model | Power BI Import mode (SQL Server connector) | Power BI Direct Lake if Delta tables, Import if views |
| Reporting | Power BI Desktop → published to workspace | Power BI Service — role-level security, certified datasets |
| Orchestration | Manual CLI (`dbt run`) | Fabric Data Factory pipelines, or Azure DevOps CI/CD |
| Version control | Local git | Azure DevOps or GitHub Enterprise |
| FX rates | Python-generated CSV (simulated IMF SDR rates) | Live IMF SDR feed, Bloomberg, or Refinitiv |
| Authentication | Azure CLI interactive login | Service Principal / Managed Identity |

> **Key interview insight:** The architecture is identical. The differences are automation (manual vs scheduled), data sources (synthetic vs live Findur), and authentication (interactive vs service principal). The SQL and data modelling logic is production-grade.

---

## 4. Data Format Flow

```
src_deals.csv          PySpark notebook       OneLake Delta/Parquet
(flat file · strings)  ──────────────────►   (bis_raw.dbo.src_deals)
                                                        │
                                                        ▼
                                          SQL Analytics Endpoint
                                          (dbt views — T-SQL)
                                                        │
                                                        ▼
                                          Semantic Model
                                          (Import mode · DAX)
                                                        │
                                                        ▼
                                          Power BI Report
                                          (visuals · slicers)
```

| Format | Where | Why |
|---|---|---|
| **CSV** | Python output / Findur extract | Universal exchange format. Simple strings — no types enforced. |
| **Delta / Parquet** | OneLake (bis_raw Lakehouse) | Columnar, compressed, ACID transactions. Native Fabric format. Much faster than CSV for analytics. |
| **SQL Views** | SQL Analytics Endpoint | dbt materialises models as views — no data duplication. A view is a saved SQL query. Only DDL option on Fabric endpoint. |
| **Power BI Import** | Semantic model / .pbix | Data pulled into VertiPaq in-memory engine. Fast DAX queries; requires periodic refresh. |
| **Direct Lake** | Would replace Import mode | Reads Delta files directly from OneLake — no import needed. Requires physical Delta tables, not views. Not used in this project. |

---

## 5. Star Schema Design

The data model follows a **star schema**: one central fact table surrounded by dimension tables, each joined via a foreign key. Standard pattern for analytical data warehouses and Power BI semantic models.

### Why not fully normalised (3NF)?

Normalised databases avoid duplication by splitting data into many tables — great for transactional systems, bad for analytics because you need many complex joins to answer a simple question. Star schemas denormalise intentionally: one join per dimension, predictable query patterns, fast aggregation.

### Why not one wide table?

The "one big table" anti-pattern creates problems: repeated strings (counterparty name duplicated per deal), no reusable calendar logic, no single place to update a dimension attribute, and Power BI cannot optimise relationships between facts and dimensions.

### Schema diagram

```
┌─────────────────────┐     ┌──────────────────────────────────────┐     ┌──────────────────┐
│  dim_counterparty   │     │       ⭐ fct_currency_deposits        │     │   dim_currency   │
│─────────────────────│     │──────────────────────────────────────│     │──────────────────│
│ PK cpty_code        │◄────│ PK  deal_id                          │────►│ PK currency      │
│    cpty_name        │     │ FK  deal_type    → dim_product       │     │    currency_name  │
│    cpty_country     │     │ FK  cpty_code    → dim_counterparty  │     │    is_sdr_basket  │
│    region           │     │ FK  currency     → dim_currency      │     └──────────────────┘
└─────────────────────┘     │ FK  trade_date_key   → dim_date      │
                            │ FK  value_date_key   → dim_date      │     ┌──────────────────┐
┌─────────────────────┐     │ FK  maturity_date_key→ dim_date      │     │   dim_product    │
│      dim_date       │     │  M  nominal_amount                   │     │──────────────────│
│─────────────────────│     │  M  nominal_amount_sdr               │────►│ PK deal_type     │
│ PK date_key         │◄────│  M  rate_pct                         │     │    product_name  │
│    full_date        │     │  M  rate_decimal                     │     │    description   │
│    fiscal_year      │     │     deal_status, portfolio, book…    │     └──────────────────┘
│    fiscal_quarter   │     └──────────────────────────────────────┘
│    month_name…      │
└─────────────────────┘     ┌──────────────────────────────────────┐
                            │      mart_maturity_ladder            │
                            │──────────────────────────────────────│
                            │     as_of_date                       │
                            │     maturity_bucket                  │
                            │  M  deal_count                       │
                            │  M  total_sdr                        │
                            │  (standalone — not joined to fact)   │
                            └──────────────────────────────────────┘
```

`PK` = Primary Key · `FK` = Foreign Key · `M` = Measure (additive numeric)

> **Three date foreign keys:** `fct_currency_deposits` has three FK relationships to `dim_date` — for trade date, value date, and maturity date. In Power BI, only one can be the "active" relationship; the others are "inactive" and must be activated in DAX with `USERELATIONSHIP()`.

---

## 6. SQL Function Reference

| Function | What it does | Used in | Example |
|---|---|---|---|
| `TRY_CAST(x AS type)` | Converts x to target type. Returns NULL on failure instead of throwing an error. | stg_deals, stg_fx_rates | `TRY_CAST('2023-04-03' AS DATE)` → `2023-04-03` |
| `EOMONTH(date)` | Returns the last calendar day of the month containing date. | int_deals_with_sdr, mart_maturity_ladder | `EOMONTH('2023-04-15')` → `2023-04-30` |
| `EOMONTH(date, n)` | Same but offsets by n months first. n=-1 means "the month before". | int_deals_with_sdr | `EOMONTH('2023-04-15', -1)` → `2023-03-31` |
| `DATEADD(part, n, date)` | Adds n units of part (day/month/year) to date. | dim_date, mart_maturity_ladder | `DATEADD(month, 3, '2023-04-01')` → `2023-07-01` |
| `DATEDIFF(part, start, end)` | Returns end - start in units of part. Positive if end > start. | mart_maturity_ladder | `DATEDIFF(day, '2025-03-31', '2025-04-07')` → `7` |
| `DATEFROMPARTS(y, m, d)` | Constructs a DATE from integer year, month, day. Used to decode date keys. | mart_maturity_ladder | `DATEFROMPARTS(2025, 3, 31)` → `2025-03-31` |
| `YEAR(date)` / `MONTH(date)` / `DAY(date)` | Extract integer year, month, or day from a date. | fct_currency_deposits, dim_date | `YEAR('2023-04-03')` → `2023` |
| `DATENAME(part, date)` | Returns the name of the specified part as a string. | dim_date | `DATENAME(month, '2023-04-03')` → `'April'` |
| `DATEPART(part, date)` | Returns an integer for the specified part. | dim_date | `DATEPART(quarter, '2023-07-01')` → `3` |
| `CONCAT(a, b, …)` | Joins strings. NULLs treated as empty strings. | dim_date | `CONCAT('FY', 2023, '-', 24)` → `'FY2023-24'` |
| `RIGHT(str, n)` | Returns the rightmost n characters of a string. | dim_date | `RIGHT(2024, 2)` → `'24'` |
| `ROW_NUMBER() OVER (ORDER BY …)` | Window function — assigns a sequential integer to each row. | dim_date | Generates 0,1,2…729 for date spine |
| `CROSS JOIN` | Cartesian product — every row of A × every row of B. | mart_maturity_ladder | 500 deals × 24 dates = 12,000 rows |
| Date key: `y*10000 + m*100 + d` | Converts date to sortable YYYYMMDD integer. Avoids FORMAT() (unavailable on Fabric). | fct_currency_deposits, dim_date | `2023*10000 + 4*100 + 3` → `20230403` |
| Date key decode: `key/10000`, `(key/100)%100`, `key%100` | Extracts year, month, day from YYYYMMDD integer. | mart_maturity_ladder | `20250331/10000=2025`, `(20250331/100)%100=3`, `20250331%100=31` |

---

## 7. The dbt Pipeline

The pipeline has four logical layers. dbt resolves execution order automatically from `ref()` and `source()` macros — this is the **DAG** (Directed Acyclic Graph).

```
src_deals / src_fx_rates
        │
        ▼  [staging layer]
stg_deals / stg_fx_rates
        │
        ▼  [intermediate layer]
int_deals_with_sdr
        │
        ▼  [mart layer]
fct_currency_deposits + dim_counterparty + dim_currency + dim_product + dim_date
        │
        ▼  [analytics mart]
mart_maturity_ladder
```

> **dbt macros:** `{{ source('bis_raw', 'src_deals') }}` declares an external source table. `{{ ref('stg_deals') }}` declares a dependency on another dbt model and ensures correct execution order. These macros build the DAG.

---

### Layer 0 — Raw Sources

The two source CSV files simulate extracts from Openlink Findur. They are loaded into Microsoft Fabric OneLake as Delta tables by a PySpark notebook.

#### src_deals — input schema

| Column | Raw type | Example | Notes |
|---|---|---|---|
| `deal_id` | VARCHAR | FIN-202304-000001 | Unique deal identifier |
| `deal_type` | VARCHAR | FIXBIS_DEP | Product code from Findur |
| `cpty_code` | VARCHAR | PBOC_01 | Counterparty short code |
| `cpty_name` | VARCHAR | People's Bank of China | Full counterparty name |
| `cpty_country` | VARCHAR | CN | ISO 3166-1 alpha-2 |
| `trade_date` | VARCHAR | 2023-04-03 | String — must be cast |
| `value_date` | VARCHAR | 2023-04-05 | Settlement date — string |
| `maturity_date` | VARCHAR | 2024-04-05 | NULL for SIGHT_DEP |
| `currency` | VARCHAR | USD | ISO 4217 |
| `nominal_amount` | VARCHAR | 500000000.00 | Full units, not millions |
| `rate_pct` | VARCHAR | 2.50 | Percentage form (2.50 = 2.50%) |
| `deal_status` | VARCHAR | VALIDATED | VALIDATED / MATURED / CANCELLED |
| `portfolio` | VARCHAR | TRSRY-APAC | Internal BIS portfolio code |
| `book` | VARCHAR | BK-001 | Trading book identifier |
| `created_dt` | VARCHAR | 2023-04-03 09:15:00 | Audit timestamp — string |
| `last_modified_dt` | VARCHAR | 2023-04-03 09:15:00 | Audit timestamp — string |

#### src_fx_rates — input schema

| Column | Raw type | Example | Notes |
|---|---|---|---|
| `rate_date` | VARCHAR | 2023-03-31 | Always last day of month |
| `currency_code` | VARCHAR | USD | ISO 4217 |
| `sdr_per_unit` | FLOAT | 0.752140 | 1 USD = 0.752140 SDR |

---

### Layer 1 — stg_deals

**File:** `models/staging/stg_deals.sql` · **Materialisation:** view

**Purpose:** Take raw `src_deals` and make it type-safe. Every VARCHAR string column is cast to its correct data type. No rows removed, no aggregation — staging is purely about type safety and naming consistency.

**Key operations:**
- `TRY_CAST` everywhere — returns NULL on failure instead of crashing the pipeline
- `rate_decimal` derived column: `rate_pct / 100` (e.g. 2.50 → 0.025000), computed once here for all downstream models

```sql
with source as (

    select * from {{ source('bis_raw', 'src_deals') }}

),

staged as (

    select

        deal_id,
        deal_type,
        cpty_code,
        cpty_name,
        cpty_country,

        -- Cast dates from string YYYY-MM-DD to DATE
        try_cast(trade_date    as date) as trade_date,
        try_cast(value_date    as date) as value_date,
        try_cast(maturity_date as date) as maturity_date,

        currency,
        try_cast(nominal_amount as decimal(20, 2))    as nominal_amount,

        -- Keep percentage form AND derive decimal form
        try_cast(rate_pct as decimal(10, 6))          as rate_pct,
        try_cast(rate_pct as decimal(10, 6)) / 100.0  as rate_decimal,

        deal_status,
        portfolio,
        book,

        -- Cast audit timestamps from string to DATETIME2
        try_cast(created_dt       as datetime2) as created_dt,
        try_cast(last_modified_dt as datetime2) as last_modified_dt

    from source

)

select * from staged
```

#### Output schema — stg_deals

| Column | Output type | Example | Changed? |
|---|---|---|---|
| `trade_date` | DATE | 2023-04-03 | ✓ VARCHAR → DATE |
| `value_date` | DATE | 2023-04-05 | ✓ VARCHAR → DATE |
| `maturity_date` | DATE | 2024-04-05 | ✓ VARCHAR → DATE (NULL for Sight) |
| `nominal_amount` | DECIMAL(20,2) | 500000000.00 | ✓ VARCHAR → DECIMAL |
| `rate_pct` | DECIMAL(10,6) | 2.500000 | ✓ VARCHAR → DECIMAL |
| `rate_decimal` | DECIMAL(10,6) | 0.025000 | ✓ New derived column |
| `created_dt` | DATETIME2 | 2023-04-03 09:15:00 | ✓ VARCHAR → DATETIME2 |

---

### Layer 2 — stg_fx_rates

**File:** `models/staging/stg_fx_rates.sql` · **Materialisation:** view

**Purpose:** Cast `rate_date` from VARCHAR to DATE. The numeric `sdr_per_unit` is already typed correctly by PySpark on load. Grain: one row per (rate_date, currency_code) pair.

```sql
with source as (

    select * from {{ source('bis_raw', 'src_fx_rates') }}

),

staged as (

    select

        try_cast(rate_date as date) as rate_date,
        currency_code,
        sdr_per_unit

    from source

)

select * from staged
```

#### Output schema — stg_fx_rates

| Column | Output type | Example |
|---|---|---|
| `rate_date` | DATE | 2023-03-31 |
| `currency_code` | VARCHAR | USD |
| `sdr_per_unit` | FLOAT | 0.752140 |

---

### Layer 3 — int_deals_with_sdr

**File:** `models/intermediate/int_deals_with_sdr.sql` · **Materialisation:** view

**Purpose:** Join staged deals to staged FX rates to compute `nominal_amount_sdr` — the deal value in SDR, the BIS unit of account.

**The join logic:**
1. **Currency match** — `deals.currency = fx_rates.currency_code`
2. **Date match** — Rate for the *last day of the month before* the deal's value date: `EOMONTH(DATEADD(month, -1, value_date))`. Example: value date 2023-04-15 → use rate for 2023-03-31

**Why the month before?** Simulates a common treasury practice: the FX rate used is the one known at the *start* of the settlement month — avoids look-ahead bias.

**Why LEFT JOIN?** Deals with no matching FX rate still appear with `nominal_amount_sdr = NULL` (visible for investigation), rather than being silently dropped.

```sql
with deals as (
    select * from {{ ref('stg_deals') }}
),

fx_rates as (
    select * from {{ ref('stg_fx_rates') }}
),

joined as (

    select
        d.deal_id,
        d.deal_type,
        d.cpty_code,
        d.cpty_name,
        d.cpty_country,
        d.trade_date,
        d.value_date,
        d.maturity_date,
        d.currency,
        d.nominal_amount,
        d.rate_pct,
        d.rate_decimal,
        d.deal_status,
        d.portfolio,
        d.book,

        fx.rate_date                               as fx_rate_date,
        fx.sdr_per_unit,

        -- SDR conversion: nominal × rate
        d.nominal_amount * fx.sdr_per_unit         as nominal_amount_sdr,

        d.created_dt,
        d.last_modified_dt

    from deals d

    left join fx_rates fx
        on  fx.currency_code = d.currency
        and fx.rate_date = eomonth(dateadd(month, -1, d.value_date))

)

select * from joined
```

#### New columns added

| Column | Type | Example | Notes |
|---|---|---|---|
| `fx_rate_date` | DATE | 2023-03-31 | Rate date that was matched |
| `sdr_per_unit` | FLOAT | 0.752140 | SDR per 1 unit of deal currency |
| `nominal_amount_sdr` | FLOAT | 376070000.00 | 500M USD × 0.752140 = 376.07M SDR |

---

### Layer 4 — dim_counterparty

**File:** `models/mart/dim_counterparty.sql` · **Materialisation:** view

**Purpose:** Build the counterparty dimension. Extract every unique counterparty from the deal book and add a `region` column derived from the ISO country code, using BIS geographic classification. *(Annual Report p.106)*

**The region classification:** A `CASE WHEN cpty_country = 'CN' THEN 'Asia-Pacific'...` statement maps each ISO alpha-2 code to one of six BIS regions. The initial version only covered ~40 countries; 27 were missing (Turkmenistan TM, Macau MO, Tajikistan TJ, etc.) and fell into `ELSE 'Other'`. A diagnostic SQL query identified all 27 missing codes, which were added to the correct regional blocks. Result: zero rows in "Other".

```sql
with counterparties as (

    select distinct cpty_code, cpty_name, cpty_country
    from {{ ref('int_deals_with_sdr') }}

),

with_region as (

    select
        cpty_code,
        cpty_name,
        cpty_country,

        case cpty_country
            -- Asia-Pacific (61% of deposits)
            when 'CN' then 'Asia-Pacific'
            when 'JP' then 'Asia-Pacific'
            when 'IN' then 'Asia-Pacific'
            when 'TM' then 'Asia-Pacific'  -- added: Turkmenistan
            when 'MO' then 'Asia-Pacific'  -- added: Macau
            when 'TJ' then 'Asia-Pacific'  -- added: Tajikistan
            when 'KZ' then 'Asia-Pacific'  -- added: Kazakhstan
            -- ... (33 total Asia-Pacific codes)

            -- Americas (9%)
            when 'US' then 'Americas'
            when 'BR' then 'Americas'
            when 'CR' then 'Americas'  -- added: Costa Rica
            when 'HN' then 'Americas'  -- added: Honduras
            -- ...

            -- Europe (8%)
            when 'DE' then 'Europe'
            when 'AZ' then 'Europe'    -- added: Azerbaijan
            when 'AM' then 'Europe'    -- added: Armenia
            when 'GE' then 'Europe'    -- added: Georgia
            -- ...

            -- Middle East (7%), Africa (6%)
            -- ...

            -- International organisations (9%)
            when 'XX' then 'International'
            when 'EU' then 'International'  -- added: European institutions

            else 'Other'  -- zero rows if all codes mapped
        end as region

    from counterparties

)

select * from with_region
```

#### Output schema — dim_counterparty

| Column | Type | Example |
|---|---|---|
| `cpty_code` | VARCHAR (PK) | PBOC_01 |
| `cpty_name` | VARCHAR | People's Bank of China |
| `cpty_country` | VARCHAR | CN |
| `region` | VARCHAR | Asia-Pacific |

---

### Layer 5 — dim_currency

**File:** `models/mart/dim_currency.sql` · **Materialisation:** view

**Purpose:** Static lookup — one row per currency. Adds human-readable name and `is_sdr_basket` flag distinguishing the five IMF SDR basket currencies from others.

```sql
with currencies as (
    select * from (
        values
            ('USD', 'US Dollar',          'Y'),
            ('EUR', 'Euro',               'Y'),
            ('GBP', 'British Pound',      'Y'),
            ('JPY', 'Japanese Yen',       'Y'),
            ('CNY', 'Chinese Renminbi',   'Y'),
            ('CHF', 'Swiss Franc',        'N'),
            ('AUD', 'Australian Dollar',  'N'),
            ('CAD', 'Canadian Dollar',    'N'),
            ('SGD', 'Singapore Dollar',   'N'),
            ('KRW', 'South Korean Won',   'N')
    ) as t (currency, currency_name, is_sdr_basket)
)
select * from currencies
```

#### Output schema — dim_currency

| Column | Type | Example |
|---|---|---|
| `currency` | VARCHAR (PK) | USD |
| `currency_name` | VARCHAR | US Dollar |
| `is_sdr_basket` | CHAR(1) | Y |

---

### Layer 6 — dim_product

**File:** `models/mart/dim_product.sql` · **Materialisation:** view

**Purpose:** Static lookup — one row per BIS deposit product type. Maps Findur deal type codes to human-readable names. *(Annual Report p.105)*

```sql
with products as (
    select * from (
        values
            ('FIXBIS_DEP', 'FIXBIS',  'Fixed-rate BIS deposit. Fixed term, fixed interest rate. Most common product.'),
            ('MTI_DEP',    'MTI',     'Medium-term instrument. Longer tenor deposit, typically 1-10 years.'),
            ('SIGHT_DEP',  'Sight',   'Sight account. Overnight / on-demand deposit. No fixed maturity.'),
            ('NOTICE_DEP', 'Notice',  'Notice account. Deposit redeemable with advance notice period.')
    ) as t (deal_type, product_name, product_description)
)
select * from products
```

#### Output schema — dim_product

| Column | Type | Example |
|---|---|---|
| `deal_type` | VARCHAR (PK) | FIXBIS_DEP |
| `product_name` | VARCHAR | FIXBIS |
| `product_description` | VARCHAR | Fixed-rate BIS deposit… |

---

### Layer 7 — dim_date

**File:** `models/mart/dim_date.sql` · **Materialisation:** view

**Purpose:** Generate a calendar dimension covering every day in the data period (1 Apr 2023 → 31 Mar 2025 = 730 days). Each row has calendar *and* BIS fiscal year/quarter attributes.

**Date spine generation trick:** On Fabric SQL Analytics Endpoint, system tables like `sys.all_objects` are unavailable. Instead, CTEs are cross-joined to multiply row counts exponentially: 2 → 4 → 16 → 256 → 65,536. Top 730 rows taken, `ROW_NUMBER()` assigns offsets 0–729, each added to the start date with `DATEADD(day, n, '2023-04-01')`.

**BIS fiscal year formula:**
- If `MONTH(date) >= 4` → fiscal year starts in `YEAR(date)` → e.g. April 2023 = FY2023-24
- If `MONTH(date) < 4` → fiscal year started the previous year → e.g. February 2024 = FY2023-24

```sql
with
n1 as (select 1 as n union all select 1),
n2 as (select 1 as n from n1, n1 as b),    -- 4 rows
n3 as (select 1 as n from n2, n2 as b),    -- 16 rows
n4 as (select 1 as n from n3, n3 as b),    -- 256 rows
n5 as (select 1 as n from n4, n4 as b),    -- 65,536 rows

nums as (
    select top 730
        row_number() over (order by (select null)) - 1 as n
    from n5
),

date_sequence as (
    select dateadd(day, n, cast('2023-04-01' as date)) as full_date
    from nums
),

with_attributes as (
    select
        -- Primary key: pure arithmetic — avoids FORMAT() issue on Fabric
        year(full_date) * 10000
        + month(full_date) * 100
        + day(full_date)                             as date_key,

        full_date,
        year(full_date)                              as year,
        month(full_date)                             as month_num,
        datename(month, full_date)                   as month_name,
        datepart(quarter, full_date)                 as quarter,

        -- BIS fiscal year: April to March
        case
            when month(full_date) >= 4
            then concat('FY', year(full_date), '-', right(year(full_date) + 1, 2))
            else concat('FY', year(full_date) - 1, '-', right(year(full_date), 2))
        end                                          as fiscal_year,

        -- BIS fiscal quarter (FQ1 = Apr/May/Jun, etc.)
        case month(full_date)
            when 4  then 'FQ1' when 5  then 'FQ1' when 6  then 'FQ1'
            when 7  then 'FQ2' when 8  then 'FQ2' when 9  then 'FQ2'
            when 10 then 'FQ3' when 11 then 'FQ3' when 12 then 'FQ3'
            when 1  then 'FQ4' when 2  then 'FQ4' when 3  then 'FQ4'
        end                                          as fiscal_quarter

    from date_sequence
)

select * from with_attributes
```

#### Output schema — dim_date

| Column | Type | Example |
|---|---|---|
| `date_key` | INT (PK) | 20230401 |
| `full_date` | DATE | 2023-04-01 |
| `year` | INT | 2023 |
| `month_num` | INT | 4 |
| `month_name` | VARCHAR | April |
| `quarter` | INT | 2 |
| `fiscal_year` | VARCHAR | FY2023-24 |
| `fiscal_quarter` | VARCHAR | FQ1 |

---

### Layer 8 — fct_currency_deposits

**File:** `models/mart/fct_currency_deposits.sql` · **Materialisation:** view

**Purpose:** Central fact table of the star schema. One row per deal. Selects from `int_deals_with_sdr` and adds integer date keys (YYYYMMDD) for joining to `dim_date`.

**Date key encoding:** `YEAR(date) * 10000 + MONTH(date) * 100 + DAY(date)` → e.g. 2 April 2023 = `20230402`. Replaces `FORMAT(date, 'yyyyMMdd')` which is unavailable on Fabric SQL Analytics Endpoint.

**Degenerate dimensions:** `deal_status`, `portfolio`, `book`, `fx_rate_date`, `sdr_per_unit` are kept on the fact table — no separate dimension table needed as they have too many values or limited analytical grouping value.

```sql
with deals as (
    select * from {{ ref('int_deals_with_sdr') }}
),

final as (
    select
        deal_id,

        -- Foreign keys to dimensions
        deal_type,    -- → dim_product.deal_type
        cpty_code,    -- → dim_counterparty.cpty_code
        currency,     -- → dim_currency.currency

        -- Date foreign keys (YYYYMMDD integer = dim_date.date_key)
        year(trade_date)    * 10000 + month(trade_date)    * 100 + day(trade_date)    as trade_date_key,
        year(value_date)    * 10000 + month(value_date)    * 100 + day(value_date)    as value_date_key,
        year(maturity_date) * 10000 + month(maturity_date) * 100 + day(maturity_date) as maturity_date_key,

        -- Measures
        nominal_amount,
        nominal_amount_sdr,
        rate_pct,
        rate_decimal,

        -- Degenerate dimensions
        deal_status,
        portfolio,
        book,
        fx_rate_date,
        sdr_per_unit,

        created_dt,
        last_modified_dt

    from deals
)

select * from final
```

#### Output schema — fct_currency_deposits

| Column | Type | Example |
|---|---|---|
| `deal_id` | VARCHAR (PK) | FIN-202304-000001 |
| `deal_type` | VARCHAR (FK) | FIXBIS_DEP |
| `cpty_code` | VARCHAR (FK) | PBOC_01 |
| `currency` | VARCHAR (FK) | USD |
| `trade_date_key` | INT (FK→dim_date) | 20230403 |
| `value_date_key` | INT (FK→dim_date) | 20230405 |
| `maturity_date_key` | INT (FK→dim_date) | 20240405 |
| `nominal_amount` | DECIMAL(20,2) | 500000000.00 |
| `nominal_amount_sdr` | FLOAT | 376070000.00 |
| `rate_pct` | DECIMAL(10,6) | 2.500000 |
| `rate_decimal` | DECIMAL(10,6) | 0.025000 |
| `deal_status` | VARCHAR | VALIDATED |
| `portfolio` | VARCHAR | TRSRY-APAC |
| `fx_rate_date` | DATE | 2023-03-31 |
| `sdr_per_unit` | FLOAT | 0.752140 |

---

### Layer 9 — mart_maturity_ladder

**File:** `models/mart/mart_maturity_ladder.sql` · **Materialisation:** view

**Purpose:** Standalone analytics mart — *not* part of the star schema. Answers: *"For any given month-end date, how much of the active deposit book matures within each time bucket?"* This is the **maturity ladder** — a standard liquidity risk management report.

**Why CROSS JOIN?** We want the maturity profile at 24 different points in time (every month-end Apr 2023 → Mar 2025). Instead of 24 separate queries, we cross-join all active deals × all 24 as-of dates. Each (deal, date) pair gets a `days_to_maturity` calculated from that specific date to the deal's maturity. Result: 71 rows (24 dates × up to 7 non-empty buckets).

**Hybrid approach:** Bucket logic (time windows) lives in dbt SQL. Date selection lives in Power BI as an `as_of_date` dropdown slicer. SQL does aggregation; Power BI does interactivity.

**Why `SIGHT_DEP` is always Overnight:** Sight accounts have no maturity date (`maturity_date` is NULL → `days_to_maturity` would be NULL). They are redeemable at any time — functionally equivalent to overnight. Explicitly assigned to bucket `'1. Overnight'` as the first CASE branch.

```sql
with

-- Step 1: generate 24 integers (0 to 23)
month_nums as (
    select 0  as n union all select 1  union all select 2  union all select 3
    union all select 4  union all select 5  union all select 6  union all select 7
    union all select 8  union all select 9  union all select 10 union all select 11
    union all select 12 union all select 13 union all select 14 union all select 15
    union all select 16 union all select 17 union all select 18 union all select 19
    union all select 20 union all select 21 union all select 22 union all select 23
),

-- Step 2: convert each integer into a month-end date
-- EOMONTH('2023-04-01' + n months) = Apr 2023, May 2023, ..., Mar 2025
as_of_dates as (
    select eomonth(dateadd(month, n, cast('2023-04-01' as date))) as as_of_date
    from month_nums
),

-- Step 3: VALIDATED deals only; reconstruct maturity_date from YYYYMMDD key
active_deals as (
    select
        f.deal_id,
        f.deal_type,
        f.nominal_amount_sdr,
        f.maturity_date_key,
        case
            when f.maturity_date_key is null then null
            else datefromparts(
                f.maturity_date_key / 10000,           -- year
                (f.maturity_date_key / 100) % 100,     -- month
                f.maturity_date_key % 100              -- day
            )
        end as maturity_date
    from {{ ref('fct_currency_deposits') }} f
    where f.deal_status = 'VALIDATED'
),

-- Step 4: CROSS JOIN — every deal × every as-of date
crossed as (
    select
        d.deal_id,
        d.deal_type,
        d.nominal_amount_sdr,
        d.maturity_date,
        a.as_of_date,
        datediff(day, a.as_of_date, d.maturity_date) as days_to_maturity
    from active_deals d
    cross join as_of_dates a
),

-- Step 5: assign to maturity bucket
with_buckets as (
    select
        as_of_date,
        case
            when deal_type = 'SIGHT_DEP'   then '1. Overnight'
            when days_to_maturity <=   7   then '2. <= 1 Week'
            when days_to_maturity <=  30   then '3. <= 1 Month'
            when days_to_maturity <=  90   then '4. <= 3 Months'
            when days_to_maturity <= 180   then '5. <= 6 Months'
            when days_to_maturity <= 365   then '6. <= 1 Year'
            else                                '7. > 1 Year'
        end as maturity_bucket,
        nominal_amount_sdr
    from crossed
    where days_to_maturity > 0       -- exclude matured deals
       or deal_type = 'SIGHT_DEP'    -- sight accounts always included
),

-- Step 6: aggregate
final as (
    select
        as_of_date,
        maturity_bucket,
        count(*)                as deal_count,
        sum(nominal_amount_sdr) as total_sdr
    from with_buckets
    group by as_of_date, maturity_bucket
)

select * from final
```

#### Output schema — mart_maturity_ladder

| Column | Type | Example |
|---|---|---|
| `as_of_date` | DATE | 2025-03-31 |
| `maturity_bucket` | VARCHAR | 2. <= 1 Week |
| `deal_count` | INT | 12 |
| `total_sdr` | FLOAT | 4820000000.00 |

**Result:** 71 rows (24 dates × up to 7 buckets). In Power BI: add `as_of_date` as a dropdown slicer. Select a month-end to see the maturity distribution for that date.

---

## 8. Business Metrics Rationale

### Why these specific marts?

| Mart / measure | Business question answered | Annual report link |
|---|---|---|
| Total SDR by region | Which central banks are our largest depositors? Where is concentration risk? | p.106 Graph 4.3 |
| Deposits by fiscal year / product | Are deposits growing? Which products are central banks preferring? | p.105 Graph 4.2 |
| Maturity ladder by as-of date | What are our cash outflows in the next week / month / year? | p.103-104 |
| deal_count | How many individual deals make up the book? Is the portfolio concentrated? | Internal KRI |
| rate_pct / rate_decimal | What interest are we paying? Cost of funds by currency? | Banking income statement |

### Why SDR and not USD?

The BIS serves central banks globally. Converting to a single national currency (e.g. USD) would introduce FX volatility into reporting — a drop in deposit volumes could be the USD strengthening, not actual outflows. The SDR basket dampens this volatility and is the official BIS unit of account.

### Why star schema and not flat tables?

Power BI's VertiPaq engine is optimised for star schemas. Relationships between one fact table and multiple dimension tables allow efficient filter propagation — a region filter on `dim_counterparty` propagates to `fct_currency_deposits` through the relationship, without requiring a SQL WHERE clause. This enables sub-second response on millions of rows.

### Why views and not tables?

On Fabric SQL Analytics Endpoint, physical tables cannot be created (DDL restriction). Views are also more flexible during development — change the SQL, re-run `dbt run`, no DROP/CREATE needed. Trade-off: no pre-computed data; every query re-reads from underlying Delta files. Acceptable for ~10,000 deals; switch to Fabric Warehouse for tens of millions of rows.

---

## 9. End-to-End Recipe

Step-by-step to rebuild the entire pipeline from scratch.

### Step 1 — Install prerequisites

```bash
pip install dbt-fabric --break-system-packages
```

Install Python 3.11+, Azure CLI, VS Code, Power BI Desktop.

### Step 2 — Set up Microsoft Fabric

In app.powerbi.com: create a workspace → create two Lakehouses: `bis_raw` and `bis_analytics`. Copy the SQL Connection String from each Lakehouse's SQL Analytics Endpoint settings.

### Step 3 — Generate synthetic source data

Run the Python data generation scripts in `BIS_Project_2026/data_generation/`. Output: `src_deals.csv` (~500 rows) and `src_fx_rates.csv` (~240 rows).

### Step 4 — Load raw data into Fabric OneLake

Open the PySpark notebook `01_load_raw.ipynb` in Fabric. Run all cells — reads CSVs from OneLake Files, writes as Delta tables in `bis_raw.dbo`. Verify: bis_raw Lakehouse → Tables → `src_deals` and `src_fx_rates` visible.

### Step 5 — Configure dbt profile

Edit `~/.dbt/profiles.yml` (Windows: `C:\Users\sosa_\.dbt\profiles.yml`). Add the bis_analytics profile pointing at your Fabric SQL Analytics Endpoint. Authentication method: `az_cli`.

### Step 6 — Authenticate Azure CLI

```bash
az login --tenant 04f991f3-0023-4b95-ab0d-bebbdbc8df14 --allow-no-subscriptions
# Press 1 — repeat every ~1 hour
```

### Step 7 — Run dbt pipeline

```bash
cd C:\Users\sosa_\Downloads\BIS_Project_2026\bis_analytics

dbt debug                  # verify connection
dbt run                    # run all 9 models
dbt run --select mart      # run only mart layer
```

Expected: `PASS=9` (all models) or `PASS=7` (mart only). All models materialise as views in `bis_analytics.dbo`.

### Step 8 — Connect Power BI semantic model to Fabric

In Power BI Desktop: Get Data → SQL Server → enter Fabric SQL Analytics Endpoint connection string → import all mart tables. Or open the existing `BIS_Analytics.pbix` and refresh. Verify all 9 tables loaded.

### Step 9 — Set up relationships in semantic model

In Power BI → Model view: verify relationships between `fct_currency_deposits` and each dim table. Active relationship for `value_date_key → dim_date.date_key`. Inactive relationships for trade and maturity date keys (activated via DAX `USERELATIONSHIP()` when needed).

### Step 10 — Build / refresh Power BI report

Verify Page 1 (deposits by region, by FY) and Page 2 (deposits by counterparty, maturity ladder). Slicers: fiscal year, currency, as_of_date. Publish to workspace when ready.

---

## 10. Lexique / Glossary

**SDR** — Special Drawing Right. IMF basket currency (USD, EUR, GBP, JPY, CNY). BIS unit of account. Not a real currency — used for accounting and reporting.

**BIS** — Bank for International Settlements. Founded 1930, Basel. "Central bank of central banks." ~200 central bank customers. Total assets: SDR 431.3bn (Mar 2025).

**dbt (data build tool)** — Open-source SQL transformation framework. Write SELECT queries; dbt handles CREATE/DROP, dependency resolution (DAG), and documentation. Uses Jinja-SQL templating.

**DAG** — Directed Acyclic Graph. In dbt, the dependency graph of all models. dbt resolves execution order from `ref()` calls. No circular dependencies allowed.

**CTE** — Common Table Expression. Named temporary result set defined with the `WITH` keyword. Makes SQL readable by breaking a long query into named steps. Not persisted to disk.

**Star schema** — Data modelling pattern: one central fact table + multiple dimension tables joined via foreign keys. Optimised for analytical queries and Power BI VertiPaq engine.

**Fact table** — Centre of a star schema. Contains measurable, additive business events (deals, transactions). Each row = one event. Foreign keys point to dimension tables.

**Dimension table** — Descriptive context for the fact table. Contains attributes used for filtering, grouping, and labelling (e.g. counterparty name, region, currency name).

**Degenerate dimension** — A dimension attribute kept on the fact table rather than in a separate dim table. Used when a column has too many values for useful grouping (e.g. deal_id, portfolio, book).

**Maturity ladder** — Liquidity risk report showing how much of a portfolio matures in each time bucket (Overnight, ≤1W, ≤1M, etc.) at a given point in time. Used by treasury/risk teams to manage cash outflows.

**FIXBIS** — Fixed-rate Investment at the BIS. Fixed-term, fixed-rate currency deposit. Most common product in the BIS deposit book.

**MTI** — Medium-Term Instrument. Longer tenor (typically 1–10 years) fixed-rate deposit.

**Sight account** — On-demand / overnight deposit. No fixed maturity. Central bank can withdraw at any time. Always classified as Overnight in the maturity ladder.

**Notice account** — Deposit redeemable after a fixed advance notice period (e.g. 7, 30, 90 days). Between Sight and FIXBIS in terms of liquidity.

**MS Fabric** — Microsoft Fabric. Unified analytics platform on Azure. Components: OneLake (storage), Lakehouse (Delta/Parquet tables), SQL Analytics Endpoint (T-SQL queries), Power BI (reports).

**OneLake** — Microsoft Fabric's unified storage layer. All data (Delta/Parquet files) lives in OneLake. Multiple compute engines (SQL, Spark, Power BI) read the same files.

**SQL Analytics Endpoint** — A T-SQL query interface over a Fabric Lakehouse. Read-only for DDL (cannot `CREATE TABLE`). dbt uses this to materialise views and run SELECT queries.

**Delta / Parquet** — Delta Lake is a storage layer using Parquet files + a transaction log. Parquet is a columnar file format. Much faster than CSV for analytical scans. Native format of MS Fabric Lakehouses.

**Direct Lake** — Power BI mode that reads directly from Delta files in OneLake — no import/refresh needed. Requires physical Delta tables. Not available for SQL views (use Import mode instead).

**Import mode** — Power BI loads data from the source into its in-memory VertiPaq engine. Fast for queries; requires periodic refresh. Used in this project (views → SQL Server connector).

**VertiPaq** — Power BI's in-memory columnar database engine. Stores imported data. Optimised for star schema queries and DAX measures.

**DAX** — Data Analysis Expressions. Power BI's formula language for calculated measures, columns, and table functions. Analogous to Excel formulas but for analytical data models.

**Findur** — Openlink Findur. Enterprise treasury and trading system. Used by banks and central banks for deal booking, position management, and risk reporting. The BIS source system.

**BIS fiscal year** — April 1 to March 31. Labelled FY2023-24, FY2024-25, etc. Different from calendar year. All BIS annual report figures use this convention.

**CROSS JOIN** — SQL operation producing the Cartesian product of two tables. Every row in A × every row in B. Used in `mart_maturity_ladder`: 500 deals × 24 dates = 12,000 rows.

**TRY_CAST** — T-SQL function. Converts a value to a target data type. Returns NULL on failure instead of throwing an error like CAST. Essential for raw string data from source systems.

**EOMONTH(date, n)** — T-SQL function returning the last day of the month, optionally offset by n months. `EOMONTH('2023-04-15', -1)` = `2023-03-31`. Used for FX rate date matching and as-of date generation.

**date_key** — Integer representation of a date: `YEAR × 10000 + MONTH × 100 + DAY`. Example: `2023-04-03` → `20230403`. Avoids `FORMAT()` (unavailable on Fabric endpoint). Sortable as integer.

**ref() / source()** — dbt Jinja macros. `ref('model_name')` declares a dependency on another dbt model. `source('schema', 'table')` declares a dependency on an external source table. These build the DAG.

**ISO 3166-1 alpha-2** — Two-letter country codes: CN (China), JP (Japan), US (United States), etc. Used in `cpty_country` and the region CASE statement in `dim_counterparty`.

**ISO 4217** — Three-letter currency codes: USD, EUR, GBP, JPY, CNY, CHF, etc. Used in the `currency` column of `src_deals` and `src_fx_rates`.

**as-of date** — The reference date for a snapshot calculation. In the maturity ladder, it is the date from which `days_to_maturity` is calculated.

**VALIDATED / MATURED / CANCELLED** — BIS deal lifecycle states. VALIDATED = active deal. MATURED = reached maturity and was repaid. CANCELLED = cancelled before maturity. Analytics marts filter to VALIDATED only.

---

*BIS Analytics Pipeline · Self-Study Guide · Built April 2026 · For interview preparation — Senior Business Data Analyst, BIS Banking Department*
