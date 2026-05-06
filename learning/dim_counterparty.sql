/* ============================================================================
   dbt run -- DAG execution order for bis_analytics
   ============================================================================

   TIER 1 (no upstream -- run first, could be run in parallel)
     models/staging/stg_deals.sql           depends on source('bis_raw','src_deals')        DONE
     models/staging/stg_fx_rates.sql        depends on source('bis_raw','src_fx_rates')     DONE
     models/mart/dim_currency.sql           VALUES literal, no ref/source                   DONE
     models/mart/dim_product.sql            VALUES literal, no ref/source                   DONE
     models/mart/dim_date.sql               date spine, no ref/source                       DONE

   TIER 2
     models/intermediate/int_deals_with_sdr.sql   ref('stg_deals') + ref('stg_fx_rates')    DONE

   TIER 3 (could be run in parallel)
     models/mart/dim_counterparty.sql       ref('int_deals_with_sdr')                       <--- We are here
     models/mart/fct_currency_deposits.sql  ref('int_deals_with_sdr')                       Next

   TIER 4
     models/mart/mart_maturity_ladder.sql   ref('fct_currency_deposits')                    Next

   ----------------------------------------------------------------------------
   Total: 9 model files = 9 views in bis_analytics.dbo (PASS=9)
   ============================================================================ */

/*
  dim_counterparty
  ================

  What this is:
    A small reference list of the counterparties (the central banks and
    international institutions) that have actually placed deposits with the
    BIS during our data period. One row per counterparty. No deals here,
    no money amounts, no dates -- just names, country codes, and a region
    label.

  Where its data comes from:
    Built from 'int_deals_with_sdr', which holds one row per individual
    deal. We use 'select distinct' on three columns ("cpty_code",
    "cpty_name", "cpty_country") to collapse "many deals per counterparty"
    down to "one row per counterparty".

  How big it is:
    97 rows.

    For context: the real BIS serves roughly 200 central banks (Annual
    Report p.104). Our dataset is a portfolio simulation that we kept
    small on purpose: only 700 deals across the two BIS fiscal years
    FY2023-24 and FY2024-25. Fewer deals means fewer distinct
    counterparties show up. That is intentional, not a bug.

    What WAS calibrated to match the real BIS is the total amount of
    money on deposit. Average active book in our data is SDR 296.9bn
    versus SDR 314bn reported in the Annual Report, a 6% gap.

  What the column "region" means:
    For each counterparty, we look at its country code ("cpty_country",
    a 2-letter ISO code like 'CN' for China) and translate it into one
    of six regions used by the BIS in its own reporting. The mapping
    follows Annual Report p.106, Graph 4.3:

      Asia-Pacific      61%   of deposit value
      International      9%   (country codes 'XX' and 'EU': supranational
                               bodies like the IMF and the ECB)
      Americas           9%
      Europe             8%
      Middle East        7%
      Africa             6%

    Important: those percentages are shares of DEPOSIT VALUE, not shares
    of counterparty COUNT. Asia-Pacific holds 61% of the money but is
    only about 39% of the 97 counterparties. A small number of very
    large central banks (People's Bank of China, Bank of Japan, Reserve
    Bank of India) account for most of the value.

  How it is stored:
    As a view in Fabric. A view does not store any data of its own. It
    is a saved SELECT statement. Every time something queries the view,
    the SQL inside it is re-executed against 'int_deals_with_sdr', and
    the 97 rows are recomputed on the spot.
*/

/*
  Jinja config block. Tells dbt to store this model as a view in Fabric.
  Optional. Overrides the materialisation default set in 'dbt_project.yml'.
*/
{{
    config(
        materialized = 'view'
    )
}}
/*
  Input table-view: 'int_deals_with_sdr'
  ======================================
  Located at        : 'bis_raw.dbo.int_deals_with_sdr'
  How it is stored  : as a view in Fabric (Tier 2 in the dbt DAG)
  Grain             : one row per deal
  Rows              : 700 (covers BIS FY2023-24 + FY2024-25)
  Columns           : 20 total. This model consumes 3 of them.

  Columns consumed by 'dim_counterparty':
    column            type *      role                example
    ----------------  ----------  ------------------  ----------------------------
    "cpty_code"       VARCHAR *   counterparty PK     'PBOC_01'
    "cpty_name"       VARCHAR *   full label          "People's Bank of China"
    "cpty_country"    VARCHAR *   ISO alpha-2 code    'CN'

  Columns ignored by this model (17 columns, listed for completeness):
    "deal_id", "deal_type", "trade_date", "value_date", "maturity_date",
    "currency", "nominal_amount", "rate_pct", "rate_decimal", "deal_status",
    "portfolio", "book", "fx_rate_date", "sdr_per_unit",
    "nominal_amount_sdr", "created_dt", "last_modified_dt".

  * Types not yet verified empirically against the live view. Per the locked
    rule, run this to lock them once and update the comment:
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'int_deals_with_sdr'
          AND COLUMN_NAME IN ('cpty_code','cpty_name','cpty_country')
        ORDER BY ORDINAL_POSITION;

  Grain transformation that happens inside this model:
    Input grain   : one row per deal                                  (700 rows)
    CTE 1 grain   : one row per distinct
                    ("cpty_code", "cpty_name", "cpty_country") triple  (97 rows)
    Output grain  : same as CTE 1                                      (97 rows)

  How the collapse happens: 'select distinct' on the three columns in CTE 1.
  A counterparty with N deals upstream collapses to 1 row downstream.
*/

/*
  CTE 1: 'counterparties'
  =======================
  First step. Pulls the distinct counterparty triples from 'int_deals_with_sdr'.

  Upstream grain  : one row per deal ("deal_id" is unique).
  This CTE grain  : one row per unique combination of the three columns
                    "cpty_code", "cpty_name", "cpty_country".

  A given counterparty (e.g. 'PBOC_01') appears on dozens of deals upstream.
  'select distinct' collapses those to a single row.
*/
with counterparties as (

    select distinct                          -- keep one row per unique triple below
        cpty_code,                           -- column "cpty_code": short code, future PK of 'dim_counterparty'
        cpty_name,                           -- column "cpty_name": full name, label for Power BI
        cpty_country                         -- column "cpty_country": ISO alpha-2 code, feeds the region CASE in CTE 2

    from {{ ref('int_deals_with_sdr') }}     -- ref() macro: declares dbt dependency on the intermediate model.
                                             -- Compiles to 'bis_analytics.dbo.int_deals_with_sdr' at run time.
),

/*
  CTE 2: 'with_region'
  ====================
  Decorates each counterparty with a "region" label derived from the column
  "cpty_country". The three columns from CTE 1 ('counterparties') pass
  through unchanged; one new column ("region") is added in the next chunk
  via CASE.

  Grain stays: one row per counterparty.
*/
with_region as (

    select
        cpty_code,         -- column "cpty_code": pass-through from CTE 'counterparties'
        cpty_name,         -- column "cpty_name": pass-through from CTE 'counterparties'
        cpty_country,      -- column "cpty_country": pass-through, AND input to the CASE below


        /*
        Build the column "region"
        =========================
        Map each ISO alpha-2 country code in the column "cpty_country" to one of
        six BIS regions, plus 'International' for supranationals and 'Other' as
        a catch-all.

        Form used: simple CASE.
        case <input expression>
            when <value 1> then <result 1>
            when <value 2> then <result 2>
            ...
            else <default>
        end as <output column>

        Reads like: "look at cpty_country; if it equals 'CN', return 'Asia-Pacific';
        if 'JP', return 'Asia-Pacific'; ... if no branch matched, return 'Other'."

        WHEN order does not matter for correctness (every country code maps to
        exactly one region). Order matters for readability only, hence grouped by
        region.

        Coverage in this file:
                            code count   share of deposits
        Asia-Pacific            38           61%
        Europe                  23            8%
        Africa                  21            6%
        Americas                17            9%
        Middle East             11            7%
        International            2            9%   (codes 'XX' and 'EU')
                           ----
        total mapped           112
        + 'Other' catch-all

        Source: BIS Annual Report p.106, Graph 4.3 (region shares).
        */
        case cpty_country -- Simple CASE form: read the column "cpty_country", match its value against the WHEN list below.
            -- Asia-Pacific  (38 codes; 61% of deposits)
            when 'CN' then 'Asia-Pacific' -- if "cpty_country" = 'CN' (ISO alpha-2 code for China),
                                          -- return 'Asia-Pacific' as the value of the new column "region".
            when 'JP' then 'Asia-Pacific'
            when 'IN' then 'Asia-Pacific'
            when 'KR' then 'Asia-Pacific'
            when 'AU' then 'Asia-Pacific'
            when 'NZ' then 'Asia-Pacific'
            when 'SG' then 'Asia-Pacific'
            when 'HK' then 'Asia-Pacific'
            when 'TH' then 'Asia-Pacific'
            when 'MY' then 'Asia-Pacific'
            when 'ID' then 'Asia-Pacific'
            when 'PH' then 'Asia-Pacific'
            when 'BD' then 'Asia-Pacific'
            when 'PK' then 'Asia-Pacific'
            when 'LK' then 'Asia-Pacific'
            when 'MM' then 'Asia-Pacific'
            when 'KH' then 'Asia-Pacific'
            when 'MN' then 'Asia-Pacific'
            when 'PG' then 'Asia-Pacific'
            when 'FJ' then 'Asia-Pacific'
			      when 'TM' then 'Asia-Pacific'
			      when 'MO' then 'Asia-Pacific'
			      when 'TJ' then 'Asia-Pacific'
			      when 'BT' then 'Asia-Pacific'
			      when 'KZ' then 'Asia-Pacific'
			      when 'KG' then 'Asia-Pacific'
			      when 'CK' then 'Asia-Pacific'
			      when 'MV' then 'Asia-Pacific'
			      when 'WS' then 'Asia-Pacific'
			      when 'UZ' then 'Asia-Pacific'
			      when 'TL' then 'Asia-Pacific'
			      when 'SB' then 'Asia-Pacific'
			      when 'NP' then 'Asia-Pacific'
			      when 'VU' then 'Asia-Pacific'
			      when 'VN' then 'Asia-Pacific'
			      when 'LA' then 'Asia-Pacific'
			      when 'BN' then 'Asia-Pacific'
			      when 'TO' then 'Asia-Pacific'
            -- Americas  (17 codes; 9%)
            when 'US' then 'Americas'
            when 'CA' then 'Americas'
            when 'BR' then 'Americas'
            when 'MX' then 'Americas'
            when 'AR' then 'Americas'
            when 'CL' then 'Americas'
            when 'CO' then 'Americas'
            when 'PE' then 'Americas'
            when 'UY' then 'Americas'
            when 'BO' then 'Americas'
            when 'EC' then 'Americas'
            when 'PY' then 'Americas'
            when 'VE' then 'Americas'
            when 'TT' then 'Americas'
            when 'JM' then 'Americas'
			      when 'CR' then 'Americas'
			      when 'HN' then 'Americas'
            -- Europe  (23 codes; 8%)
            when 'DE' then 'Europe'
            when 'FR' then 'Europe'
            when 'GB' then 'Europe'
            when 'IT' then 'Europe'
            when 'ES' then 'Europe'
            when 'NL' then 'Europe'
            when 'BE' then 'Europe'
            when 'SE' then 'Europe'
            when 'NO' then 'Europe'
            when 'DK' then 'Europe'
            when 'CH' then 'Europe'
            when 'PL' then 'Europe'
            when 'CZ' then 'Europe'
            when 'HU' then 'Europe'
            when 'RO' then 'Europe'
            when 'BG' then 'Europe'
            when 'HR' then 'Europe'
            when 'RS' then 'Europe'
            when 'TR' then 'Europe'
            when 'IS' then 'Europe'
			      when 'AZ' then 'Europe'
			      when 'AM' then 'Europe'
			      when 'GE' then 'Europe'
            -- Middle East  (11 codes; 7%)
            when 'SA' then 'Middle East'
            when 'AE' then 'Middle East'
            when 'KW' then 'Middle East'
            when 'QA' then 'Middle East'
            when 'BH' then 'Middle East'
            when 'OM' then 'Middle East'
            when 'JO' then 'Middle East'
            when 'LB' then 'Middle East'
            when 'IQ' then 'Middle East'
            when 'IR' then 'Middle East'
            when 'IL' then 'Middle East'
            -- Africa  (21 codes; 6%)
            when 'ZA' then 'Africa'
            when 'NG' then 'Africa'
            when 'EG' then 'Africa'
            when 'KE' then 'Africa'
            when 'GH' then 'Africa'
            when 'TZ' then 'Africa'
            when 'UG' then 'Africa'
            when 'ET' then 'Africa'
            when 'MA' then 'Africa'
            when 'TN' then 'Africa'
            when 'DZ' then 'Africa'
            when 'MU' then 'Africa'
            when 'RW' then 'Africa'
            when 'BW' then 'Africa'
            when 'NA' then 'Africa'
            when 'MZ' then 'Africa'
            when 'ZM' then 'Africa'
            when 'ZW' then 'Africa'
			      when 'SN' then 'Africa'
			      when 'CD' then 'Africa'
			      when 'MW' then 'Africa'
            -- International organisations  (2 codes; 9%)
            when 'XX' then 'International'
			      when 'EU' then 'International'
else 'Other'                  -- catch-all branch. If "cpty_country" did not match any WHEN above, "region" gets 'Other'. 
                              -- Diagnostic target: zero rows on a clean run.
        end as region                     -- close the CASE expression and alias its result as the new column "region" in CTE 'with_region'.

    from counterparties                   -- CTE 2 reads from CTE 'counterparties' (97 rows in). 
                                          -- Each row passes through SELECT with 3 pass-through columns + 1 derived "region" = 4 columns out.

)                                         -- close CTE 'with_region'. No trailing comma here because this is the LAST CTE.

select * from with_region                 -- the model's actual output query. dbt wraps it as `CREATE VIEW bis_raw.dbo.dim_counterparty AS ( ... )` at run time. 
                                          -- Filename drives the view name; CTE name 'with_region' disappears.


/*
  Output table-view: 'bis_raw.dbo.dim_counterparty'
  =================================================
  Grain    : one row per counterparty.
  Rows     : 97 (verified May 6, 2026 via SELECT COUNT(*) on the live view).
             Synthetic dataset, downsized vs the BIS's ~200 real customers.
             Total SDR volume IS calibrated to BIS reality (avg active book
             SDR 296.9bn vs reported SDR 314bn -- within 6%).
  Columns  : 4

  Schema:
    column            type *      role             example
    ----------------  ----------  ---------------  ----------------------------
    "cpty_code"       VARCHAR *   PK               'PBOC_01'
    "cpty_name"       VARCHAR *   label            "People's Bank of China"
    "cpty_country"    VARCHAR *   ISO alpha-2 FK   'CN'
    "region"          VARCHAR *   derived label    'Asia-Pacific'

  * VARCHAR widths still pending empirical verification. Run the
    INFORMATION_SCHEMA query below to lock them.

  Possible values in column "region" (8 categories):
    'Asia-Pacific', 'Europe', 'Africa', 'Americas', 'Middle East',
    'International', 'Other'.
    'Other' should have zero rows on a clean run. Diagnostic:
        SELECT region, COUNT(*) FROM bis_raw.dbo.dim_counterparty
        GROUP BY region ORDER BY 2 DESC;

  Storage: zero bytes on disk. The view re-runs the query on every read.
*/