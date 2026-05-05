/*
  int_deals_with_sdr 
  (the SQL table-VIEW to be created as output)
  ==================
  Middle step. Takes the cleaned deals (from table-view 'stg_deals') and
  the cleaned FX rates (from table-view 'stg_fx_rates'), then computes
  one new column: "nominal_amount_sdr" (the deal value in SDR, the
  currency BIS reports everything in). Two FX columns also ride along
  on each row so we can always see which FX rate was applied.

  Columns:
    Input 1 ('stg_deals'):     17 columns
    Input 2 ('stg_fx_rates'):  3 columns
    Total input pool:        20 columns

    Old columns kept (pass-through):  19
                                      = 17 columns from table-view 'stg_deals' (all of them)
                                      + 2 columns from table-view 'stg_fx_rates':
                                          column "rate_date"    (renamed "fx_rate_date")
                                          column "sdr_per_unit" (same name)

    Old columns dropped:              1
                                      = column "currency_code" from table-view 'stg_fx_rates'.
                                        Used inside the join condition
                                        but not carried forward, since
                                        the deal's own "currency" column
                                        already carries that info.

    New columns derived:              1
                                      = column "nominal_amount_sdr"
                                        ( = "nominal_amount" * "sdr_per_unit" )

    Output total:                     20 columns
                                      = 19 pass-through columns + 1 derived column

  How the match works:
    Match by currency: a deal's "currency" column must equal the rate's
                       "currency_code" column (USD with USD, EUR with EUR).
    Match by date:     for each deal, pick the FX rate from the last
                       day of the month BEFORE its "value_date" column.
                       Example: a deal whose "value_date" column equals
                       2023-04-15 uses the rate from 2023-03-31.
                       T-SQL: EOMONTH(DATEADD(month, -1, value_date)).

  Why LEFT JOIN: even if a deal has no matching FX rate, we still
  want the deal to appear in the output. Its "nominal_amount_sdr"
  column will come out as NULL, which makes the problem visible rather
  than silently dropping the row.

  How it is stored: as a view in Fabric. A view stores no data of
  its own; it just runs this query each time something asks for it.

  Grain: one row per deal. Each deal appears exactly once because
  column "deal_id" is unique.
*/

/*
  CTE 1 of 3: "deals"
  -------------------
  Read every column from table-view 'stg_deals' (the staged deals
  table-view) and call this whole result "deals" so the next CTEs
  in this file can refer to it by that short name.
  
    About the dbt macro {{ ref('stg_deals') }}:
  - At compile time dbt rewrites it into the full three-part name
    'bis_analytics.dbo.stg_deals'.
  - We use ref() here (rather than source() like in the staging
    files) because we are pointing at another dbt model, not at a
    raw source table-view. Every ref() call is also how dbt knows
    this model depends on 'stg_deals', so it runs 'stg_deals' first.*/
with CTE_deals as (
    select * from {{ ref('stg_deals') }}
),

/*
  CTE 2 of 3: "fx_rates"
  ----------------------
  Read every column from table-view 'stg_fx_rates' (the staged FX
  rates table-view) and call this whole result "fx_rates" so the
  next CTE can refer to it by that short name.

  Same mechanics as CTE 1 ('deals'):
  - {{ ref('stg_fx_rates') }} compiles to 'bis_analytics.dbo.stg_fx_rates'
    at run time.
  - ref() (rather than source()) tells dbt this points at another
    dbt model, so dbt will run 'stg_fx_rates' first.

  Reminder on grain: 'stg_fx_rates' has 3 columns and a composite
  grain of one row per ("rate_date", "currency_code") pair. The same
  currency repeats every month and every month has many currencies,
  so it takes BOTH columns together to point at a single row. That
  composite grain is why the join in CTE 3 needs to match on BOTH
  currency AND date.*/
CTE_fx_rates as (
    select * from {{ ref('stg_fx_rates') }}
            ),

/*
  CTE 3 of 3: opening line "joined as ("
  --------------------------------------
  This starts the third (and last) CTE in this file. The label
  "joined" is the name we give to whatever this CTE produces, so
  the final SELECT at the bottom of the file can refer to it by
  that short name.

  Same syntax pattern as CTE 1 ("deals as (") and CTE 2 ("fx_rates
  as ("):
       <label> as (
           ... query body goes here ...
       )

  The label "joined" is descriptive: this is the CTE where the
  'deals' CTE and the 'fx_rates' CTE finally come together via a
  join. The body inside the parentheses is what we will read next.
*/
CTE_joined as (
    select select  -- opens the query body; the column list follows, ending at the FROM clause

        -- identifiers column
        alias_CTE_deals.deal_id,  -- column "deal_id" from 'deals'; unique deal identifier (one row per deal)

        -- product or instrument column
        alias_CTE_deals.deal_type,  -- column "deal_type" from 'deals'; BIS deposit product code (FIXBIS_DEP, MTI_DEP, SIGHT_DEP, NOTICE_DEP)

        -- counterparty columns
        alias_CTE_deals.cpty_code,     -- column "cpty_code" from 'deals'; counterparty short code (e.g. PBOC_01)
        alias_CTE_deals.cpty_name,     -- column "cpty_name" from 'deals'; full counterparty name (e.g. People's Bank of China)
        alias_CTE_deals.cpty_country,  -- column "cpty_country" from 'deals'; ISO 3166-1 alpha-2 country code (e.g. CN, JP, US)

        -- dates' columns
        alias_CTE_deals.trade_date,     -- column "trade_date" from 'deals'; date the deal was struck
        alias_CTE_deals.value_date,     -- column "value_date" from 'deals'; settlement date; used in the JOIN to pick the FX rate
        alias_CTE_deals.maturity_date,  -- column "maturity_date" from 'deals'; date the deal matures (NULL for sight accounts)

        -- currency and amounts (original currency) columns
        alias_CTE_deals.currency,        -- column "currency" from 'deals'; ISO 4217 currency code; used in the JOIN to pick the FX rate
        alias_CTE_deals.nominal_amount,  -- column "nominal_amount" from 'deals'; deal principal in original currency (whole-unit integer)

        -- interest rate columns
        alias_CTE_deals.rate_pct,      -- column "rate_pct" from 'deals'; interest rate in percentage form (2.50 = 2.50%)
        alias_CTE_deals.rate_decimal,  -- column "rate_decimal" from 'deals'; decimal form (= "rate_pct" / 100, e.g. 0.025 = 2.50%)

        -- deal lifecycle column
        alias_CTE_deals.deal_status,  -- column "deal_status" from 'deals'; VALIDATED / MATURED / CANCELLED

        -- internal classification columns
        alias_CTE_deals.portfolio,  -- column "portfolio" from 'deals'; internal BIS portfolio code (e.g. TRSRY-APAC)
        alias_CTE_deals.book,       -- column "book" from 'deals'; trading book identifier (e.g. BK-001)

-- fx rate applied columns (kept for auditability)
        alias_CTE_fx_rates.rate_date                               as fx_rate_date,/*column "rate_date" from 'fx_rates', renamed to "fx_rate_date"; 
                                                                    FX-rate snapshot date (audit: which rate matched)*/
        alias_CTE_fx_rates.sdr_per_unit,  -- column "sdr_per_unit" from 'fx_rates'; FX rate value, SDR per 1 unit of deal currency (audit: the rate value)

-- SDR conversion column: "nominal_amount" * "sdr_per_unit" (the headline derived column)
        alias_CTE_deals.nominal_amount * fx.sdr_per_unit         as nominal_amount_sdr, /*new column "nominal_amount_sdr"; 
                                                                            deal value in SDR (the whole point of this model)*/

        -- audit timestamps columns
        alias_CTE_deals.created_dt,        -- column "created_dt" from 'deals'; record creation timestamp (audit)
        alias_CTE_deals.last_modified_dt   /*column "last_modified_dt" from 'deals'; last-modified timestamp (audit); 
                             NO trailing comma - final column in the SELECT*/

from CTE_deals alias_CTE_deals  -- LEFT side of the join: use the 'deals' CTE here, aliased as "d"

    left join CTE_fx_rates alias_CTE_fx_rates  -- LEFT JOIN: keep every deal row even if no matching FX rate is found (unmatched fx columns become NULL); 'fx_rates' is the RIGHT side, aliased as "fx"
        on  alias_CTE_fx_rates.currency_code = alias_CTE_deals.currency  -- match condition 1 of 2: column "currency_code" of the rate must equal column "currency" of the deal (USD with USD, etc.)
        and alias_CTE_fx_rates.rate_date = eomonth(dateadd(month, -1, alias_CTE_deals.value_date))  -- match condition 2 of 2: column "rate_date" of the rate must equal the last day of the month BEFORE the deal's "value_date" column


)  -- closes the body of CTE 3 ('joined'); NO trailing comma here because 'joined' is the LAST CTE in the file

/*
  End of CTE 3, then the final SELECT that the file returns.
  ----------------------------------------------------------
  - The ")" closes the body of CTE 3 ("joined"). NO trailing comma
    here because "joined" is the LAST CTE in the file. CTEs 1 and 2
    had trailing commas to chain into the next CTE; CTE 3 does not.

  - "select * from joined" is the line that actually defines the
    model's output. Everything above (the WITH keyword, the three
    CTEs) was just setup. This is the working query. "*" means "all
    20 columns of the 'joined' CTE".

  When dbt runs this model, it wraps the whole file in a CREATE
  VIEW statement. The result is a view named 'int_deals_with_sdr'
  in the Lakehouse. Anyone who queries that view later (Power BI,
  downstream dbt models, ad-hoc SSMS query) sees those 20 columns
  coming back.
*/
select *    -- final query: returns all 20 columns of CTE 'CTE_joined';
from CTE_joined -- this is what dbt wraps in CREATE VIEW and materializes as the view 'int_deals_with_sdr'


/*
  How the view name 'int_deals_with_sdr' gets created (dbt convention)
  -------------------------------------------------------------------
  dbt's default chain:

      int_deals_with_sdr.sql        <-- file in models/ folder
              |
              |  dbt drops the ".sql" extension to derive model name
              v
      int_deals_with_sdr            <-- model name
              |
              |  by default, view name = model name
              |  database + schema come from profiles.yml / dbt_project.yml
              v
      bis_analytics.dbo.int_deals_with_sdr   <-- final view in the Lakehouse

  Two caveats specific to THIS file:

  1. The file you are reading is at
        learning\int_deals_with_sdr_learning.sql
     not at
        bis_analytics\models\intermediate\int_deals_with_sdr.sql
     The filename includes "_learning" and the folder is outside
     models\. dbt does NOT process it. It is a sandbox copy for
     study only; no view gets materialized from this file.

  2. The actual view 'int_deals_with_sdr' in the Lakehouse is built
     from the production file at
        bis_analytics\models\intermediate\int_deals_with_sdr.sql
     Same content, but the filename and folder match dbt's processing
     rules.

  You can override the default with a config() block at the top of
  any model:
        {{ config(alias='something_else') }}
  That forces the view to be created as
        bis_analytics.dbo.something_else
  regardless of filename. But the default (filename drives view name)
  is what almost every dbt project relies on, and what this project
  does for 'int_deals_with_sdr'.
*/

/*
  Recap - int_deals_with_sdr_learning.sql
  =======================================

  Summary:
  --------
  Middle (intermediate) step in the pipeline. Joins the staged deals
  with the staged FX rates to produce the deal value in SDR (the
  basket-currency BIS uses to report all its figures). Same row count
  as the input deals (LEFT JOIN does not inflate). Materialised as a
  view in Fabric.

  File at a glance:
  -----------------
    Input 1            : view 'stg_deals' (17 columns)
    Input 2            : view 'stg_fx_rates' (3 columns)
    Output             : view 'int_deals_with_sdr' (20 columns)
    Materialisation    : view (no physical data, just a saved query)
    Grain              : one row per deal (column "deal_id" is unique)
    Number of CTEs     : 3 ('deals', 'fx_rates', 'joined')
    Join type          : LEFT JOIN on composite key (currency + date)

  Input 1 - view 'stg_deals' (17 columns):
  ----------------------------------------
    #   Column                  Data type        Notes
    --  ----------------------  ---------------  ------------------------------------------------
    1   "deal_id"               VARCHAR          unique key (file grain)
    2   "deal_type"             VARCHAR          FIXBIS_DEP / MTI_DEP / SIGHT_DEP / NOTICE_DEP
    3   "cpty_code"             VARCHAR          counterparty short code
    4   "cpty_name"             VARCHAR          full counterparty name
    5   "cpty_country"          VARCHAR          ISO 3166-1 alpha-2 code
    6   "trade_date"            DATE             date the deal was struck
    7   "value_date"            DATE             settlement date; used in JOIN
    8   "maturity_date"         DATE             NULL for sight accounts
    9   "currency"              VARCHAR          ISO 4217; used in JOIN
    10  "nominal_amount"        DECIMAL(20, 2)   deal principal in original currency
    11  "rate_pct"              DECIMAL(10, 6)   interest rate, percentage form (2.50 = 2.50%)
    12  "rate_decimal"          DECIMAL(10, 6)   derived in stg_deals (= "rate_pct" / 100)
    13  "deal_status"           VARCHAR          VALIDATED / MATURED / CANCELLED
    14  "portfolio"             VARCHAR          internal BIS portfolio code
    15  "book"                  VARCHAR          trading book identifier
    16  "created_dt"            DATETIME2        audit timestamp
    17  "last_modified_dt"      DATETIME2        audit timestamp

  Input 2 - view 'stg_fx_rates' (3 columns):
  ------------------------------------------
    #   Column                  Data type        Notes
    --  ----------------------  ---------------  ------------------------------------------------
    1   "rate_date"             DATE             always a month-end
    2   "currency_code"         VARCHAR          ISO 4217; used in JOIN, dropped from output
    3   "sdr_per_unit"          FLOAT            SDR per 1 unit of currency_code

  Output - view 'int_deals_with_sdr' (20 columns):
  ------------------------------------------------
    #   Column                  Data type        Origin         Change
    --  ----------------------  ---------------  -------------  ------------------------------------------
    1   "deal_id"               VARCHAR          stg_deals      pass-through
    2   "deal_type"             VARCHAR          stg_deals      pass-through
    3   "cpty_code"             VARCHAR          stg_deals      pass-through
    4   "cpty_name"             VARCHAR          stg_deals      pass-through
    5   "cpty_country"          VARCHAR          stg_deals      pass-through
    6   "trade_date"            DATE             stg_deals      pass-through
    7   "value_date"            DATE             stg_deals      pass-through
    8   "maturity_date"         DATE             stg_deals      pass-through
    9   "currency"              VARCHAR          stg_deals      pass-through
    10  "nominal_amount"        DECIMAL(20, 2)   stg_deals      pass-through
    11  "rate_pct"              DECIMAL(10, 6)   stg_deals      pass-through
    12  "rate_decimal"          DECIMAL(10, 6)   stg_deals      pass-through
    13  "deal_status"           VARCHAR          stg_deals      pass-through
    14  "portfolio"             VARCHAR          stg_deals      pass-through
    15  "book"                  VARCHAR          stg_deals      pass-through
    16  "fx_rate_date"          DATE             stg_fx_rates   renamed from "rate_date" (audit)
    17  "sdr_per_unit"          FLOAT            stg_fx_rates   pass-through (audit)
    18  "nominal_amount_sdr"    FLOAT            DERIVED        new; = "nominal_amount" * "sdr_per_unit"
    19  "created_dt"            DATETIME2        stg_deals      pass-through
    20  "last_modified_dt"      DATETIME2        stg_deals      pass-through

  Column accounting:
  ------------------
    Bucket                              Count  Detail
    ----------------------------------  -----  ---------------------------------------------------------
    Old columns kept (pass-through)     19     17 from 'stg_deals' + 2 from 'stg_fx_rates'
    Old columns dropped                 1      "currency_code" from 'stg_fx_rates' (used only in JOIN ON)
    New columns derived                 1      "nominal_amount_sdr"
    Output total                        20     19 pass-through + 1 derived

  Data type quirk to remember:
  ----------------------------
  The derived column "nominal_amount_sdr" comes out as FLOAT, not
  DECIMAL. Reason: T-SQL data type precedence rules. When a DECIMAL
  value ("nominal_amount", exact base-10) is multiplied by a FLOAT
  value ("sdr_per_unit", IEEE 754 approximate), the result is promoted
  to FLOAT. The output therefore inherits FLOAT's approximate-
  arithmetic behaviour. Worth flagging if downstream marts ever do
  further arithmetic on the SDR column; they may need
  CAST(nominal_amount_sdr AS DECIMAL(38, 10)) to reclaim exact
  arithmetic.
*/