-- REMINDER
/* ============================================================================
   dbt run -- DAG execution order for bis_analytics
   ============================================================================

   TIER 1 (no upstream -- run first, one at a time, alphabetical-ish topological order) (could be in parallel if set up)
     models/staging/stg_deals.sql           depends on source('bis_raw','src_deals')     DONE (1)
     models/staging/stg_fx_rates.sql        depends on source('bis_raw','src_fx_rates')  DONE (1)
---> models/mart/dim_currency.sql           VALUES literal, no ref/source                <--- WE ARE HERE
     models/mart/dim_product.sql            VALUES literal, no ref/source
     models/mart/dim_date.sql               date spine, no ref/source

   TIER 2
     models/intermediate/int_deals_with_sdr.sql   ref('stg_deals') + ref('stg_fx_rates') DONE (2)

   TIER 3 (parallel)
     models/mart/dim_counterparty.sql       ref('int_deals_with_sdr')
     models/mart/fct_currency_deposits.sql  ref('int_deals_with_sdr')

   TIER 4
     models/mart/mart_maturity_ladder.sql   ref('fct_currency_deposits')

   ----------------------------------------------------------------------------
   Total: 9 model files = 9 views in bis_analytics.dbo (PASS=9)
   ============================================================================ */


/*
  dim_currency
  ============
  Lookup table for the 10 currencies that appear in the BIS deposit book.
  One row per currency.

  Hardcoded because the list is fixed by the data we generate.

  is_sdr_basket flag:
    'Y' for the 5 IMF SDR basket currencies: USD, EUR, GBP, JPY, CNY.
    'N' for the 5 others: AUD, CAD, CHF, SGD, KRW (they appear in the
    deposit book but are not part of the SDR basket).

  How it is stored: as a view in Fabric. A view stores no data of its own,
  it just runs the query each time something asks for it.
*/

/*
  Tell dbt how to store this model in Fabric.
  Here: as a view (the query is saved, no data is stored).
*/
{{
    config(
        materialized = 'view'
    )
}}

with CTE_currencies as (              -- open CTE 'currencies' (sticky note)

    select *                      -- grab every column
    from (                        -- ...from the sub-query that follows

values -- open hand-typed rows
            ('USD', 'US Dollar',          'Y'),  -- row 1: code, name, SDR-basket flag
            ('EUR', 'Euro',               'Y'),  -- row 2
            ('GBP', 'British Pound',      'Y'),  -- row 3
            ('JPY', 'Japanese Yen',       'Y'),  -- row 4
            ('CNY', 'Chinese Renminbi',   'Y'),  -- row 5  (last SDR-basket currency)
            ('CHF', 'Swiss Franc',        'N'),  -- row 6
            ('AUD', 'Australian Dollar',  'N'),  -- row 7
            ('CAD', 'Canadian Dollar',    'N'),  -- row 8
            ('SGD', 'Singapore Dollar',   'N'),  -- row 9
            ('KRW', 'South Korean Won',   'N')   -- row 10  (no trailing comma on last row)            
) as t (currency, currency_name, is_sdr_basket)
    --  |          |
    --  |          +-- name the 3 columns of the sub-query, in order
    --  +-- close the sub-query and call it 't'

) -- close CTE 'CTE_currencies'

select * from CTE_currencies   -- final query: this is what dbt materialises as the view


/* ============================================================================
   ) as t (currency, currency_name, is_sdr_basket)
   ============================================================================

   Three pieces in one line:

     )                                            close the sub-query opened
                                                  by 'from (' earlier

     as t                                         give the sub-query a short
                                                  name. T-SQL requires every
                                                  sub-query to have one;
                                                  't' is a throwaway alias

     (currency, currency_name, is_sdr_basket)     label the 3 columns of the
                                                  sub-query, in order

   In one sentence:
   "Close the sub-query, call it 't', and name its 3 columns
    "currency", "currency_name", "is_sdr_basket" (in that order)."

   ----------------------------------------------------------------------------
   Why this line carries all the column names

   The 'values' rows are naked strings in fixed order:
       ('USD', 'US Dollar', 'Y')
   No column names are attached to them. Without this line, T-SQL would
   auto-name the columns column1, column2, column3.

   The matching is purely positional:
       1st value in each row  ->  "currency"
       2nd value in each row  ->  "currency_name"
       3rd value in each row  ->  "is_sdr_basket"

   Swap two names on this line and every row's data would land in the
   wrong column, with no error and no warning. T-SQL trusts the order;
   it does not check whether 'USD' looks like a code or a name.

   ----------------------------------------------------------------------------
   Why 't' and not something descriptive

   The alias 't' exists only to satisfy the syntax requirement. It is
   never referenced again in this file. The outer 'select * from
   CTE_currencies' reads from the CTE above, not from 't'. So the alias
   does no work; it just has to exist. Standard dbt convention: short
   throwaway alias for the inner sub-query, real names on the CTEs.
============================================================================ */


/* ============================================================================
   Output view: 'bis_analytics.dbo.dim_currency'
   ============================================================================

   10 rows x 3 columns

   +----------+--------------------+----------------+
   | currency | currency_name      | is_sdr_basket  |
   +----------+--------------------+----------------+
   | USD      | US Dollar          | Y              |
   | EUR      | Euro               | Y              |
   | GBP      | British Pound      | Y              |
   | JPY      | Japanese Yen       | Y              |
   | CNY      | Chinese Renminbi   | Y              |
   | CHF      | Swiss Franc        | N              |
   | AUD      | Australian Dollar  | N              |
   | CAD      | Canadian Dollar    | N              |
   | SGD      | Singapore Dollar   | N              |
   | KRW      | South Korean Won   | N              |
   +----------+--------------------+----------------+

   ----------------------------------------------------------------------------
   Columns

     "currency"        VARCHAR(3)    primary key, ISO 4217 code,
                                     joins to 'fct_currency_deposits'."currency"
     "currency_name"   VARCHAR(20)   human-readable label for Power BI
                                     tooltips, axis labels, slicer text
     "is_sdr_basket"   VARCHAR(1)    'Y' for IMF SDR basket members,
                                     'N' otherwise; filterable in Power BI

   ----------------------------------------------------------------------------
   How to verify in SSMS

     select * from bis_analytics.dbo.dim_currency;

     -- with explicit ordering:
     select *
     from bis_analytics.dbo.dim_currency
     order by is_sdr_basket desc, currency;

   ----------------------------------------------------------------------------
   Storage footprint

   The view stores zero bytes. Every time something queries 'dim_currency',
   Fabric re-runs the saved query (the 'values' literal + the 'select *')
   and returns the 10 rows. Sub-millisecond on this size, entirely free.
   The 'values' block is a hardcoded constant inside the saved query.
============================================================================ */