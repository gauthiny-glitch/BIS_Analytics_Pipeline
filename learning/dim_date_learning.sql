/* ============================================================================
   dbt run -- DAG execution order for bis_analytics
   ============================================================================

   TIER 1 (no upstream -- run first, below 5 models could be in parallel)
     models/staging/stg_deals.sql           depends on source('bis_raw','src_deals')      DONE
     models/staging/stg_fx_rates.sql        depends on source('bis_raw','src_fx_rates')   DONE
     models/mart/dim_currency.sql           VALUES literal, no ref/source                 DONE
     models/mart/dim_product.sql            VALUES literal, no ref/source                 DONE
     models/mart/dim_date.sql               date spine, no ref/source                     <--- WE ARE HERE (dbt core)

   TIER 2 (run in 6th position)
     models/intermediate/int_deals_with_sdr.sql   ref('stg_deals') + ref('stg_fx_rates')  DONE

   TIER 3 (run in 7th and 8th) (the 2 below models could be in run parallel)
     models/mart/dim_counterparty.sql       ref('int_deals_with_sdr')
     models/mart/fct_currency_deposits.sql  ref('int_deals_with_sdr')

   TIER 4
     models/mart/mart_maturity_ladder.sql   ref('fct_currency_deposits')

   ----------------------------------------------------------------------------
   Total: 9 model files = 9 views in bis_analytics.dbo (PASS=9)
   ============================================================================ */


/*
  dim_date
  ========
  Dimension table: one row per calendar date.
  Grain: alias_full_date (unique).

  Date range: 1 April 2023 to 31 March 2025
  Covers the full period of the simulated BIS deposit book.

  Date sequence generated using a cross-join CTE technique:
    CTE_n1 = 2 rows, CTE_n2 = 4, CTE_n3 = 16, CTE_n4 = 256, CTE_n5 = 65536
    TOP 731 taken from CTE_n5 — no system tables or recursion required.
    Works on Fabric SQL Analytics Endpoint.

  alias_date_key uses pure arithmetic (year * 10000 + month * 100 + day)
  to avoid format() compatibility issues.

  alias_fiscal_year follows BIS convention: April to March.
    e.g. 1 April 2023 to 31 March 2024 = FY2023-24
         1 April 2024 to 31 March 2025 = FY2024-25

  alias_fiscal_quarter:
    FQ1 = April, May, June
    FQ2 = July, August, September
    FQ3 = October, November, December
    FQ4 = January, February, March

  [CONFIRMED] BIS financial year: April to March — BIS annual report

  Materialisation: view
*/

/*
  dim_date
  ========
  What this is:
  A calendar table with one row per day, from 1 April 2023 to 31 March 2025.
  731 rows total, covering the full period of the simulated BIS deposit book.

  How the row list is built:
  Five small CTEs ('CTE_n1', 'CTE_n2', 'CTE_n3', 'CTE_n4', 'CTE_n5') doubled
  into themselves to produce 65,536 rows. The top 731 are kept and numbered
  0 to 730. Each number is added as a day offset to 1 April 2023, which
  gives every calendar date in the range. This trick avoids system tables
  and recursion, neither of which works on Fabric SQL Analytics Endpoint.

  How the primary key is built:
  Column "alias_date_key" is the date written as an integer in YYYYMMDD form,
  built with pure arithmetic: year * 10000 + month * 100 + day. So 1 April
  2023 becomes 20230401. This avoids the FORMAT() function, which is
  unavailable on Fabric SQL Analytics Endpoint.

  BIS fiscal year (column "alias_fiscal_year"):
  The BIS year runs April to March, not January to December.
    April 2023 to March 2024 = FY2023-24
    April 2024 to March 2025 = FY2024-25
  Source: BIS annual report.

  BIS fiscal quarter (column "alias_fiscal_quarter"):
    FQ1 = April, May, June
    FQ2 = July, August, September
    FQ3 = October, November, December
    FQ4 = January, February, March

  How it is stored:
  As a view in Fabric. A view stores no data of its own, it just runs the
  query each time something asks for it.
*/

/*
  Jinja config block. Tells dbt to store this model as a view in Fabric.
  Optional. Overrides the materialisation default set in dbt_project.yml.
*/
{{
    config(
        materialized = 'view'
    )
}}

/*
  CTE chain that manufactures 65,536 rows from nothing.
  Each CTE doubles the row count of the one before it.

    'CTE_n1' = 2 rows           (literal: select 1 union all select 1)
    'CTE_n2' = 2 x 2 = 4        (cross-join 'CTE_n1' with itself)
    'CTE_n3' = 4 x 4 = 16
    'CTE_n4' = 16 x 16 = 256
    'CTE_n5' = 256 x 256 = 65,536

  The actual data does not matter. Each CTE just selects the literal 1.
  We are manufacturing row counts, not column values.

  The comma in 'from CTE_n1, CTE_n1 as alias_b' is shorthand for a CROSS
  JOIN. Every row of the left side pairs with every row of the right
  side. The 'as alias_b' alias is required because you cannot reference
  the same table-view twice in one FROM without disambiguating.

  Why this trick? Fabric SQL Analytics Endpoint does not support system
  tables (sys.all_objects, master..spt_values) or recursive CTEs, so the
  usual row-generation patterns are unavailable. Manual doubling is the
  workaround.

  Why stop at 65,536? We need 731 rows (one per day, 1 April 2023 to
  31 March 2025 inclusive — 366 days for FY2023-24 plus 365 days for
  FY2024-25). 256 is too few; 65,536 is the next doubling step. The
  'CTE_nums' CTE below trims to top 731.
*/
with                                                                  -- start of the CTE chain

CTE_n1 as (select 1 as alias_n union all select 1),                   -- 2 rows: two literal 1s joined by UNION ALL
CTE_n2 as (select 1 as alias_n from CTE_n1, CTE_n1 as alias_b),       -- cross-join 'CTE_n1' with itself: 2 x 2 = 4 rows
CTE_n3 as (select 1 as alias_n from CTE_n2, CTE_n2 as alias_b),       -- cross-join 'CTE_n2' with itself: 4 x 4 = 16 rows
CTE_n4 as (select 1 as alias_n from CTE_n3, CTE_n3 as alias_b),       -- cross-join 'CTE_n3' with itself: 16 x 16 = 256 rows
CTE_n5 as (select 1 as alias_n from CTE_n4, CTE_n4 as alias_b),       -- cross-join 'CTE_n4' with itself: 256 x 256 = 65,536 rows

/*
  Trim 'CTE_n5' down to 731 rows, numbered 0 to 730.

  'CTE_n5' coming in: 65,536 rows, every row is just alias_n = 1.

  Three things happen here:
    1. row_number() assigns a sequential integer to each row,
       starting at 1. The 'over (order by (select null))' clause
       is a syntactic placeholder. T-SQL requires an ORDER BY
       inside OVER; we do not care about the order, so (select null)
       satisfies the rule without actually ordering anything.

    2. The '- 1' shifts the numbering from 1-indexed to 0-indexed.
       Needed because the next CTE uses this column as a day offset,
       and we want day 0 to be 1 April 2023.

    3. 'top 731' keeps only the first 731 rows. The rest are
       discarded.

  Output: a 731-row table with one column "alias_n" holding
  0, 1, 2, ..., 730.
*/
CTE_nums as (                                                         -- open 'CTE_nums': trim and number the rows
    select top 731                                                    -- keep only the first 731 rows
        row_number() over (order by (select null)) - 1 as alias_n     -- row_number gives 1..731, then -1 shifts to 0..730
    from CTE_n5                                                       -- read from the 65,536-row 'CTE_n5'
),                                                                    -- close 'CTE_nums'
/*
  Output of 'CTE_nums':

    +-----------+
    | "alias_n" |
    +-----------+
    |         0 |
    |         1 |
    |         2 |
    |         3 |
    |       ... |
    |       728 |
    |       729 |
    |       730 |
    +-----------+

  731 rows. One column "alias_n". Integer values 0 to 730 inclusive.
*/

/*
  Turn each integer into a real calendar date.

  Input: 'CTE_nums' has column "alias_n" with values 0, 1, 2, ..., 730.

  For every row, we compute:
    dateadd(day, alias_n, cast('2023-04-01' as date))

  Two pieces inside the expression:
    1. cast('2023-04-01' as date) turns the string literal '2023-04-01'
       into a proper DATE type. Forces unambiguous interpretation.
    2. dateadd(day, alias_n, <base_date>) adds alias_n days to the base.

  Examples:
    alias_n = 0    ->  dateadd(day, 0,   '2023-04-01')  =  2023-04-01
    alias_n = 1    ->  dateadd(day, 1,   '2023-04-01')  =  2023-04-02
    alias_n = 30   ->  dateadd(day, 30,  '2023-04-01')  =  2023-05-01
    alias_n = 365  ->  dateadd(day, 365, '2023-04-01')  =  2024-03-31
    alias_n = 730  ->  dateadd(day, 730, '2023-04-01')  =  2025-03-31

  Output: 731 rows, one column "alias_full_date" of type DATE,
  every calendar date from 1 April 2023 to 31 March 2025 inclusive.
*/
CTE_date_sequence as (                                                            -- open 'CTE_date_sequence': turn each integer into a calendar date
    select dateadd(day, alias_n, cast('2023-04-01' as date)) as alias_full_date   -- add alias_n days to 2023-04-01, naming the result "alias_full_date"
    from CTE_nums                                                                 -- read the 731-row sequence (alias_n = 0..730) from 'CTE_nums'
),                                                                                -- close 'CTE_date_sequence'
/*
  dateadd() is a built-in T-SQL function. Signature:

    dateadd(<part>, <number>, <date>)  ->  date

  It adds <number> units of <part> to <date> and returns the result.

  Examples:
    dateadd(day,   1, '2023-04-01')  ->  2023-04-02
    dateadd(day,  30, '2023-04-01')  ->  2023-05-01
    dateadd(month, 2, '2023-04-01')  ->  2023-06-01
    dateadd(year,  1, '2023-04-01')  ->  2024-04-01

  The first argument is a date-part keyword, not a string.
  Valid keywords without quotes: day, month, year, hour, minute,
  second, quarter, week, plus a few others.

  Part of a small family of T-SQL date functions used throughout
  'dim_date' and 'mart_maturity_ladder':
    dateadd, datediff, datepart, datename, eomonth,
    plus the shorthand year(date), month(date), day(date).
*/
/*
  Output of 'CTE_date_sequence':

    +-------------------+
    | "alias_full_date" |
    +-------------------+
    |        2023-04-01 |
    |        2023-04-02 |
    |        2023-04-03 |
    |        2023-04-04 |
    |               ... |
    |        2025-03-29 |
    |        2025-03-30 |
    |        2025-03-31 |
    +-------------------+

  731 rows. One column "alias_full_date" of type DATE.
  Calendar dates from 1 April 2023 to 31 March 2025 inclusive.
*/

/*
  Open 'CTE_with_attributes': decorate each date from 'CTE_date_sequence'
  with 8 calendar and BIS-fiscal attributes.

  First CTE in this file that produces a multi-column output. Earlier
  CTEs each had a single output column ("alias_n" or "alias_full_date").
  This one will list 8, all derived from "alias_full_date" via T-SQL
  date functions and a couple of CASE expressions.

  The 'select' keyword opens the column list. Everything between here
  and the 'from CTE_date_sequence' line at the bottom is the column
  inventory of the output table.
*/
CTE_with_attributes as (

    select

        -- COLUMN 1
        /* Column 1 (out of 8): "alias_date_key". Primary key of 'dim_date'.

          A single integer per row in YYYYMMDD form, computed by pure
          arithmetic: year * 10000 + month * 100 + day.

          Examples:
            2023-04-01  ->  2023 * 10000 +  4 * 100 +  1  =  20230401
            2024-12-25  ->  2024 * 10000 + 12 * 100 + 25  =  20241225
            2025-03-31  ->  2025 * 10000 +  3 * 100 + 31  =  20250331

          Two useful properties:
            1. Sortable. Ordering by "alias_date_key" gives chronological
               order automatically.
            2. Decodable. year = key / 10000, month = (key / 100) % 100,
               day = key % 100.

          Why arithmetic and not FORMAT(date, 'yyyyMMdd')?
          Fabric SQL Analytics Endpoint does not support FORMAT(),
          so we cannot use the usual one-liner. Arithmetic also avoids
          per-row string conversion, which is marginally faster on
          large tables.*/

        -- Column primary key (pure arithmetic — no format() needed)
        year(alias_full_date) * 10000
        + month(alias_full_date) * 100
        + day(alias_full_date)                       as alias_date_key,


          -- COLUMN 2
          /* Column 2 (out of 8): "alias_full_date". Passthrough.

          The date itself, carried through from 'CTE_date_sequence'
          unchanged. No expression, no AS clause, just the column
          name. The output column inherits the upstream name and
          DATE type.

          Why include it at all?
          The fact table 'fct_currency_deposits' joins on the integer
          "alias_date_key", but downstream Power BI users frequently
          want the actual date for filtering, axis labels, and slicer
          pickers. Keeping "alias_full_date" on the dim saves a
          CAST/CONVERT in every report.
        */
        -- Column date itself
        alias_full_date,

        -- COLUMN 3
        /* Column 3 of 8: "alias_year". Calendar year as an integer.

          The year() function extracts the year part from a DATE.
          Returns an INT.

          Examples:
            year('2023-04-01')  ->  2023
            year('2024-01-15')  ->  2024
            year('2025-03-31')  ->  2025

          Note: this is the calendar year, not the BIS fiscal year.
          1 February 2024 has alias_year = 2024 but
          alias_fiscal_year = 'FY2023-24' (column 7 handles that).
        */
        -- Column calendar attribute
        year(alias_full_date)                        as alias_year,


        -- COLUMN 4
        /* Column 4 of 8: "alias_month_num". Calendar month as an integer
        1 to 12.

        The month() function extracts the month part from a DATE.
        Returns an INT.

        Examples:
          month('2023-04-01')  ->   4
          month('2024-12-25')  ->  12
          month('2025-03-31')  ->   3

        The "_num" suffix marks this as the integer form, distinct
        from "alias_month_name" (column 5) which is the string
        form ('April', 'December', etc.).
        */
        -- Column calendar attribute
        month(alias_full_date)                       as alias_month_num,


        -- COLUMN 5
        /* Column 5 (out of 8): "alias_month_name". Calendar month as a string.

        The datename() function returns the name of a date part as
        a string. Signature:

          datename(<part>, <date>)  ->  string

        Same shape as dateadd() and datepart() (date-part keyword
        first, date second).

        Examples:
          datename(month, '2023-04-01')  ->  'April'
          datename(month, '2024-12-25')  ->  'December'
          datename(month, '2025-03-31')  ->  'March'

        Localization note: the language of the returned string
        depends on the session's SET LANGUAGE setting. Default on
        Fabric is English, so 'April', 'December' etc. If you ever
        need French ('Avril', 'Decembre'), it is a one-line session
        setting, not a code change.
        */
        -- Column calendar attribute
        datename(month, alias_full_date)             as alias_month_name,

        -- COLUMN 6
        /* Column 6 (out of 8): "alias_quarter". Calendar quarter as an integer
        1 to 4.

        The datepart() function returns the integer value of a date
        part. Signature:

          datepart(<part>, <date>)  ->  int

         Same shape as datename() and dateadd() (date-part keyword
        first), but returns an integer instead of a string.

        Examples:
          datepart(quarter, '2023-04-01')  ->  2  (April -> Q2)
          datepart(quarter, '2024-12-25')  ->  4  (December -> Q4)
          datepart(quarter, '2025-03-31')  ->  1  (March -> Q1)

        Quarter mapping (calendar):
          Q1 = January, February, March
          Q2 = April, May, June
          Q3 = July, August, September
          Q4 = October, November, December

        Note: this is the calendar quarter, not the BIS fiscal
        quarter. 1 April is calendar Q2 but BIS FQ1. Column 8
        handles the fiscal version.
        */
        -- Column calendar attribute
        datepart(quarter, alias_full_date)           as alias_quarter,


        -- COLUMN 7
        /*
          Column 7 (out of 8): "alias_fiscal_year". BIS fiscal year as a string,
          format 'FY{start}-{end_short}'.

          Examples:
            2023-04-01  ->  'FY2023-24'   (April starts FY2023-24)
            2023-12-31  ->  'FY2023-24'   (December stays in FY2023-24)
            2024-01-01  ->  'FY2023-24'   (January is the second half)
            2024-03-31  ->  'FY2023-24'   (last day of FY2023-24)
            2024-04-01  ->  'FY2024-25'   (first day of FY2024-25)
            2025-03-31  ->  'FY2024-25'   (last day of FY2024-25)

          Two new constructs in this column:

          1. SEARCHED CASE form
                case
                    when <condition> then <result>
                    else                  <result>
                end
             Each WHEN tests an arbitrary boolean. No expression after
             CASE itself. Different from the simple CASE form
             (column 8) which compares one expression to a list of
             values.

          2. right() and concat()
             - right(<value>, <n>) returns the rightmost n characters
               of a string. Integers are implicitly converted, so
               right(2024, 2) returns '24'.
             - concat(<a>, <b>, ...) joins multiple values into a
               single string. Integers convert automatically; NULL
               is treated as empty.

          Branch logic:
          The condition month(alias_full_date) >= 4 splits the calendar
          year into the two halves of the BIS fiscal year.

            April to December:
              Fiscal year started THIS calendar year.
              Label: 'FY' + thisYear + '-' + last2(thisYear + 1)
              Example: 2023-06-15 -> 'FY2023-24'

            January to March:
              Fiscal year started LAST calendar year.
              Label: 'FY' + (thisYear - 1) + '-' + last2(thisYear)
              Example: 2024-02-29 -> 'FY2023-24'
        */
        -- Column BIS fiscal year (April to March)
        case
            when month(alias_full_date) >= 4
            then concat('FY', year(alias_full_date), '-', right(year(alias_full_date) + 1, 2))
            else concat('FY', year(alias_full_date) - 1, '-', right(year(alias_full_date), 2))
        end                                          as alias_fiscal_year,

        -- COLUMN 8
        /*
          Column 8 of 8: "alias_fiscal_quarter". BIS fiscal quarter
          as a string, 'FQ1' to 'FQ4'.

          BIS fiscal quarters (April to March fiscal year):
            FQ1 = April, May, June
            FQ2 = July, August, September
            FQ3 = October, November, December
            FQ4 = January, February, March

          Examples:
            2023-04-01  ->  'FQ1'   (April)
            2023-09-30  ->  'FQ2'   (September)
            2023-12-31  ->  'FQ3'   (December)
            2024-01-15  ->  'FQ4'   (January)
            2025-03-31  ->  'FQ4'   (March, last month of fiscal year)

          New construct: SIMPLE CASE form
                case <expression>
                    when <value1> then <result1>
                    when <value2> then <result2>
                    ...
                end
          The expression is evaluated once, then compared by equality
          to each WHEN value. Use this form when you are mapping one
          value against a flat list of options. Use the SEARCHED CASE
          form (column 7) when each branch needs an arbitrary
          boolean condition.

          No ELSE clause here. If month() somehow returned a value
          outside 1 to 12, the result would be NULL. Safe in practice
          because month() of a valid DATE is always 1 to 12.
        */
        -- Column BIS fiscal quarter
case month(alias_full_date)                                                  -- open simple CASE on the month integer
            when 4  then 'FQ1'                                                       -- April     -> FQ1
            when 5  then 'FQ1'                                                       -- May       -> FQ1
            when 6  then 'FQ1'                                                       -- June      -> FQ1
            when 7  then 'FQ2'                                                       -- July      -> FQ2
            when 8  then 'FQ2'                                                       -- August    -> FQ2
            when 9  then 'FQ2'                                                       -- September -> FQ2
            when 10 then 'FQ3'                                                       -- October   -> FQ3
            when 11 then 'FQ3'                                                       -- November  -> FQ3
            when 12 then 'FQ3'                                                       -- December  -> FQ3
            when 1  then 'FQ4'                                                       -- January   -> FQ4
            when 2  then 'FQ4'                                                       -- February  -> FQ4
            when 3  then 'FQ4'                                                       -- March     -> FQ4
        end                                          as alias_fiscal_quarter        -- close CASE, name the column "alias_fiscal_quarter"

    from CTE_date_sequence                                                           -- pull the 731-row date list from 'CTE_date_sequence'

)                                                                                    -- close 'CTE_with_attributes'

select * from CTE_with_attributes                                                    -- final output query; dbt materialises it as the view 'dim_date'


/*
  Output of 'select * from CTE_with_attributes' (the final 'dim_date' view):

  +----------------+-----------------+------------+-----------------+------------------+---------------+-------------------+----------------------+
  | alias_date_key | alias_full_date | alias_year | alias_month_num | alias_month_name | alias_quarter | alias_fiscal_year | alias_fiscal_quarter |
  +----------------+-----------------+------------+-----------------+------------------+---------------+-------------------+----------------------+
  |       20230401 |      2023-04-01 |       2023 |               4 | April            |             2 | FY2023-24         | FQ1                  |
  |       20230402 |      2023-04-02 |       2023 |               4 | April            |             2 | FY2023-24         | FQ1                  |
  |       20230930 |      2023-09-30 |       2023 |               9 | September        |             3 | FY2023-24         | FQ2                  |
  |       20231231 |      2023-12-31 |       2023 |              12 | December         |             4 | FY2023-24         | FQ3                  |
  |       20240331 |      2024-03-31 |       2024 |               3 | March            |             1 | FY2023-24         | FQ4                  |
  |       20240401 |      2024-04-01 |       2024 |               4 | April            |             2 | FY2024-25         | FQ1                  |
  |            ... |             ... |        ... |             ... | ...              |           ... | ...               | ...                  |
  |       20250331 |      2025-03-31 |       2025 |               3 | March            |             1 | FY2024-25         | FQ4                  |
  +----------------+-----------------+------------+-----------------+------------------+---------------+-------------------+----------------------+

  731 rows, 8 columns. Materialised as the view 'bis_analytics.dbo.dim_date'.

  Column types:
    "alias_date_key"       INT          YYYYMMDD form, sortable, primary key
    "alias_full_date"      DATE         calendar date itself
    "alias_year"           INT          calendar year
    "alias_month_num"      INT          calendar month, 1 to 12
    "alias_month_name"     NVARCHAR     calendar month name (English)
    "alias_quarter"        INT          calendar quarter, 1 to 4
    "alias_fiscal_year"    NVARCHAR     BIS fiscal year, 'FY{start}-{end_short}'
    "alias_fiscal_quarter" NVARCHAR     BIS fiscal quarter, 'FQ1' to 'FQ4'
*/