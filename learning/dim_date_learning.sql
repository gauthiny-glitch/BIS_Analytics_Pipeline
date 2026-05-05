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
with

CTE_n1 as (select 1 as alias_n union all select 1),
CTE_n2 as (select 1 as alias_n from CTE_n1, CTE_n1 as alias_b),
CTE_n3 as (select 1 as alias_n from CTE_n2, CTE_n2 as alias_b),
CTE_n4 as (select 1 as alias_n from CTE_n3, CTE_n3 as alias_b),
CTE_n5 as (select 1 as alias_n from CTE_n4, CTE_n4 as alias_b),

CTE_nums as (
    select top 731
        row_number() over (order by (select null)) - 1 as alias_n
    from CTE_n5
),



CTE_date_sequence as (
    select dateadd(day, alias_n, cast('2023-04-01' as date)) as alias_full_date
    from CTE_nums
),

CTE_with_attributes as (

    select

        -- primary key (pure arithmetic — no format() needed)
        year(alias_full_date) * 10000
        + month(alias_full_date) * 100
        + day(alias_full_date)                       as alias_date_key,

        -- date itself
        alias_full_date,

        -- calendar attributes
        year(alias_full_date)                        as alias_year,
        month(alias_full_date)                       as alias_month_num,
        datename(month, alias_full_date)             as alias_month_name,
        datepart(quarter, alias_full_date)           as alias_quarter,

        -- BIS fiscal year (April to March)
        case
            when month(alias_full_date) >= 4
            then concat('FY', year(alias_full_date), '-', right(year(alias_full_date) + 1, 2))
            else concat('FY', year(alias_full_date) - 1, '-', right(year(alias_full_date), 2))
        end                                          as alias_fiscal_year,

        -- BIS fiscal quarter
        case month(alias_full_date)
            when 4  then 'FQ1'
            when 5  then 'FQ1'
            when 6  then 'FQ1'
            when 7  then 'FQ2'
            when 8  then 'FQ2'
            when 9  then 'FQ2'
            when 10 then 'FQ3'
            when 11 then 'FQ3'
            when 12 then 'FQ3'
            when 1  then 'FQ4'
            when 2  then 'FQ4'
            when 3  then 'FQ4'
        end                                          as alias_fiscal_quarter

    from CTE_date_sequence

)

select * from CTE_with_attributes