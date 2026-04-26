/*
  dim_date
  ========
  Dimension table: one row per calendar date.
  Grain: full_date (unique).

  Date range: 1 April 2023 to 31 March 2025
  Covers the full period of the simulated BIS deposit book.

  Date sequence generated using a cross-join CTE technique:
    n1 = 2 rows, n2 = 4, n3 = 16, n4 = 256, n5 = 65536
    TOP 730 taken from n5 — no system tables or recursion required.
    Works on Fabric SQL Analytics Endpoint.

  date_key uses pure arithmetic (year * 10000 + month * 100 + day)
  to avoid format() compatibility issues.

  fiscal_year follows BIS convention: April to March.
    e.g. 1 April 2023 to 31 March 2024 = FY2023-24
         1 April 2024 to 31 March 2025 = FY2024-25

  fiscal_quarter:
    FQ1 = April, May, June
    FQ2 = July, August, September
    FQ3 = October, November, December
    FQ4 = January, February, March

  [CONFIRMED] BIS financial year: April to March — BIS annual report

  Materialisation: view
*/

{{
    config(
        materialized = 'view'
    )
}}

with

n1 as (select 1 as n union all select 1),
n2 as (select 1 as n from n1, n1 as b),
n3 as (select 1 as n from n2, n2 as b),
n4 as (select 1 as n from n3, n3 as b),
n5 as (select 1 as n from n4, n4 as b),

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

        -- primary key (pure arithmetic — no format() needed)
        year(full_date) * 10000
        + month(full_date) * 100
        + day(full_date)                             as date_key,

        -- date itself
        full_date,

        -- calendar attributes
        year(full_date)                              as year,
        month(full_date)                             as month_num,
        datename(month, full_date)                   as month_name,
        datepart(quarter, full_date)                 as quarter,

        -- BIS fiscal year (April to March)
        case
            when month(full_date) >= 4
            then concat('FY', year(full_date), '-', right(year(full_date) + 1, 2))
            else concat('FY', year(full_date) - 1, '-', right(year(full_date), 2))
        end                                          as fiscal_year,

        -- BIS fiscal quarter
        case month(full_date)
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
        end                                          as fiscal_quarter

    from date_sequence

)

select * from with_attributes
