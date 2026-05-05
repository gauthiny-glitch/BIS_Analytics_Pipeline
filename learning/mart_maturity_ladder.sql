/*
  mart_maturity_ladder
  ====================
  Aggregated maturity profile of the VALIDATED (active) deposit book.
  Grain: one row per maturity_bucket per as_of_date.

  as_of_dates: every month-end from April 2023 to March 2025 (24 dates).
  EOMONTH() ensures the last calendar day of each month is used.

  Only VALIDATED deals included — MATURED and CANCELLED are excluded.
  For each as_of_date, deals already matured by that date are excluded
  (days_to_maturity > 0), except Sight accounts which are always Overnight.

  Maturity buckets:
    1. Overnight   — Sight accounts (no fixed maturity, on-demand)
    2. <= 1 Week   — days_to_maturity <= 7
    3. <= 1 Month  — days_to_maturity <= 30
    4. <= 3 Months — days_to_maturity <= 90
    5. <= 6 Months — days_to_maturity <= 180
    6. <= 1 Year   — days_to_maturity <= 365
    7. > 1 Year    — days_to_maturity > 365

  maturity_date reconstructed from maturity_date_key (YYYYMMDD integer)
  using datefromparts() — avoids string conversion.

  Output: up to 168 rows (24 dates x 7 buckets), fewer if some buckets
  are empty for a given date.

  In Power BI: use as_of_date as a slicer to see the maturity profile
  at any month-end across the data period.

  Source: fct_currency_deposits (VALIDATED deals only)

  Materialisation: view
*/

{{
    config(
        materialized = 'view'
    )
}}

with

month_nums as (

    select 0  as n union all select 1  union all select 2  union all select 3
    union all select 4  union all select 5  union all select 6  union all select 7
    union all select 8  union all select 9  union all select 10 union all select 11
    union all select 12 union all select 13 union all select 14 union all select 15
    union all select 16 union all select 17 union all select 18 union all select 19
    union all select 20 union all select 21 union all select 22 union all select 23

),

as_of_dates as (

    -- Generate 24 month-end dates: April 2023 to March 2025
    select eomonth(dateadd(month, n, cast('2023-04-01' as date))) as as_of_date
    from month_nums

),

active_deals as (

    select
        f.deal_id,
        f.deal_type,
        f.nominal_amount_sdr,
        f.maturity_date_key,

        -- Reconstruct maturity date from YYYYMMDD integer key
        case
            when f.maturity_date_key is null then null
            else datefromparts(
                f.maturity_date_key / 10000,
                (f.maturity_date_key / 100) % 100,
                f.maturity_date_key % 100
            )
        end as maturity_date

    from {{ ref('fct_currency_deposits') }} f
    where f.deal_status = 'VALIDATED'

),

crossed as (

    -- Cross join: every deal x every as-of date
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

with_buckets as (

    select
        as_of_date,
        case
            when deal_type = 'SIGHT_DEP'       then '1. Overnight'
            when days_to_maturity <=   7        then '2. <= 1 Week'
            when days_to_maturity <=  30        then '3. <= 1 Month'
            when days_to_maturity <=  90        then '4. <= 3 Months'
            when days_to_maturity <= 180        then '5. <= 6 Months'
            when days_to_maturity <= 365        then '6. <= 1 Year'
            else                                     '7. > 1 Year'
        end as maturity_bucket,
        nominal_amount_sdr

    from crossed
    where days_to_maturity > 0
       or deal_type = 'SIGHT_DEP'  -- Sight accounts are always included

),

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
