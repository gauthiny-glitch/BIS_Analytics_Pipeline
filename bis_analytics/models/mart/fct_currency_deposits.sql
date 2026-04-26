/*
  fct_currency_deposits
  =====================
  Fact table: one row per currency deposit deal.
  Grain: deal_id (unique).

  Centre of the star schema. All dimension tables join to this fact table.

  Source: int_deals_with_sdr (deals already joined to FX rates, SDR amounts computed)

  Foreign keys:
    cpty_code         -> dim_counterparty.cpty_code
    deal_type         -> dim_product.deal_type
    currency          -> dim_currency.currency
    trade_date_key    -> dim_date.date_key
    value_date_key    -> dim_date.date_key
    maturity_date_key -> dim_date.date_key (NULL for sight accounts)

  date_key format: YYYYMMDD integer using pure arithmetic
  (year * 10000 + month * 100 + day) — avoids format() compatibility issues.

  Measures:
    nominal_amount      : deal face value in original currency
    nominal_amount_sdr  : deal face value converted to SDR
    rate_pct            : interest rate as percentage (e.g. 2.50)
    rate_decimal        : interest rate as decimal (e.g. 0.025000)

  Degenerate dimensions (no separate dim table):
    deal_id, deal_status, portfolio, book, fx_rate_date, sdr_per_unit

  [CONFIRMED] Currency deposits = borrowed funds at fair value — BIS annual report
  [CONFIRMED] BIS unit of account = SDR — BIS annual report

  Materialisation: view
*/

{{
    config(
        materialized = 'view'
    )
}}

with deals as (

    select * from {{ ref('int_deals_with_sdr') }}

),

final as (

    select

        -- natural key
        deal_id,

        -- foreign keys to dimensions
        deal_type,
        cpty_code,
        currency,

        -- date foreign keys (integer YYYYMMDD matching dim_date.date_key)
        year(trade_date)    * 10000 + month(trade_date)    * 100 + day(trade_date)    as trade_date_key,
        year(value_date)    * 10000 + month(value_date)    * 100 + day(value_date)    as value_date_key,
        year(maturity_date) * 10000 + month(maturity_date) * 100 + day(maturity_date) as maturity_date_key,

        -- measures
        nominal_amount,
        nominal_amount_sdr,
        rate_pct,
        rate_decimal,

        -- degenerate dimensions
        deal_status,
        portfolio,
        book,
        fx_rate_date,
        sdr_per_unit,

        -- audit
        created_dt,
        last_modified_dt

    from deals

)

select * from final
