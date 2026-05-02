/*
  int_deals_with_sdr
  ==================
  Intermediate model: joins stg_deals to stg_fx_rates to compute
  nominal_amount_sdr (deal value in BIS unit of account).

  Join logic:
    currency match : deals.currency = fx_rates.currency_code
    date match     : rate for the last day of the month prior to value_date
                     e.g. value_date 2023-04-15 -> rate_date 2023-03-31
                     T-SQL: EOMONTH(DATEADD(month, -1, value_date))

  Left join is used so deals with no matching FX rate still appear
  (nominal_amount_sdr will be NULL — visible for investigation).

  Materialisation: view
  Grain: one row per deal (deal_id is unique)
*/

with deals as (

    select * from {{ ref('stg_deals') }}

),

fx_rates as (

    select * from {{ ref('stg_fx_rates') }}

),

joined as (

    select

        -- identifiers
        d.deal_id,

        -- product / instrument
        d.deal_type,

        -- counterparty
        d.cpty_code,
        d.cpty_name,
        d.cpty_country,

        -- dates
        d.trade_date,
        d.value_date,
        d.maturity_date,

        -- currency and amounts (original currency)
        d.currency,
        d.nominal_amount,

        -- interest rate
        d.rate_pct,
        d.rate_decimal,

        -- deal lifecycle
        d.deal_status,

        -- internal classification
        d.portfolio,
        d.book,

        -- fx rate applied (kept for auditability)
        fx.rate_date                               as fx_rate_date,
        fx.sdr_per_unit,

        -- SDR conversion: nominal_amount * sdr_per_unit
        d.nominal_amount * fx.sdr_per_unit         as nominal_amount_sdr,

        -- audit timestamps
        d.created_dt,
        d.last_modified_dt

    from deals d

    left join fx_rates fx
        on  fx.currency_code = d.currency
        and fx.rate_date = eomonth(dateadd(month, -1, d.value_date))

)

select * from joined
