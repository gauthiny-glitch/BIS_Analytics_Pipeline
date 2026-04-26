/*
  stg_fx_rates
  ============
  Staging model for src_fx_rates (monthly SDR exchange rate reference).

  What this model does:
    1. Selects from the raw source table via the dbt source() macro
    2. Casts rate_date string to DATE
    3. Keeps currency_code and sdr_per_unit as-is (already clean)
    4. No rows filtered or aggregated — staging is type-safe only

  Materialisation: view (no data stored, just a query alias)
  Grain: one row per (rate_date, currency_code) — unique
*/

with source as (

    select * from {{ source('bis_raw', 'src_fx_rates') }}

),

staged as (

    select

        -- date: cast from string YYYY-MM-DD to DATE
        try_cast(rate_date as date) as rate_date,

        -- currency identifier
        currency_code,

        -- SDR rate: 1 unit of currency_code = sdr_per_unit SDR
        sdr_per_unit

    from source

)

select * from staged
