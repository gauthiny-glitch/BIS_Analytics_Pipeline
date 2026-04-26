/*
  stg_deals
  =========
  Staging model for src_deals (Findur deal extract).

  What this model does:
    1. Selects from the raw source table via the dbt source() macro
    2. Casts all date strings to DATE
    3. Casts timestamp strings to DATETIME2
    4. Casts nominal_amount to DECIMAL(20, 2)
    5. Casts rate_pct to DECIMAL(10, 6) — keeps original percentage value
    6. Derives rate_decimal = rate_pct / 100  (e.g. 2.50 → 0.025000)
       Used in downstream interest calculations
    7. No rows are filtered or aggregated here — staging is type-safe only

  Materialisation: view (no data stored, just a query alias)
  Grain: one row per deal (deal_id is unique)
*/

with source as (

    select * from {{ source('bis_raw', 'src_deals') }}

),

staged as (

    select

        -- identifiers
        deal_id,

        -- product / instrument
        deal_type,

        -- counterparty
        cpty_code,
        cpty_name,
        cpty_country,

        -- dates: cast from string YYYY-MM-DD to DATE
        try_cast(trade_date    as date) as trade_date,
        try_cast(value_date    as date) as value_date,
        try_cast(maturity_date as date) as maturity_date,

        -- currency and amounts
        currency,
        try_cast(nominal_amount as decimal(20, 2))    as nominal_amount,

        -- interest rate: keep percentage form AND derive decimal form
        try_cast(rate_pct as decimal(10, 6))          as rate_pct,
        try_cast(rate_pct as decimal(10, 6)) / 100.0  as rate_decimal,

        -- deal lifecycle
        deal_status,

        -- internal classification
        portfolio,
        book,

        -- audit timestamps: cast from string to DATETIME2
        try_cast(created_dt       as datetime2) as created_dt,
        try_cast(last_modified_dt as datetime2) as last_modified_dt

    from source

)

select * from staged
