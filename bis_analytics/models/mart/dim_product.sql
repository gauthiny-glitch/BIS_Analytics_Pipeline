/*
  dim_product
  ===========
  Dimension table: one row per BIS Banking Department product type.
  Grain: deal_type (unique).

  Static lookup — hardcoded because there are only 4 product types
  and they are defined by BIS internal instrument conventions.

  deal_type codes follow the Findur convention: INSTRUMENT_DEP
  where DEP = Deposit.

  [CONFIRMED] Product types: FIXBIS, MTI, Sight, Notice — BIS annual report
  [CONFIRMED] All are currency deposits (borrowed funds) — BIS annual report

  Materialisation: table
*/

{{
    config(
        materialized = 'view'
    )
}}

with products as (

    select *
    from (
        values
            ('FIXBIS_DEP', 'FIXBIS',  'Fixed-rate BIS deposit. Fixed term, fixed interest rate. Most common product.'),
            ('MTI_DEP',    'MTI',     'Medium-term instrument. Longer tenor fixed-rate deposit, typically 1-10 years.'),
            ('SIGHT_DEP',  'Sight',   'Sight account. Overnight / on-demand deposit. No fixed maturity.'),
            ('NOTICE_DEP', 'Notice',  'Notice account. Deposit redeemable with advance notice period.')
    ) as t (deal_type, product_name, product_description)

)

select * from products
