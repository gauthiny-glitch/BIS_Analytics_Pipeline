/*
  dim_currency
  ============
  Dimension table: one row per currency in the BIS deposit book.
  Grain: currency (unique).

  Static lookup — hardcoded because the 10 currencies are fixed
  by the composition of the simulated deposit book.

  is_sdr_basket flag:
    [CONFIRMED] SDR basket currencies: USD, EUR, GBP, JPY, CNY — IMF
    AUD, CAD, CHF, SGD, KRW are present in the deposit book
    but are not SDR basket currencies.

  Materialisation: table
*/

{{
    config(
        materialized = 'view'
    )
}}

with currencies as (

    select *
    from (
        values
            ('USD', 'US Dollar',          'Y'),
            ('EUR', 'Euro',               'Y'),
            ('GBP', 'British Pound',      'Y'),
            ('JPY', 'Japanese Yen',       'Y'),
            ('CNY', 'Chinese Renminbi',   'Y'),
            ('CHF', 'Swiss Franc',        'N'),
            ('AUD', 'Australian Dollar',  'N'),
            ('CAD', 'Canadian Dollar',    'N'),
            ('SGD', 'Singapore Dollar',   'N'),
            ('KRW', 'South Korean Won',   'N')
    ) as t (currency, currency_name, is_sdr_basket)

)

select * from currencies
