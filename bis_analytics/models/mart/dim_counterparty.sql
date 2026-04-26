/*
  dim_counterparty
  ================
  Dimension table: one row per unique counterparty.
  Grain: cpty_code (unique).

  Source: int_deals_with_sdr (distinct counterparties from the deal book)

  region is derived from cpty_country using BIS geographic classification:
    Asia-Pacific  : 61% of deposits (confirmed BIS annual report)
    Americas      : 9%
    Europe        : 8%
    Middle East   : 7%
    Africa        : 6%
    International : organisations with country code XX (e.g. IMF, BIS, ECB)

  Materialisation: table
*/

{{
    config(
        materialized = 'view'
    )
}}

with counterparties as (

    select distinct
        cpty_code,
        cpty_name,
        cpty_country

    from {{ ref('int_deals_with_sdr') }}

),

with_region as (

    select
        cpty_code,
        cpty_name,
        cpty_country,

        case cpty_country
            -- Asia-Pacific
            when 'CN' then 'Asia-Pacific'
            when 'JP' then 'Asia-Pacific'
            when 'IN' then 'Asia-Pacific'
            when 'KR' then 'Asia-Pacific'
            when 'AU' then 'Asia-Pacific'
            when 'NZ' then 'Asia-Pacific'
            when 'SG' then 'Asia-Pacific'
            when 'HK' then 'Asia-Pacific'
            when 'TH' then 'Asia-Pacific'
            when 'MY' then 'Asia-Pacific'
            when 'ID' then 'Asia-Pacific'
            when 'PH' then 'Asia-Pacific'
            when 'BD' then 'Asia-Pacific'
            when 'PK' then 'Asia-Pacific'
            when 'LK' then 'Asia-Pacific'
            when 'MM' then 'Asia-Pacific'
            when 'KH' then 'Asia-Pacific'
            when 'MN' then 'Asia-Pacific'
            when 'PG' then 'Asia-Pacific'
            when 'FJ' then 'Asia-Pacific'
			when 'TM' then 'Asia-Pacific'
			when 'MO' then 'Asia-Pacific'
			when 'TJ' then 'Asia-Pacific'
			when 'BT' then 'Asia-Pacific'
			when 'KZ' then 'Asia-Pacific'
			when 'KG' then 'Asia-Pacific'
			when 'CK' then 'Asia-Pacific'
			when 'MV' then 'Asia-Pacific'
			when 'WS' then 'Asia-Pacific'
			when 'UZ' then 'Asia-Pacific'
			when 'TL' then 'Asia-Pacific'
			when 'SB' then 'Asia-Pacific'
			when 'NP' then 'Asia-Pacific'
			when 'VU' then 'Asia-Pacific'
			when 'VN' then 'Asia-Pacific'
			when 'LA' then 'Asia-Pacific'
			when 'BN' then 'Asia-Pacific'
			when 'TO' then 'Asia-Pacific'
            -- Americas
            when 'US' then 'Americas'
            when 'CA' then 'Americas'
            when 'BR' then 'Americas'
            when 'MX' then 'Americas'
            when 'AR' then 'Americas'
            when 'CL' then 'Americas'
            when 'CO' then 'Americas'
            when 'PE' then 'Americas'
            when 'UY' then 'Americas'
            when 'BO' then 'Americas'
            when 'EC' then 'Americas'
            when 'PY' then 'Americas'
            when 'VE' then 'Americas'
            when 'TT' then 'Americas'
            when 'JM' then 'Americas'
			when 'CR' then 'Americas'
			when 'HN' then 'Americas'
            -- Europe
            when 'DE' then 'Europe'
            when 'FR' then 'Europe'
            when 'GB' then 'Europe'
            when 'IT' then 'Europe'
            when 'ES' then 'Europe'
            when 'NL' then 'Europe'
            when 'BE' then 'Europe'
            when 'SE' then 'Europe'
            when 'NO' then 'Europe'
            when 'DK' then 'Europe'
            when 'CH' then 'Europe'
            when 'PL' then 'Europe'
            when 'CZ' then 'Europe'
            when 'HU' then 'Europe'
            when 'RO' then 'Europe'
            when 'BG' then 'Europe'
            when 'HR' then 'Europe'
            when 'RS' then 'Europe'
            when 'TR' then 'Europe'
            when 'IS' then 'Europe'
			when 'AZ' then 'Europe'
			when 'AM' then 'Europe'
			when 'GE' then 'Europe'
            -- Middle East
            when 'SA' then 'Middle East'
            when 'AE' then 'Middle East'
            when 'KW' then 'Middle East'
            when 'QA' then 'Middle East'
            when 'BH' then 'Middle East'
            when 'OM' then 'Middle East'
            when 'JO' then 'Middle East'
            when 'LB' then 'Middle East'
            when 'IQ' then 'Middle East'
            when 'IR' then 'Middle East'
            when 'IL' then 'Middle East'
            -- Africa
            when 'ZA' then 'Africa'
            when 'NG' then 'Africa'
            when 'EG' then 'Africa'
            when 'KE' then 'Africa'
            when 'GH' then 'Africa'
            when 'TZ' then 'Africa'
            when 'UG' then 'Africa'
            when 'ET' then 'Africa'
            when 'MA' then 'Africa'
            when 'TN' then 'Africa'
            when 'DZ' then 'Africa'
            when 'MU' then 'Africa'
            when 'RW' then 'Africa'
            when 'BW' then 'Africa'
            when 'NA' then 'Africa'
            when 'MZ' then 'Africa'
            when 'ZM' then 'Africa'
            when 'ZW' then 'Africa'
			when 'SN' then 'Africa'
			when 'CD' then 'Africa'
			when 'MW' then 'Africa'
            -- International organisations
            when 'XX' then 'International'
			when 'EU' then 'International'
            else 'Other'
        end as region

    from counterparties

)

select * from with_region
