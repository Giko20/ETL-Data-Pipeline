{{ config(materialized="table", schema="CRYPTO_L1_DATA") }}


with cte as (
    select
        quote_key,
        Quote_Id,
        Price,
        Volume_24h,
        Volume_Change_24h,
        Percent_Change_1h,
        Percent_Change_24h,
        Percent_Change_7d,
        Percent_change_30d,
        Percent_change_60d,
        Percent_change_90d,
        Market_cap,
        Market_cap_dominance,
        Fully_diluted_market_cap,
        Tvl,
        Last_updated,
        INS_DATE
    from {{ ref("quote_data") }}
    where INS_DATE = (select max(INS_DATE) from {{ ref("quote_data") }} )
)

select
    cast(current_timestamp as timestamp) as FACT_DATE,
    quote_key,
    Quote_Id,
    Price,
    Volume_24h,
    Volume_Change_24h
    Percent_Change_1h,
    Percent_Change_24h,
    Percent_Change_7d,
    Percent_change_30d,
    Percent_change_60d,
    Percent_change_90d,
    Market_cap,
    Market_cap_dominance,
    Fully_diluted_market_cap,
    Tvl,
    Last_updated,
    INS_DATE
from cte
