{{ config(materialized="table", schema="CRYPTO_L1_DATA") }}


with cte as (
    select
        gen_key,
        gen_id,
        name,
        symbol,
        slug,
        num_market_pairs,
        date_added,
        tags,
        max_supply,
        circulating_supply,
        total_supply,
        infinite_supply,
        cmc_rank,
        self_reported_circulating_supply,
        self_reported_market_cap,
        tvl_ratio,
        last_updated,
        platform_id,
        ins_date
    from {{ ref("general_data") }}
)

select
    cast(current_timestamp as timestamp) as FACT_DATE,
    gen_key,
    gen_id,
    name,
    symbol,
    slug,
    num_market_pairs,
    date_added,
    tags,
    max_supply,
    circulating_supply,
    total_supply,
    infinite_supply,
    cmc_rank,
    self_reported_circulating_supply,
    self_reported_market_cap,
    tvl_ratio,
    last_updated,
    platform_id,
    ins_date
from cte
