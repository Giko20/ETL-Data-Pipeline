{{ config(materialized="table", schema="CRYPTO_L2_DATA") }}

select 
    {{ dbt_utils.generate_surrogate_key(['genh.gen_id', 'genh.symbol', 'qh.last_updated']) }} as delinfo_key,
    genh.gen_id,
    genh.symbol as symbol,
    qh.price as price,
    {{ price_change_ndays(1, column="qh.price", partition_by="genh.gen_id", order_by="qh.last_updated") }},
    {{ price_change_ndays(3, column="qh.price", partition_by="genh.gen_id", order_by="qh.last_updated") }},
    {{ price_change_ndays(7, column="qh.price", partition_by="genh.gen_id", order_by="qh.last_updated") }},
    {{ price_change_ndays(14, column="qh.price", partition_by="genh.gen_id", order_by="qh.last_updated") }},
    {{ price_change_ndays(30, column="qh.price", partition_by="genh.gen_id", order_by="qh.last_updated") }},
    {{ price_change_ndays(60, column="qh.price", partition_by="genh.gen_id", order_by="qh.last_updated") }},
    {{ price_change_ndays(90, column="qh.price", partition_by="genh.gen_id", order_by="qh.last_updated") }},
    {{ price_change_ndays(180, column="qh.price", partition_by="genh.gen_id", order_by="qh.last_updated") }},
    {{ price_change_ndays(365, column="qh.price", partition_by="genh.gen_id", order_by="qh.last_updated") }},
    qh.volume_24h as volume_24h,
    qh.percent_change_1h as percent_change_1h,
    qh.percent_change_24h as percent_change_24h,
    qh.percent_change_7d as percent_change_7d,
    qh.percent_change_30d as percent_change_30d,
    qh.percent_change_60d as percent_change_60d,
    qh.percent_change_90d as percent_change_90d,
    qh.market_cap as market_cap,
    qh.market_cap_dominance as market_cap_dominance,
    qh.fully_diluted_market_cap as fully_diluted_market_cap,
    qh.last_updated as last_updated
from {{ ref("gen_data_h") }} genh
    join {{ ref("quotes_data_h") }} qh
        on genh.gen_id = qh.Quote_id
            and qh.last_updated = (
                select max(qh2.last_updated)
                from {{ ref("quotes_data_h") }} qh2
                where qh2.Quote_id = genh.gen_id
            )
where genh.last_updated = (
    select max(genh2.last_updated)
    from {{ ref("gen_data_h") }} genh2
    where genh2.gen_id = genh.gen_id
)
