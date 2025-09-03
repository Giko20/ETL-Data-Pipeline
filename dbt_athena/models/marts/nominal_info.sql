{{ config(materialized="table", schema="CRYPTO_L2_DATA") }}

select
    {{ dbt_utils.generate_surrogate_key(['gen_data_h.gen_id', 'quotes_data_h.last_updated']) }} as nominal_info_key,
    gen_data_h.gen_id,
    gen_data_h.name,
    gen_data_h.symbol,
    gen_data_h.date_added,
    gen_data_h.circulating_supply,
    quotes_data_h.price,
    quotes_data_h.volume_24h
from {{ ref("gen_data_h") }}
    join {{ ref("quotes_data_h") }}
        on gen_data_h.gen_id = quotes_data_h.Quote_id
