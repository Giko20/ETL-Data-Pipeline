{{ config(materialized="table", schema="CRYPTO_L1_DATA") }}


with cte as (
    select
        statuslog_key,
        credit_count,
        elapsed,
        error_message,
        notice,
        timestamp,
        total_count
    from {{ ref("statuslog_data") }}
)

select
    cast(current_timestamp as timestamp) as FACT_DATE,
    statuslog_key,
    credit_count,
    elapsed,
    error_message,
    notice,
    timestamp,
    total_count
from cte
