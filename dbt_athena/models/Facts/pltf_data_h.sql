{{ config(materialized="table", schema="CRYPTO_L1_DATA") }}


with cte as (
    select
        Platform_Key,
        Platform_Id,
        Name,
        Symbol,
        Slug,
        Token_address,
        INS_DATE
    from {{ ref("platform_data") }}
)

select
    cast(current_timestamp as timestamp) as FACT_DATE,
    Platform_Key
    Platform_Id,
    Name,
    Symbol,
    Slug,
    Token_address,
    INS_DATE
from cte
