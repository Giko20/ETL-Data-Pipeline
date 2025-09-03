select  {{ dbt_utils.generate_surrogate_key(['Quote_Id', 'Last_updated']) }} as quote_key,
        Quote_Id,
        Price,
        Volume_24h,
        volume_change_24h,
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
from crypto_database.quote
where ins_date = (select max(ins_date) from crypto_database.quote)