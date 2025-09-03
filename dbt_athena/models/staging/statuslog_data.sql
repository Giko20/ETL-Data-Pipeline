select  {{ dbt_utils.generate_surrogate_key(['timestamp', 'error_message', 'credit_count', 'elapsed', 'notice']) }} as statuslog_key,
        credit_count,
        elapsed,
        error_message,
        notice,
        timestamp,
        total_count
from crypto_database.statuslog
where timestamp = (select max(timestamp) from crypto_database.statuslog)