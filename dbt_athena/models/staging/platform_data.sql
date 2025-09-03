select  {{ dbt_utils.generate_surrogate_key(['Platform_Id', 'Name', 'INS_DATE']) }} as Platform_Key,
        Platform_Id,
        Name,
        Symbol,
        Slug,
        Token_address,
        INS_DATE
from crypto_database.platform
where ins_date = (select max(ins_date) from crypto_database.platform)