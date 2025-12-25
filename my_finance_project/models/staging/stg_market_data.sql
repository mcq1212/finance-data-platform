select 
    -- 解析 JSON
    raw_data:tether:usd::float as usdt_price,
    raw_data:tether:usd_market_cap::float as usdt_market_cap,
    raw_data:"usd-coin":usd::float as usdc_price,
    loaded_at
from {{ source('crypto_raw', 'raw_market_data') }} 