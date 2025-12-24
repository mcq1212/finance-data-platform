import requests
import snowflake.connector
import os
import json
from datetime import datetime
from dotenv import load_dotenv

# 1. 加载环境变量 (安全！不把密码写在代码里)
load_dotenv() 

def fetch_crypto_data():
    """从 CoinGecko 获取 USDT 和 USDC 的数据"""
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        'ids': 'tether,usd-coin', # tether=USDT, usd-coin=USDC
        'vs_currencies': 'usd',
        'include_market_cap': 'true',
        'include_24hr_vol': 'true',
        'include_last_updated_at': 'true'
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        print(f"成功获取数据: {data}")
        return data
    except Exception as e:
        print(f"API 请求失败: {e}")
        return None

def load_to_snowflake(data):
    """把数据写入 Snowflake 的 RAW 层"""
    if not data:
        return

    # 建立连接
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse='COMPUTE_WH',
        database='CRYPTO_RAW',
        schema='BINANCE'
    )
    
    try:
        cs = conn.cursor()
        
        # 2. 确保目标表存在 (如果不存在则自动创建)
        # 我们使用 VARIANT 类型存 JSON，这是 ELT 的最佳实践
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS RAW_MARKET_DATA (
            raw_data VARIANT,
            loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
        cs.execute(create_table_sql)

        # 3. 插入数据
        # 把 Python 字典转成 JSON 字符串存入
        json_data = json.dumps(data)
        insert_sql = f"INSERT INTO RAW_MARKET_DATA (raw_data) SELECT PARSE_JSON('{json_data}')"
        cs.execute(insert_sql)
        
        print("数据成功写入 Snowflake!")
        
    except Exception as e:
        print(f"Snowflake 写入失败: {e}")
    finally:
        cs.close()
        conn.close()

if __name__ == "__main__":
    market_data = fetch_crypto_data()
    load_to_snowflake(market_data)