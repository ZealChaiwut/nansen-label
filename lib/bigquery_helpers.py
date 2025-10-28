"""
BigQuery helper functions for Phoenix Flipper project.
Provides utilities for loading UDFs and common query patterns.
"""
from pathlib import Path


def load_ethereum_udfs():
    """Load Ethereum UDF definitions from lib/ethereum_udfs.sql"""
    lib_dir = Path(__file__).parent
    udf_file = lib_dir / "ethereum_udfs.sql"
    
    with open(udf_file, 'r') as f:
        return f.read()


def create_query_with_udfs(query_sql):
    """Create a complete query with UDFs prepended"""
    udfs = load_ethereum_udfs()
    return f"{udfs}\n\n{query_sql}"


# Common constants for Ethereum analysis
ETHEREUM_CONSTANTS = {
    'UNISWAP_V2_FACTORY': '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
    'UNISWAP_V3_FACTORY': '0x1f98431c8ad98523631ae4a59f267346ea31f984',
    'V2_PAIR_CREATED_TOPIC': '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9',
    'V3_POOL_CREATED_TOPIC': '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118',
    'BASE_TOKENS': {
        '0x6b175474e89094c44da98b954eedeac495271d0f': 'DAI',
        '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': 'USDC',
        '0xdac17f958d2ee523a2206206994597c13d831ec7': 'USDT',
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2': 'WETH',
    }
}
