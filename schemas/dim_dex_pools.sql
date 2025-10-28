-- Dim DEX Pools table schema
-- Contains metadata about DEX liquidity pools

CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.dim_dex_pools` (
  pool_address STRING NOT NULL OPTIONS(description="Unique address identifier for the DEX pool contract"),
  pool_name STRING OPTIONS(description="Human-readable name of the pool (e.g., 'USDC-WETH Pool')"),
  token0_address STRING NOT NULL OPTIONS(description="Contract address of the first token in the pair"),
  token0_symbol STRING OPTIONS(description="Symbol/ticker of the first token (e.g., 'USDC')"),
  token1_address STRING NOT NULL OPTIONS(description="Contract address of the second token in the pair"),
  token1_symbol STRING OPTIONS(description="Symbol/ticker of the second token (e.g., 'WETH')"),
  dex_protocol STRING OPTIONS(description="Name of the DEX protocol (e.g., 'Uniswap V3', 'Balancer')"),
  chain STRING NOT NULL OPTIONS(description="Blockchain network where the pool exists (e.g., 'ethereum', 'arbitrum')")
)
OPTIONS (
  description = "DEX pool metadata including token pairs and protocols"
);
