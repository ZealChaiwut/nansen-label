-- Dim Token Price History table schema
-- Contains daily price and market data for tokens

CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.dim_token_price_history` (
  token_address STRING NOT NULL OPTIONS(description="Contract address of the token"),
  price_usd FLOAT64 OPTIONS(description="Token price in USD at end of day"),
  volume_24h_usd FLOAT64 OPTIONS(description="24-hour trading volume in USD"),
  market_cap_usd FLOAT64 OPTIONS(description="Market capitalization in USD"),
  price_change_24h_pct FLOAT64 OPTIONS(description="24-hour price change percentage (e.g., -15.5 for 15.5% drop)"),
  liquidity_usd FLOAT64 OPTIONS(description="Total liquidity available across DEX pools in USD"),
  high_24h_usd FLOAT64 OPTIONS(description="24-hour high price in USD"),
  low_24h_usd FLOAT64 OPTIONS(description="24-hour low price in USD"),
  dt DATE OPTIONS(description="Date of the price record for partitioning")
)
PARTITION BY dt
CLUSTER BY token_address, dt
OPTIONS (
  description = "Daily price and market data history for tokens"
);
