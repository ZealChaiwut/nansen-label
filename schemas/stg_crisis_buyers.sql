-- Staging Crisis Buyers table schema
-- Output of Milestone 3 (M3) - Wallets that bought during crisis window

CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.stg_crisis_buyers` (
  crisis_id STRING NOT NULL OPTIONS(description="Crisis event identifier this buyer participated in"),
  wallet_address STRING NOT NULL OPTIONS(description="Ethereum wallet address of the buyer"),
  token_address STRING NOT NULL OPTIONS(description="Address of the token that was purchased during crisis"),
  first_buy_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the first purchase during the crisis window"),
  first_buy_price FLOAT64 OPTIONS(description="Price per token at the time of first purchase"),
  total_amount_bought FLOAT64 OPTIONS(description="Total quantity of tokens bought during the entire crisis window"),
  total_usd_spent FLOAT64 OPTIONS(description="Total USD value spent buying tokens during crisis window"),
  num_transactions INT64 OPTIONS(description="Number of separate buy transactions during the crisis window")
)
OPTIONS (
  description = "Wallets that purchased tokens during identified crisis windows"
);
