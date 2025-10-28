-- Dim Wallet Labels table schema
-- Final output table with Phoenix Flipper labels

CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.dim_wallet_labels` (
  wallet_address STRING NOT NULL OPTIONS(description="Ethereum wallet address being labeled"),
  label_value STRING NOT NULL OPTIONS(description="Specific label value (e.g., 'phoenix_flipper', 'elite_flipper')"),
  num_crisis_events INT64 OPTIONS(description="Total number of crisis events this wallet participated in"),
  total_profited_crises INT64 OPTIONS(description="Number of crisis events where wallet made profit"),
  success_rate FLOAT64 OPTIONS(description="Success rate as decimal (e.g., 0.75 for 75% success rate)"),
  total_pnl_pct FLOAT64 OPTIONS(description="Total profit/loss percentage across all crisis events"),
  total_pnl_usdt FLOAT64 OPTIONS(description="Total profit/loss amount in USDT across all crisis events"),
  last_updated_timestamp TIMESTAMP NOT NULL OPTIONS(description="When this label was last updated")
)
OPTIONS (
  description = "Final wallet labels identifying Phoenix Flippers"
);
