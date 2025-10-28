-- Staging Profitable Flippers table schema
-- Output of Milestone 4 (M4) - Buyers who profited from recovery

CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.stg_profitable_flippers` (
  crisis_id STRING NOT NULL OPTIONS(description="Crisis event identifier where profit was realized"),
  wallet_address STRING NOT NULL OPTIONS(description="Ethereum wallet address of the profitable flipper"),
  token_address STRING NOT NULL OPTIONS(description="Address of the token that generated profit"),
  buy_price FLOAT64 OPTIONS(description="Average price per token during crisis purchases"),
  peak_recovery_price FLOAT64 OPTIONS(description="Highest price reached during token recovery period"),
  estimated_profit_pct FLOAT64 OPTIONS(description="Estimated profit percentage (e.g., 150.5 for 150.5% gain)"),
  estimated_profit_usd FLOAT64 OPTIONS(description="Estimated profit in USD based on recovery price"),
  buy_timestamp TIMESTAMP OPTIONS(description="Timestamp when crisis purchases occurred"),
  peak_recovery_timestamp TIMESTAMP OPTIONS(description="Timestamp when peak recovery price was reached")
)
OPTIONS (
  description = "Crisis buyers who profited from token recovery"
);
