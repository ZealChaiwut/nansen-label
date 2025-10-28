-- Crisis Events with Window table schema
-- Output of Milestone 2 (M2) - Crisis events with their contrarian buy windows

CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.crisis_events_with_window` (
  crisis_id STRING NOT NULL OPTIONS(description="Unique identifier for this specific crisis event"),
  token_address STRING NOT NULL OPTIONS(description="Address of the token that experienced the crisis"),
  crisis_date DATE NOT NULL OPTIONS(description="Date when the crisis event was identified"),
  crisis_name STRING OPTIONS(description="Human-readable name describing the crisis event"),
  window_start_date DATE NOT NULL OPTIONS(description="Start date of the contrarian buy window for this crisis"),
  window_end_date DATE NOT NULL OPTIONS(description="End date of the contrarian buy window for this crisis"),
  dt DATE OPTIONS(description="Partition date for efficient querying")
)
PARTITION BY dt
OPTIONS (
  description = "Identified crisis events with defined contrarian buy windows"
);
