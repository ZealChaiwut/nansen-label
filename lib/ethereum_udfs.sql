-- ===============================================
-- Phoenix Flipper - Ethereum UDFs
-- Reusable functions for processing Ethereum logs
-- ===============================================

-- Extract clean token address from Ethereum log topics
-- Usage: EXTRACT_TOKEN_ADDRESS(topics, 1) for token0, EXTRACT_TOKEN_ADDRESS(topics, 2) for token1
CREATE TEMP FUNCTION EXTRACT_TOKEN_ADDRESS(topics ARRAY<STRING>, topic_index INT64)
RETURNS STRING
AS (
  LOWER(CONCAT('0x', SUBSTR(topics[SAFE_OFFSET(topic_index)], 27, 40)))
);

-- Extract pool/pair address from Uniswap V2 data field
-- Usage: EXTRACT_V2_PAIR_ADDRESS(data)
CREATE TEMP FUNCTION EXTRACT_V2_PAIR_ADDRESS(data STRING)
RETURNS STRING
AS (
  LOWER(CONCAT('0x', SUBSTR(data, 27, 40)))
);

-- Extract pool address from Uniswap V3 data field  
-- Usage: EXTRACT_V3_POOL_ADDRESS(data)
CREATE TEMP FUNCTION EXTRACT_V3_POOL_ADDRESS(data STRING)
RETURNS STRING
AS (
  LOWER(CONCAT('0x', SUBSTR(data, 1+96, 40)))
);

-- Check if address is a known factory
-- Usage: IS_KNOWN_FACTORY(address)
CREATE TEMP FUNCTION IS_KNOWN_FACTORY(address STRING)
RETURNS BOOLEAN
AS (
  LOWER(address) IN (
    '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',  -- Uniswap V2
    '0x1f98431c8ad98523631ae4a59f267346ea31f984'   -- Uniswap V3
  )
);

-- Get DEX protocol name from factory address
-- Usage: GET_DEX_PROTOCOL(address)
CREATE TEMP FUNCTION GET_DEX_PROTOCOL(address STRING)
RETURNS STRING
AS (
  CASE LOWER(address)
    WHEN '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f' THEN 'Uniswap V2'
    WHEN '0x1f98431c8ad98523631ae4a59f267346ea31f984' THEN 'Uniswap V3'
    ELSE 'Unknown'
  END
);
