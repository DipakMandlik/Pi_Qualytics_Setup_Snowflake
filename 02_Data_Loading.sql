-- ============================================================================
-- DATA LOADING INTO BRONZE LAYER
-- Pi-Qualytics Data Quality Platform
-- ============================================================================
-- Purpose: Load raw CSV data into Bronze layer staging tables
-- Prerequisites: 01_Environment_Setup.sql must be executed first
-- Version: 1.0.0
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE BANKING_DW;
USE SCHEMA BRONZE;
USE WAREHOUSE DQ_INGESTION_WH;

-- ============================================================================
-- SECTION 1: PRE-LOAD VALIDATION
-- ============================================================================

-- Verify stage exists and contains files
LIST @CSV_STAGE;

-- Check current row counts (before loading)
SELECT 'STG_CUSTOMER' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM STG_CUSTOMER
UNION ALL
SELECT 'STG_ACCOUNT', COUNT(*) FROM STG_ACCOUNT
UNION ALL
SELECT 'STG_TRANSACTION', COUNT(*) FROM STG_TRANSACTION
UNION ALL
SELECT 'STG_DAILY_BALANCE', COUNT(*) FROM STG_DAILY_BALANCE
UNION ALL
SELECT 'STG_FX_RATE', COUNT(*) FROM STG_FX_RATE;

-- ============================================================================
-- SECTION 2: TRUNCATE TABLES (OPTIONAL - FOR RELOAD)
-- ============================================================================
-- Uncomment if you want to reload data from scratch

-- TRUNCATE TABLE STG_CUSTOMER;
-- TRUNCATE TABLE STG_ACCOUNT;
-- TRUNCATE TABLE STG_TRANSACTION;
-- TRUNCATE TABLE STG_DAILY_BALANCE;
-- TRUNCATE TABLE STG_FX_RATE;

-- ============================================================================
-- SECTION 3: LOAD CUSTOMER DATA
-- ============================================================================

COPY INTO STG_CUSTOMER (
    customer_id,
    customer_name,
    email,
    dob,
    phone,
    kyc_status,
    country,
    created_date,
    source_file,
    load_batch_id
)
FROM (
    SELECT
        $1,                                         -- customer_id
        $2,                                         -- customer_name
        $3,                                         -- email
        $4,                                         -- dob
        $5,                                         -- phone
        $6,                                         -- kyc_status
        $7,                                         -- country
        $8,                                         -- created_date
        METADATA$FILENAME,                          -- source_file
        'BATCH_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS') -- load_batch_id
    FROM @CSV_STAGE/customer_low_quality_last_next_90_days.csv
)
FILE_FORMAT = (FORMAT_NAME = CSV_FILE_FORMAT)
ON_ERROR = 'CONTINUE'                               -- Continue loading even if errors occur
FORCE = TRUE;                                       -- Reload files even if already loaded

-- Check load results
SELECT 
    'STG_CUSTOMER' AS table_name,
    COUNT(*) AS rows_loaded,
    COUNT(DISTINCT load_batch_id) AS batch_count,
    MIN(load_timestamp) AS first_load,
    MAX(load_timestamp) AS last_load
FROM STG_CUSTOMER;

-- ============================================================================
-- SECTION 4: LOAD ACCOUNT DATA
-- ============================================================================

COPY INTO STG_ACCOUNT (
    account_id,
    customer_id,
    account_type,
    account_status,
    opening_balance,
    opened_date,
    source_file,
    load_batch_id
)
FROM (
    SELECT
        $1,                                         -- account_id
        $2,                                         -- customer_id
        $3,                                         -- account_type
        $4,                                         -- account_status
        $5,                                         -- opening_balance
        $6,                                         -- opened_date
        METADATA$FILENAME,                          -- source_file
        'BATCH_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS') -- load_batch_id
    FROM @CSV_STAGE/STG_ACCOUNT.csv
)
FILE_FORMAT = (FORMAT_NAME = CSV_FILE_FORMAT)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Check load results
SELECT 
    'STG_ACCOUNT' AS table_name,
    COUNT(*) AS rows_loaded,
    COUNT(DISTINCT load_batch_id) AS batch_count,
    MIN(load_timestamp) AS first_load,
    MAX(load_timestamp) AS last_load
FROM STG_ACCOUNT;

-- ============================================================================
-- SECTION 5: LOAD TRANSACTION DATA
-- ============================================================================

COPY INTO STG_TRANSACTION (
    transaction_id,
    account_id,
    transaction_type,
    transaction_amount,
    transaction_date,
    source_file,
    load_batch_id
)
FROM (
    SELECT
        $1,                                         -- transaction_id
        $2,                                         -- account_id
        $3,                                         -- transaction_type
        $4,                                         -- transaction_amount
        $5,                                         -- transaction_date
        METADATA$FILENAME,                          -- source_file
        'BATCH_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS') -- load_batch_id
    FROM @CSV_STAGE/Transaction_03-01-2026.csv
)
FILE_FORMAT = (FORMAT_NAME = CSV_FILE_FORMAT)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Check load results
SELECT 
    'STG_TRANSACTION' AS table_name,
    COUNT(*) AS rows_loaded,
    COUNT(DISTINCT load_batch_id) AS batch_count,
    MIN(load_timestamp) AS first_load,
    MAX(load_timestamp) AS last_load
FROM STG_TRANSACTION;

-- ============================================================================
-- SECTION 6: LOAD DAILY BALANCE DATA
-- ============================================================================

COPY INTO STG_DAILY_BALANCE (
    account_id,
    balance_date,
    closing_balance,
    source_file,
    load_batch_id
)
FROM (
    SELECT
        $1,                                         -- account_id
        $2,                                         -- balance_date
        $3,                                         -- closing_balance
        METADATA$FILENAME,                          -- source_file
        'BATCH_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS') -- load_batch_id
    FROM @CSV_STAGE/Daily_Balance_03-01-2026.csv
)
FILE_FORMAT = (FORMAT_NAME = CSV_FILE_FORMAT)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Check load results
SELECT 
    'STG_DAILY_BALANCE' AS table_name,
    COUNT(*) AS rows_loaded,
    COUNT(DISTINCT load_batch_id) AS batch_count,
    MIN(load_timestamp) AS first_load,
    MAX(load_timestamp) AS last_load
FROM STG_DAILY_BALANCE;

-- ============================================================================
-- SECTION 7: LOAD FX RATE DATA
-- ============================================================================

COPY INTO STG_FX_RATE (
    currency_code,
    fx_rate_to_usd,
    rate_date,
    source_file,
    load_batch_id
)
FROM (
    SELECT
        $1,                                         -- currency_code
        $2,                                         -- fx_rate_to_usd
        $3,                                         -- rate_date
        METADATA$FILENAME,                          -- source_file
        'BATCH_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS') -- load_batch_id
    FROM @CSV_STAGE/fx_rate_low_quality_last_next_90_days.csv
)
FILE_FORMAT = (FORMAT_NAME = CSV_FILE_FORMAT)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Check load results
SELECT 
    'STG_FX_RATE' AS table_name,
    COUNT(*) AS rows_loaded,
    COUNT(DISTINCT load_batch_id) AS batch_count,
    MIN(load_timestamp) AS first_load,
    MAX(load_timestamp) AS last_load
FROM STG_FX_RATE;

-- ============================================================================
-- SECTION 8: POST-LOAD VALIDATION
-- ============================================================================

-- Summary of all loaded tables
SELECT 
    'STG_CUSTOMER' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT customer_id) AS unique_keys,
    COUNT(DISTINCT load_batch_id) AS batch_count
FROM STG_CUSTOMER
UNION ALL
SELECT 
    'STG_ACCOUNT',
    COUNT(*),
    COUNT(DISTINCT account_id),
    COUNT(DISTINCT load_batch_id)
FROM STG_ACCOUNT
UNION ALL
SELECT 
    'STG_TRANSACTION',
    COUNT(*),
    COUNT(DISTINCT transaction_id),
    COUNT(DISTINCT load_batch_id)
FROM STG_TRANSACTION
UNION ALL
SELECT 
    'STG_DAILY_BALANCE',
    COUNT(*),
    COUNT(DISTINCT account_id || balance_date),
    COUNT(DISTINCT load_batch_id)
FROM STG_DAILY_BALANCE
UNION ALL
SELECT 
    'STG_FX_RATE',
    COUNT(*),
    COUNT(DISTINCT currency_code || rate_date),
    COUNT(DISTINCT load_batch_id)
FROM STG_FX_RATE
ORDER BY table_name;

-- Sample data verification
SELECT 'CUSTOMER SAMPLE' AS dataset, * FROM STG_CUSTOMER LIMIT 5;
SELECT 'ACCOUNT SAMPLE' AS dataset, * FROM STG_ACCOUNT LIMIT 5;
SELECT 'TRANSACTION SAMPLE' AS dataset, * FROM STG_TRANSACTION LIMIT 5;
SELECT 'DAILY BALANCE SAMPLE' AS dataset, * FROM STG_DAILY_BALANCE LIMIT 5;
SELECT 'FX RATE SAMPLE' AS dataset, * FROM STG_FX_RATE LIMIT 5;

-- Check for load errors (if any)
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'STG_CUSTOMER',
    START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
));

-- ============================================================================
-- DATA LOADING COMPLETE
-- ============================================================================
-- Next Steps:
-- 1. Review load statistics above
-- 2. Execute 03_Silver_Layer_Setup.sql to create cleansed tables
-- 3. Execute transformation procedures to populate Silver layer
-- ============================================================================
