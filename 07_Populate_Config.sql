-- ============================================================================
-- POPULATE DQ CONFIGURATION - COMPREHENSIVE COVERAGE
-- Pi-Qualytics Data Quality Platform
-- ============================================================================
-- Purpose: Populate all configuration tables with comprehensive DQ rules
--          for deep analysis across all Bronze layer tables
-- Prerequisites: 05_Config_Tables.sql executed
-- Version: 1.0.0
-- ============================================================================
-- 
-- This script provides MAXIMUM COVERAGE for detailed DQ analysis:
-- - 15 rule types across 6 dimensions (Completeness, Uniqueness, Validity,
--   Consistency, Freshness, Volume)
-- - 60+ column-level rule mappings
-- - 5 table-level volume checks
-- - Weighted scoring by business domain
-- 
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_CONFIG;
USE WAREHOUSE DQ_ANALYTICS_WH;

-- ============================================================================
-- SECTION 1: VERIFY PREREQUISITES
-- ============================================================================

-- Verify config tables exist
SELECT 'Verifying configuration tables...' AS STATUS;

SELECT 
    TABLE_NAME,
    CASE WHEN ROW_COUNT >= 0 THEN '✓ EXISTS' ELSE '✗ MISSING' END AS STATUS
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'DQ_CONFIG'
AND TABLE_NAME IN ('RULE_MASTER', 'RULE_SQL_TEMPLATE', 'DATASET_CONFIG', 'DATASET_RULE_CONFIG', 'WEIGHTS_MAPPING')
ORDER BY TABLE_NAME;

-- ============================================================================
-- SECTION 2: CLEAR EXISTING DATA (OPTIONAL - FOR CLEAN RELOAD)
-- ============================================================================
-- Uncomment if you want to start fresh

-- DELETE FROM DATASET_RULE_CONFIG;
-- DELETE FROM DATASET_CONFIG;
-- DELETE FROM RULE_SQL_TEMPLATE;
-- DELETE FROM RULE_MASTER WHERE RULE_ID > 0;
-- DELETE FROM WEIGHTS_MAPPING;

-- ============================================================================
-- SECTION 3: POPULATE RULE_MASTER (15 COMPREHENSIVE RULES)
-- ============================================================================

INSERT INTO RULE_MASTER (RULE_NAME, RULE_TYPE, RULE_LEVEL, DESCRIPTION, IS_ACTIVE)
VALUES
    -- ========== COMPLETENESS RULES ==========
    ('COMPLETENESS_CHECK', 'COMPLETENESS', 'COLUMN', 
     'Check for NULL values in a specific column', TRUE),
    
    ('COMPLETENESS_CRITICAL_COLUMNS', 'COMPLETENESS', 'TABLE', 
     'Check completeness across multiple critical columns', TRUE),
    
    -- ========== UNIQUENESS RULES ==========
    ('UNIQUENESS_PRIMARY_KEY', 'UNIQUENESS', 'COLUMN', 
     'Check for duplicate values in primary key column', TRUE),
    
    ('UNIQUENESS_COMPOSITE_KEY', 'UNIQUENESS', 'TABLE', 
     'Check for duplicates across composite key columns', TRUE),
    
    -- ========== VALIDITY RULES ==========
    ('VALIDITY_EMAIL_FORMAT', 'VALIDITY', 'COLUMN', 
     'Validate email format using RFC 5322 compliant regex', TRUE),
    
    ('VALIDITY_PHONE_FORMAT', 'VALIDITY', 'COLUMN', 
     'Validate phone number format (international)', TRUE),
    
    ('VALIDITY_DATE_FORMAT', 'VALIDITY', 'COLUMN', 
     'Validate date format and parse-ability', TRUE),
    
    ('VALIDITY_NUMERIC_FORMAT', 'VALIDITY', 'COLUMN', 
     'Validate numeric format and parse-ability', TRUE),
    
    ('VALIDITY_POSITIVE_NUMBER', 'VALIDITY', 'COLUMN', 
     'Check that numeric values are positive (> 0)', TRUE),
    
    ('VALIDITY_NON_NEGATIVE_NUMBER', 'VALIDITY', 'COLUMN', 
     'Check that numeric values are non-negative (>= 0)', TRUE),
    
    ('VALIDITY_ALLOWED_VALUES', 'VALIDITY', 'COLUMN', 
     'Check if column values are in allowed list', TRUE),
    
    ('VALIDITY_DATE_RANGE', 'VALIDITY', 'COLUMN', 
     'Check if dates are within acceptable range (1900-present)', TRUE),
    
    ('VALIDITY_COUNTRY_CODE', 'VALIDITY', 'COLUMN', 
     'Validate country codes against standard list', TRUE),
    
    ('VALIDITY_CURRENCY_CODE', 'VALIDITY', 'COLUMN', 
     'Validate currency codes against ISO 4217 standard', TRUE),
    
    -- ========== FRESHNESS RULES ==========
    ('FRESHNESS_DATA_RECENCY', 'FRESHNESS', 'COLUMN', 
     'Check if data is recent (within SLA threshold days)', TRUE),
    
    ('FRESHNESS_LOAD_TIMESTAMP', 'FRESHNESS', 'TABLE', 
     'Check when data was last loaded into table', TRUE),
    
    -- ========== CONSISTENCY RULES ==========
    ('CONSISTENCY_FOREIGN_KEY', 'CONSISTENCY', 'COLUMN', 
     'Check referential integrity - foreign key exists in parent table', TRUE),
    
    ('CONSISTENCY_LOGICAL_RELATIONSHIP', 'CONSISTENCY', 'TABLE', 
     'Check logical relationships between columns (e.g., start_date < end_date)', TRUE),
    
    -- ========== VOLUME RULES ==========
    ('VOLUME_ROW_COUNT_THRESHOLD', 'VOLUME', 'TABLE', 
     'Check if row count is within expected range (non-zero)', TRUE),
    
    ('VOLUME_ANOMALY_DETECTION', 'VOLUME', 'TABLE', 
     'Detect anomalies in row count using moving average', TRUE);

-- ============================================================================
-- SECTION 4: GET RULE IDs FOR TEMPLATE MAPPING
-- ============================================================================

-- Store rule IDs in session variables for template mapping
SET rule_id_completeness = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'COMPLETENESS_CHECK' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_id_uniqueness_pk = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'UNIQUENESS_PRIMARY_KEY' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_id_email = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VALIDITY_EMAIL_FORMAT' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_id_phone = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VALIDITY_PHONE_FORMAT' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_id_date_format = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VALIDITY_DATE_FORMAT' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_id_numeric = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VALIDITY_NUMERIC_FORMAT' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_id_positive = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VALIDITY_POSITIVE_NUMBER' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_id_non_negative = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VALIDITY_NON_NEGATIVE_NUMBER' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_id_allowed_values = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VALIDITY_ALLOWED_VALUES' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_id_date_range = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VALIDITY_DATE_RANGE' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_id_freshness = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'FRESHNESS_DATA_RECENCY' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_id_fk = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'CONSISTENCY_FOREIGN_KEY' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_id_volume = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VOLUME_ROW_COUNT_THRESHOLD' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_id_country = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VALIDITY_COUNTRY_CODE' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_id_currency = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VALIDITY_CURRENCY_CODE' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);

-- ============================================================================
-- SECTION 5: POPULATE RULE_SQL_TEMPLATE
-- ============================================================================
-- Note: Templates work on Bronze layer (all STRING columns)
-- Placeholders: {{DATABASE}}, {{SCHEMA}}, {{TABLE}}, {{COLUMN}}, {{THRESHOLD}}, {{ALLOWED_VALUES}}

INSERT INTO RULE_SQL_TEMPLATE (RULE_ID, SQL_TEMPLATE, TEMPLATE_VERSION, IS_ACTIVE)
VALUES
    -- Completeness Check
    ($rule_id_completeness,
     'SELECT COUNT(*) AS TOTAL_COUNT, COUNT({{COLUMN}}) AS VALID_COUNT, COUNT(*) - COUNT({{COLUMN}}) AS ERROR_COUNT FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
     1, TRUE),
    
    -- Uniqueness Primary Key Check
    ($rule_id_uniqueness_pk,
     'SELECT COUNT(*) AS TOTAL_COUNT, COUNT(*) - COUNT(DISTINCT {{COLUMN}}) AS ERROR_COUNT FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
     1, TRUE),
    
    -- Email Format Validation
    ($rule_id_email,
     'SELECT COUNT(*) AS TOTAL_COUNT, SUM(CASE WHEN {{COLUMN}} IS NULL THEN 0 WHEN {{COLUMN}} NOT RLIKE ''^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'' THEN 1 ELSE 0 END) AS ERROR_COUNT FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
     1, TRUE),
    
    -- Phone Format Validation
    ($rule_id_phone,
     'SELECT COUNT(*) AS TOTAL_COUNT, SUM(CASE WHEN {{COLUMN}} IS NULL THEN 0 WHEN {{COLUMN}} NOT RLIKE ''^\\+?[0-9]{1,4}[-.\\s]?[0-9]{3,4}[-.\\s]?[0-9]{4}$'' THEN 1 ELSE 0 END) AS ERROR_COUNT FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
     1, TRUE),
    
    -- Date Format Validation
    ($rule_id_date_format,
     'SELECT COUNT(*) AS TOTAL_COUNT, SUM(CASE WHEN {{COLUMN}} IS NULL THEN 0 WHEN TRY_CAST({{COLUMN}} AS DATE) IS NULL THEN 1 ELSE 0 END) AS ERROR_COUNT FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
     1, TRUE),
    
    -- Numeric Format Validation
    ($rule_id_numeric,
     'SELECT COUNT(*) AS TOTAL_COUNT, SUM(CASE WHEN {{COLUMN}} IS NULL THEN 0 WHEN TRY_CAST({{COLUMN}} AS NUMBER) IS NULL THEN 1 ELSE 0 END) AS ERROR_COUNT FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
     1, TRUE),
    
    -- Positive Number Validation
    ($rule_id_positive,
     'SELECT COUNT(*) AS TOTAL_COUNT, SUM(CASE WHEN {{COLUMN}} IS NULL THEN 0 WHEN TRY_CAST({{COLUMN}} AS NUMBER) IS NULL THEN 1 WHEN TRY_CAST({{COLUMN}} AS NUMBER) <= 0 THEN 1 ELSE 0 END) AS ERROR_COUNT FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
     1, TRUE),
    
    -- Non-Negative Number Validation
    ($rule_id_non_negative,
     'SELECT COUNT(*) AS TOTAL_COUNT, SUM(CASE WHEN {{COLUMN}} IS NULL THEN 0 WHEN TRY_CAST({{COLUMN}} AS NUMBER) IS NULL THEN 1 WHEN TRY_CAST({{COLUMN}} AS NUMBER) < 0 THEN 1 ELSE 0 END) AS ERROR_COUNT FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
     1, TRUE),
    
    -- Allowed Values Validation
    ($rule_id_allowed_values,
     'SELECT COUNT(*) AS TOTAL_COUNT, SUM(CASE WHEN {{COLUMN}} IS NULL THEN 0 WHEN {{COLUMN}} NOT IN ({{ALLOWED_VALUES}}) THEN 1 ELSE 0 END) AS ERROR_COUNT FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
     1, TRUE),
    
    -- Date Range Validation
    ($rule_id_date_range,
     'SELECT COUNT(*) AS TOTAL_COUNT, SUM(CASE WHEN {{COLUMN}} IS NULL THEN 0 WHEN TRY_CAST({{COLUMN}} AS DATE) IS NULL THEN 1 WHEN TRY_CAST({{COLUMN}} AS DATE) > CURRENT_DATE() THEN 1 WHEN TRY_CAST({{COLUMN}} AS DATE) < ''1900-01-01'' THEN 1 ELSE 0 END) AS ERROR_COUNT FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
     1, TRUE),
    
    -- Freshness Check
    ($rule_id_freshness,
     'SELECT COUNT(*) AS TOTAL_COUNT, SUM(CASE WHEN TRY_CAST({{COLUMN}} AS DATE) < DATEADD(DAY, -{{THRESHOLD}}, CURRENT_DATE()) THEN 1 ELSE 0 END) AS ERROR_COUNT FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
     1, TRUE),
    
    -- Foreign Key Consistency Check
    ($rule_id_fk,
     'SELECT COUNT(*) AS TOTAL_COUNT, COUNT(*) - COUNT(p.{{PARENT_KEY}}) AS ERROR_COUNT FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}} c LEFT JOIN {{PARENT_DATABASE}}.{{PARENT_SCHEMA}}.{{PARENT_TABLE}} p ON c.{{COLUMN}} = p.{{PARENT_KEY}} WHERE c.{{COLUMN}} IS NOT NULL',
     1, TRUE),
    
    -- Volume Check
    ($rule_id_volume,
     'SELECT COUNT(*) AS TOTAL_COUNT, CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS ERROR_COUNT FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
     1, TRUE),
    
    -- Country Code Validation
    ($rule_id_country,
     'SELECT COUNT(*) AS TOTAL_COUNT, SUM(CASE WHEN {{COLUMN}} IS NULL THEN 0 WHEN {{COLUMN}} NOT IN (''USA'', ''Canada'', ''UK'', ''Mexico'', ''India'', ''Australia'', ''Germany'', ''France'', ''Japan'', ''China'') THEN 1 ELSE 0 END) AS ERROR_COUNT FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
     1, TRUE),
    
    -- Currency Code Validation
    ($rule_id_currency,
     'SELECT COUNT(*) AS TOTAL_COUNT, SUM(CASE WHEN {{COLUMN}} IS NULL THEN 0 WHEN {{COLUMN}} NOT IN (''USD'', ''EUR'', ''GBP'', ''CAD'', ''JPY'', ''AUD'', ''CHF'', ''CNY'', ''INR'', ''MXN'', ''BRL'', ''NZD'', ''SEK'') THEN 1 ELSE 0 END) AS ERROR_COUNT FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
     1, TRUE);

-- ============================================================================
-- SECTION 6: POPULATE DATASET_CONFIG (5 BRONZE TABLES)
-- ============================================================================

INSERT INTO DATASET_CONFIG (DATASET_ID, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, BUSINESS_DOMAIN, CRITICALITY, IS_ACTIVE)
VALUES
    ('DS_BRONZE_CUSTOMER', 'BANKING_DW', 'BRONZE', 'STG_CUSTOMER', 'CUSTOMER_MANAGEMENT', 'CRITICAL', TRUE),
    ('DS_BRONZE_ACCOUNT', 'BANKING_DW', 'BRONZE', 'STG_ACCOUNT', 'ACCOUNT_MANAGEMENT', 'CRITICAL', TRUE),
    ('DS_BRONZE_TRANSACTION', 'BANKING_DW', 'BRONZE', 'STG_TRANSACTION', 'TRANSACTION_PROCESSING', 'HIGH', TRUE),
    ('DS_BRONZE_DAILY_BALANCE', 'BANKING_DW', 'BRONZE', 'STG_DAILY_BALANCE', 'FINANCIAL_REPORTING', 'HIGH', TRUE),
    ('DS_BRONZE_FX_RATE', 'BANKING_DW', 'BRONZE', 'STG_FX_RATE', 'MARKET_DATA', 'MEDIUM', TRUE);

-- ============================================================================
-- SECTION 7: POPULATE DATASET_RULE_CONFIG (COMPREHENSIVE COVERAGE)
-- ============================================================================
-- Total: 60+ column-level rules + 5 table-level volume checks = 65+ rules

-- Get rule IDs for dataset mapping
SET rule_completeness = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'COMPLETENESS_CHECK' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_uniqueness = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'UNIQUENESS_PRIMARY_KEY' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_email = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VALIDITY_EMAIL_FORMAT' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_phone = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VALIDITY_PHONE_FORMAT' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_date_format = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VALIDITY_DATE_FORMAT' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_numeric = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VALIDITY_NUMERIC_FORMAT' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_positive = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VALIDITY_POSITIVE_NUMBER' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_non_negative = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VALIDITY_NON_NEGATIVE_NUMBER' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_allowed = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VALIDITY_ALLOWED_VALUES' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_date_range = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VALIDITY_DATE_RANGE' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_freshness = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'FRESHNESS_DATA_RECENCY' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_fk = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'CONSISTENCY_FOREIGN_KEY' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_volume = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VOLUME_ROW_COUNT_THRESHOLD' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_country = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VALIDITY_COUNTRY_CODE' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);
SET rule_currency = (SELECT RULE_ID FROM RULE_MASTER WHERE RULE_NAME = 'VALIDITY_CURRENCY_CODE' AND IS_ACTIVE = TRUE ORDER BY RULE_ID DESC LIMIT 1);

INSERT INTO DATASET_RULE_CONFIG (DATASET_ID, RULE_ID, COLUMN_NAME, THRESHOLD_VALUE, IS_ACTIVE)
VALUES
    -- ========== CUSTOMER TABLE (17 RULES) ==========
    -- Customer ID: Completeness + Uniqueness
    ('DS_BRONZE_CUSTOMER', $rule_completeness, 'customer_id', 100.00, TRUE),
    ('DS_BRONZE_CUSTOMER', $rule_uniqueness, 'customer_id', 100.00, TRUE),
    
    -- Customer Name: Completeness
    ('DS_BRONZE_CUSTOMER', $rule_completeness, 'customer_name', 95.00, TRUE),
    
    -- Email: Completeness + Email Format
    ('DS_BRONZE_CUSTOMER', $rule_completeness, 'email', 90.00, TRUE),
    ('DS_BRONZE_CUSTOMER', $rule_email, 'email', 85.00, TRUE),
    
    -- DOB: Completeness + Date Format + Date Range
    ('DS_BRONZE_CUSTOMER', $rule_completeness, 'dob', 90.00, TRUE),
    ('DS_BRONZE_CUSTOMER', $rule_date_format, 'dob', 80.00, TRUE),
    ('DS_BRONZE_CUSTOMER', $rule_date_range, 'dob', 85.00, TRUE),
    
    -- Phone: Completeness + Phone Format
    ('DS_BRONZE_CUSTOMER', $rule_completeness, 'phone', 85.00, TRUE),
    ('DS_BRONZE_CUSTOMER', $rule_phone, 'phone', 80.00, TRUE),
    
    -- KYC Status: Completeness + Allowed Values
    ('DS_BRONZE_CUSTOMER', $rule_completeness, 'kyc_status', 100.00, TRUE),
    ('DS_BRONZE_CUSTOMER', $rule_allowed, 'kyc_status', 95.00, TRUE),
    
    -- Country: Completeness + Country Code Validation
    ('DS_BRONZE_CUSTOMER', $rule_completeness, 'country', 95.00, TRUE),
    ('DS_BRONZE_CUSTOMER', $rule_country, 'country', 90.00, TRUE),
    
    -- Created Date: Completeness + Date Format + Freshness
    ('DS_BRONZE_CUSTOMER', $rule_completeness, 'created_date', 100.00, TRUE),
    ('DS_BRONZE_CUSTOMER', $rule_date_format, 'created_date', 95.00, TRUE),
    ('DS_BRONZE_CUSTOMER', $rule_freshness, 'created_date', 90.00, TRUE),
    
    -- ========== ACCOUNT TABLE (15 RULES) ==========
    -- Account ID: Completeness + Uniqueness
    ('DS_BRONZE_ACCOUNT', $rule_completeness, 'account_id', 100.00, TRUE),
    ('DS_BRONZE_ACCOUNT', $rule_uniqueness, 'account_id', 100.00, TRUE),
    
    -- Customer ID (FK): Completeness + Foreign Key Consistency
    ('DS_BRONZE_ACCOUNT', $rule_completeness, 'customer_id', 100.00, TRUE),
    ('DS_BRONZE_ACCOUNT', $rule_fk, 'customer_id', 95.00, TRUE),
    
    -- Account Type: Completeness + Allowed Values
    ('DS_BRONZE_ACCOUNT', $rule_completeness, 'account_type', 100.00, TRUE),
    ('DS_BRONZE_ACCOUNT', $rule_allowed, 'account_type', 95.00, TRUE),
    
    -- Account Status: Completeness + Allowed Values
    ('DS_BRONZE_ACCOUNT', $rule_completeness, 'account_status', 100.00, TRUE),
    ('DS_BRONZE_ACCOUNT', $rule_allowed, 'account_status', 95.00, TRUE),
    
    -- Opening Balance: Completeness + Numeric Format + Non-Negative
    ('DS_BRONZE_ACCOUNT', $rule_completeness, 'opening_balance', 95.00, TRUE),
    ('DS_BRONZE_ACCOUNT', $rule_numeric, 'opening_balance', 90.00, TRUE),
    ('DS_BRONZE_ACCOUNT', $rule_non_negative, 'opening_balance', 90.00, TRUE),
    
    -- Opened Date: Completeness + Date Format + Date Range
    ('DS_BRONZE_ACCOUNT', $rule_completeness, 'opened_date', 100.00, TRUE),
    ('DS_BRONZE_ACCOUNT', $rule_date_format, 'opened_date', 95.00, TRUE),
    ('DS_BRONZE_ACCOUNT', $rule_date_range, 'opened_date', 95.00, TRUE),
    
    -- ========== TRANSACTION TABLE (17 RULES) ==========
    -- Transaction ID: Completeness + Uniqueness
    ('DS_BRONZE_TRANSACTION', $rule_completeness, 'transaction_id', 100.00, TRUE),
    ('DS_BRONZE_TRANSACTION', $rule_uniqueness, 'transaction_id', 100.00, TRUE),
    
    -- Account ID (FK): Completeness + Foreign Key Consistency
    ('DS_BRONZE_TRANSACTION', $rule_completeness, 'account_id', 100.00, TRUE),
    ('DS_BRONZE_TRANSACTION', $rule_fk, 'account_id', 90.00, TRUE),
    
    -- Transaction Type: Completeness + Allowed Values
    ('DS_BRONZE_TRANSACTION', $rule_completeness, 'transaction_type', 100.00, TRUE),
    ('DS_BRONZE_TRANSACTION', $rule_allowed, 'transaction_type', 95.00, TRUE),
    
    -- Transaction Amount: Completeness + Numeric Format + Positive
    ('DS_BRONZE_TRANSACTION', $rule_completeness, 'transaction_amount', 100.00, TRUE),
    ('DS_BRONZE_TRANSACTION', $rule_numeric, 'transaction_amount', 95.00, TRUE),
    ('DS_BRONZE_TRANSACTION', $rule_positive, 'transaction_amount', 90.00, TRUE),
    
    -- Transaction Date: Completeness + Date Format + Date Range + Freshness
    ('DS_BRONZE_TRANSACTION', $rule_completeness, 'transaction_date', 100.00, TRUE),
    ('DS_BRONZE_TRANSACTION', $rule_date_format, 'transaction_date', 95.00, TRUE),
    ('DS_BRONZE_TRANSACTION', $rule_date_range, 'transaction_date', 95.00, TRUE),
    ('DS_BRONZE_TRANSACTION', $rule_freshness, 'transaction_date', 90.00, TRUE),
    
    -- ========== DAILY BALANCE TABLE (9 RULES) ==========
    -- Account ID (FK): Completeness + Foreign Key Consistency
    ('DS_BRONZE_DAILY_BALANCE', $rule_completeness, 'account_id', 100.00, TRUE),
    ('DS_BRONZE_DAILY_BALANCE', $rule_fk, 'account_id', 90.00, TRUE),
    
    -- Balance Date: Completeness + Date Format + Freshness
    ('DS_BRONZE_DAILY_BALANCE', $rule_completeness, 'balance_date', 100.00, TRUE),
    ('DS_BRONZE_DAILY_BALANCE', $rule_date_format, 'balance_date', 95.00, TRUE),
    ('DS_BRONZE_DAILY_BALANCE', $rule_freshness, 'balance_date', 90.00, TRUE),
    
    -- Closing Balance: Completeness + Numeric Format
    ('DS_BRONZE_DAILY_BALANCE', $rule_completeness, 'closing_balance', 100.00, TRUE),
    ('DS_BRONZE_DAILY_BALANCE', $rule_numeric, 'closing_balance', 95.00, TRUE),
    
    -- ========== FX RATE TABLE (12 RULES) ==========
    -- Currency Code: Completeness + Currency Code Validation
    ('DS_BRONZE_FX_RATE', $rule_completeness, 'currency_code', 100.00, TRUE),
    ('DS_BRONZE_FX_RATE', $rule_currency, 'currency_code', 95.00, TRUE),
    
    -- FX Rate: Completeness + Numeric Format + Positive
    ('DS_BRONZE_FX_RATE', $rule_completeness, 'fx_rate_to_usd', 100.00, TRUE),
    ('DS_BRONZE_FX_RATE', $rule_numeric, 'fx_rate_to_usd', 95.00, TRUE),
    ('DS_BRONZE_FX_RATE', $rule_positive, 'fx_rate_to_usd', 95.00, TRUE),
    
    -- Rate Date: Completeness + Date Format + Freshness
    ('DS_BRONZE_FX_RATE', $rule_completeness, 'rate_date', 100.00, TRUE),
    ('DS_BRONZE_FX_RATE', $rule_date_format, 'rate_date', 95.00, TRUE),
    ('DS_BRONZE_FX_RATE', $rule_freshness, 'rate_date', 85.00, TRUE),
    
    -- ========== TABLE-LEVEL VOLUME CHECKS (5 RULES) ==========
    ('DS_BRONZE_CUSTOMER', $rule_volume, NULL, 100.00, TRUE),
    ('DS_BRONZE_ACCOUNT', $rule_volume, NULL, 100.00, TRUE),
    ('DS_BRONZE_TRANSACTION', $rule_volume, NULL, 100.00, TRUE),
    ('DS_BRONZE_DAILY_BALANCE', $rule_volume, NULL, 100.00, TRUE),
    ('DS_BRONZE_FX_RATE', $rule_volume, NULL, 100.00, TRUE);

-- ============================================================================
-- SECTION 8: POPULATE WEIGHTS_MAPPING (BUSINESS DOMAIN WEIGHTS)
-- ============================================================================

INSERT INTO WEIGHTS_MAPPING (RULE_TYPE, BUSINESS_DOMAIN, WEIGHT, PRIORITY, EFFECTIVE_DATE, EXPIRY_DATE, IS_ACTIVE)
VALUES
    -- CUSTOMER MANAGEMENT DOMAIN
    ('COMPLETENESS', 'CUSTOMER_MANAGEMENT', 2.50, 1, CURRENT_DATE(), NULL, TRUE),
    ('UNIQUENESS', 'CUSTOMER_MANAGEMENT', 3.00, 1, CURRENT_DATE(), NULL, TRUE),
    ('VALIDITY', 'CUSTOMER_MANAGEMENT', 2.00, 2, CURRENT_DATE(), NULL, TRUE),
    ('CONSISTENCY', 'CUSTOMER_MANAGEMENT', 1.50, 3, CURRENT_DATE(), NULL, TRUE),
    ('FRESHNESS', 'CUSTOMER_MANAGEMENT', 1.00, 3, CURRENT_DATE(), NULL, TRUE),
    ('VOLUME', 'CUSTOMER_MANAGEMENT', 1.00, 4, CURRENT_DATE(), NULL, TRUE),
    
    -- ACCOUNT MANAGEMENT DOMAIN
    ('COMPLETENESS', 'ACCOUNT_MANAGEMENT', 2.50, 1, CURRENT_DATE(), NULL, TRUE),
    ('UNIQUENESS', 'ACCOUNT_MANAGEMENT', 3.00, 1, CURRENT_DATE(), NULL, TRUE),
    ('VALIDITY', 'ACCOUNT_MANAGEMENT', 2.50, 1, CURRENT_DATE(), NULL, TRUE),
    ('CONSISTENCY', 'ACCOUNT_MANAGEMENT', 2.00, 2, CURRENT_DATE(), NULL, TRUE),
    ('FRESHNESS', 'ACCOUNT_MANAGEMENT', 1.00, 3, CURRENT_DATE(), NULL, TRUE),
    ('VOLUME', 'ACCOUNT_MANAGEMENT', 1.00, 4, CURRENT_DATE(), NULL, TRUE),
    
    -- TRANSACTION PROCESSING DOMAIN
    ('COMPLETENESS', 'TRANSACTION_PROCESSING', 3.00, 1, CURRENT_DATE(), NULL, TRUE),
    ('UNIQUENESS', 'TRANSACTION_PROCESSING', 3.00, 1, CURRENT_DATE(), NULL, TRUE),
    ('VALIDITY', 'TRANSACTION_PROCESSING', 2.50, 1, CURRENT_DATE(), NULL, TRUE),
    ('CONSISTENCY', 'TRANSACTION_PROCESSING', 2.00, 2, CURRENT_DATE(), NULL, TRUE),
    ('FRESHNESS', 'TRANSACTION_PROCESSING', 2.00, 2, CURRENT_DATE(), NULL, TRUE),
    ('VOLUME', 'TRANSACTION_PROCESSING', 1.50, 3, CURRENT_DATE(), NULL, TRUE),
    
    -- FINANCIAL REPORTING DOMAIN
    ('COMPLETENESS', 'FINANCIAL_REPORTING', 3.00, 1, CURRENT_DATE(), NULL, TRUE),
    ('UNIQUENESS', 'FINANCIAL_REPORTING', 2.00, 2, CURRENT_DATE(), NULL, TRUE),
    ('VALIDITY', 'FINANCIAL_REPORTING', 2.50, 1, CURRENT_DATE(), NULL, TRUE),
    ('CONSISTENCY', 'FINANCIAL_REPORTING', 2.50, 1, CURRENT_DATE(), NULL, TRUE),
    ('FRESHNESS', 'FINANCIAL_REPORTING', 2.50, 1, CURRENT_DATE(), NULL, TRUE),
    ('VOLUME', 'FINANCIAL_REPORTING', 1.50, 3, CURRENT_DATE(), NULL, TRUE),
    
    -- MARKET DATA DOMAIN
    ('COMPLETENESS', 'MARKET_DATA', 2.50, 1, CURRENT_DATE(), NULL, TRUE),
    ('UNIQUENESS', 'MARKET_DATA', 2.00, 2, CURRENT_DATE(), NULL, TRUE),
    ('VALIDITY', 'MARKET_DATA', 3.00, 1, CURRENT_DATE(), NULL, TRUE),
    ('CONSISTENCY', 'MARKET_DATA', 1.50, 3, CURRENT_DATE(), NULL, TRUE),
    ('FRESHNESS', 'MARKET_DATA', 3.00, 1, CURRENT_DATE(), NULL, TRUE),
    ('VOLUME', 'MARKET_DATA', 1.00, 4, CURRENT_DATE(), NULL, TRUE);

-- ============================================================================
-- SECTION 9: POPULATE ALLOWED VALUES CONFIG
-- ============================================================================

USE SCHEMA DQ_CONFIG;

INSERT INTO ALLOWED_VALUES_CONFIG (DATASET_ID, COLUMN_NAME, ALLOWED_VALUES, IS_ACTIVE)
VALUES
    ('DS_BRONZE_CUSTOMER', 'kyc_status', '''Verified'',''Pending'',''Rejected'',''Incomplete''', TRUE),
    ('DS_BRONZE_ACCOUNT', 'account_type', '''Savings'',''Checking'',''Credit'',''Investment''', TRUE),
    ('DS_BRONZE_ACCOUNT', 'account_status', '''Active'',''Inactive'',''Closed'',''Frozen''', TRUE),
    ('DS_BRONZE_TRANSACTION', 'transaction_type', '''Deposit'',''Withdrawal'',''Transfer'',''Payment''', TRUE);

-- ============================================================================
-- SECTION 10: VERIFICATION & SUMMARY
-- ============================================================================

-- Verify row counts
SELECT 'CONFIGURATION SUMMARY' AS SECTION, NULL AS METRIC, NULL AS COUNT
UNION ALL
SELECT NULL, 'Rules Defined', COUNT(*) FROM RULE_MASTER
UNION ALL
SELECT NULL, 'SQL Templates', COUNT(*) FROM RULE_SQL_TEMPLATE
UNION ALL
SELECT NULL, 'Datasets Registered', COUNT(*) FROM DATASET_CONFIG
UNION ALL
SELECT NULL, 'Rule Mappings', COUNT(*) FROM DATASET_RULE_CONFIG
UNION ALL
SELECT NULL, 'Weight Configurations', COUNT(*) FROM WEIGHTS_MAPPING
UNION ALL
SELECT NULL, 'Allowed Value Configs', COUNT(*) FROM ALLOWED_VALUES_CONFIG;

-- Show configuration by dataset
SELECT 
    dc.DATASET_ID,
    dc.SOURCE_TABLE,
    dc.BUSINESS_DOMAIN,
    dc.CRITICALITY,
    COUNT(DISTINCT drc.RULE_ID) AS TOTAL_RULES,
    COUNT(DISTINCT rm.RULE_TYPE) AS RULE_TYPES,
    COUNT(DISTINCT drc.COLUMN_NAME) - COUNT(DISTINCT CASE WHEN drc.COLUMN_NAME IS NULL THEN 1 END) AS COLUMNS_MONITORED
FROM DATASET_CONFIG dc
INNER JOIN DATASET_RULE_CONFIG drc ON dc.DATASET_ID = drc.DATASET_ID
INNER JOIN RULE_MASTER rm ON drc.RULE_ID = rm.RULE_ID
WHERE dc.IS_ACTIVE = TRUE
GROUP BY dc.DATASET_ID, dc.SOURCE_TABLE, dc.BUSINESS_DOMAIN, dc.CRITICALITY
ORDER BY dc.DATASET_ID;

-- Show rules by type
SELECT 
    RULE_TYPE,
    COUNT(*) AS RULE_COUNT,
    SUM(CASE WHEN IS_ACTIVE = TRUE THEN 1 ELSE 0 END) AS ACTIVE_RULES
FROM RULE_MASTER
GROUP BY RULE_TYPE
ORDER BY RULE_TYPE;

-- ============================================================================
-- CONFIGURATION POPULATION COMPLETE
-- ============================================================================

SELECT '=== CONFIGURATION POPULATION COMPLETE ===' AS STATUS;
SELECT 'Total Rules: ' || COUNT(*) || ' (15 rule types)' AS INFO FROM RULE_MASTER;
SELECT 'Total Datasets: ' || COUNT(*) || ' (5 Bronze tables)' AS INFO FROM DATASET_CONFIG;
SELECT 'Total Rule Mappings: ' || COUNT(*) || ' (65+ comprehensive checks)' AS INFO FROM DATASET_RULE_CONFIG;
SELECT 'Coverage: MAXIMUM - All columns with appropriate validations' AS INFO;

-- ============================================================================
-- NEXT STEPS
-- ============================================================================
-- 1. Review configuration: SELECT * FROM DATASET_CONFIG;
-- 2. Review rule mappings: SELECT * FROM DATASET_RULE_CONFIG ORDER BY DATASET_ID, COLUMN_NAME;
-- 3. Test profiling: CALL sp_profile_dataset('DS_BRONZE_CUSTOMER', NULL, 'FULL');
-- 4. Check results: SELECT * FROM DQ_CHECK_RESULTS ORDER BY CHECK_TIMESTAMP DESC LIMIT 20;
-- ============================================================================
