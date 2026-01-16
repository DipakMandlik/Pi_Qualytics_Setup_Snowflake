-- =====================================================================================
--                   DATA QUALITY CONFIG TABLES - COMPLETE SETUP
-- =====================================================================================
-- This script creates and populates ALL configuration tables from scratch
-- =====================================================================================

USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_CONFIG;
USE WAREHOUSE DQ_ANALYTICS_WH;

-- =====================================================================================
-- STEP 1: CREATE CONFIGURATION TABLES
-- =====================================================================================

-- Create Rule Master table
CREATE OR REPLACE TABLE rule_master (
    rule_id                     NUMBER AUTOINCREMENT PRIMARY KEY,
    rule_name                   VARCHAR(100) NOT NULL UNIQUE,
    rule_type                   VARCHAR(50) NOT NULL,
    rule_level                  VARCHAR(20) NOT NULL,
    description                 VARCHAR(500),
    is_active                   BOOLEAN DEFAULT TRUE,
    created_by                  VARCHAR(100) DEFAULT CURRENT_USER(),
    created_ts                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    modified_by                 VARCHAR(100),
    modified_ts                 TIMESTAMP_NTZ
)
COMMENT = 'Master table for all DQ rule definitions';

-- Create Rule SQL Template table
CREATE OR REPLACE TABLE rule_sql_template (
    template_id                 NUMBER AUTOINCREMENT PRIMARY KEY,
    rule_id                     NUMBER NOT NULL,
    sql_template                VARCHAR(4000) NOT NULL,
    template_version            NUMBER DEFAULT 1,
    is_active                   BOOLEAN DEFAULT TRUE,
    created_by                  VARCHAR(100) DEFAULT CURRENT_USER(),
    created_ts                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    modified_by                 VARCHAR(100),
    modified_ts                 TIMESTAMP_NTZ,
    FOREIGN KEY (rule_id) REFERENCES rule_master(rule_id)
)
COMMENT = 'SQL templates for each DQ rule';

-- Create Dataset Configuration table
CREATE OR REPLACE TABLE dataset_config (
    dataset_id                  VARCHAR(100) PRIMARY KEY,
    source_database             VARCHAR(100) NOT NULL,
    source_schema               VARCHAR(100) NOT NULL,
    source_table                VARCHAR(100) NOT NULL,
    business_domain             VARCHAR(100),
    criticality                 VARCHAR(20) DEFAULT 'MEDIUM',
    is_active                   BOOLEAN DEFAULT TRUE,
    created_by                  VARCHAR(100) DEFAULT CURRENT_USER(),
    created_ts                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    modified_by                 VARCHAR(100),
    modified_ts                 TIMESTAMP_NTZ
)
COMMENT = 'Configuration for datasets to monitor';

-- Create Dataset Rule Configuration table
CREATE OR REPLACE TABLE dataset_rule_config (
    config_id                   NUMBER AUTOINCREMENT PRIMARY KEY,
    dataset_id                  VARCHAR(100) NOT NULL,
    rule_id                     NUMBER NOT NULL,
    column_name                 VARCHAR(100),
    threshold_value             NUMBER(10,2) DEFAULT 90.00,
    is_active                   BOOLEAN DEFAULT TRUE,
    created_by                  VARCHAR(100) DEFAULT CURRENT_USER(),
    created_ts                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    modified_by                 VARCHAR(100),
    modified_ts                 TIMESTAMP_NTZ,
    FOREIGN KEY (dataset_id) REFERENCES dataset_config(dataset_id),
    FOREIGN KEY (rule_id) REFERENCES rule_master(rule_id)
)
COMMENT = 'Mapping of rules to datasets and columns';

-- Create Weights Mapping table
CREATE OR REPLACE TABLE weights_mapping (
    weight_id                   NUMBER AUTOINCREMENT PRIMARY KEY,
    rule_type                   VARCHAR(50) NOT NULL,
    business_domain             VARCHAR(100),
    weight                      NUMBER(5,2) DEFAULT 1.00,
    priority                    NUMBER DEFAULT 3,
    effective_date              DATE DEFAULT CURRENT_DATE(),
    expiry_date                 DATE,
    is_active                   BOOLEAN DEFAULT TRUE,
    created_by                  VARCHAR(100) DEFAULT CURRENT_USER(),
    created_ts                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Weights for DQ score calculation';

-- =====================================================================================
-- STEP 2: POPULATE RULE_MASTER
-- =====================================================================================

INSERT INTO rule_master (rule_name, rule_type, rule_level, description, is_active)
VALUES
    -- COMPLETENESS
    ('COMPLETENESS_CHECK', 'COMPLETENESS', 'COLUMN', 'Check for NULL values', TRUE),
    
    -- UNIQUENESS
    ('UNIQUENESS_PRIMARY_KEY', 'UNIQUENESS', 'COLUMN', 'Check for duplicates', TRUE),
    
    -- VALIDITY
    ('VALIDITY_EMAIL_FORMAT', 'VALIDITY', 'COLUMN', 'Validate email format', TRUE),
    ('VALIDITY_PHONE_FORMAT', 'VALIDITY', 'COLUMN', 'Validate phone format', TRUE),
    ('VALIDITY_DATE_FORMAT', 'VALIDITY', 'COLUMN', 'Validate date format', TRUE),
    ('VALIDITY_NUMERIC_FORMAT', 'VALIDITY', 'COLUMN', 'Validate numeric format', TRUE),
    ('VALIDITY_POSITIVE_NUMBER', 'VALIDITY', 'COLUMN', 'Check positive numbers', TRUE),
    ('VALIDITY_NON_NEGATIVE_NUMBER', 'VALIDITY', 'COLUMN', 'Check non-negative numbers', TRUE),
    ('VALIDITY_ALLOWED_VALUES', 'VALIDITY', 'COLUMN', 'Check allowed values', TRUE),
    ('VALIDITY_DATE_RANGE', 'VALIDITY', 'COLUMN', 'Check date range', TRUE),
    ('VALIDITY_COUNTRY_CODE', 'VALIDITY', 'COLUMN', 'Validate country codes', TRUE),
    ('VALIDITY_CURRENCY_CODE', 'VALIDITY', 'COLUMN', 'Validate currency codes', TRUE),
    
    -- FRESHNESS
    ('FRESHNESS_DATA_RECENCY', 'FRESHNESS', 'COLUMN', 'Check data recency', TRUE),
    
    -- CONSISTENCY
    ('CONSISTENCY_FOREIGN_KEY', 'CONSISTENCY', 'COLUMN', 'Check foreign key integrity', TRUE),
    
    -- VOLUME
    ('VOLUME_ROW_COUNT_THRESHOLD', 'VOLUME', 'TABLE', 'Check row count', TRUE);

-- =====================================================================================
-- STEP 3: POPULATE RULE_SQL_TEMPLATE
-- =====================================================================================

-- Completeness Check
INSERT INTO rule_sql_template (rule_id, sql_template, template_version, is_active)
SELECT 
    rule_id,
    'SELECT COUNT(*) AS total_count, COUNT(*) - COUNT({{COLUMN}}) AS error_count FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
    1,
    TRUE
FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK';

-- Uniqueness Check
INSERT INTO rule_sql_template (rule_id, sql_template, template_version, is_active)
SELECT 
    rule_id,
    'SELECT COUNT(*) AS total_count, COUNT(*) - COUNT(DISTINCT {{COLUMN}}) AS error_count FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
    1,
    TRUE
FROM rule_master WHERE rule_name = 'UNIQUENESS_PRIMARY_KEY';

-- Email Format
INSERT INTO rule_sql_template (rule_id, sql_template, template_version, is_active)
SELECT 
    rule_id,
    'SELECT COUNT(*) AS total_count, SUM(CASE WHEN {{COLUMN}} IS NULL THEN 0 WHEN {{COLUMN}} NOT RLIKE ''^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'' THEN 1 ELSE 0 END) AS error_count FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
    1,
    TRUE
FROM rule_master WHERE rule_name = 'VALIDITY_EMAIL_FORMAT';

-- Phone Format
INSERT INTO rule_sql_template (rule_id, sql_template, template_version, is_active)
SELECT 
    rule_id,
    'SELECT COUNT(*) AS total_count, SUM(CASE WHEN {{COLUMN}} IS NULL THEN 0 WHEN {{COLUMN}} NOT RLIKE ''^\\+?[0-9]{1,4}[-.\\s]?[0-9]{3,4}[-.\\s]?[0-9]{4}$'' THEN 1 ELSE 0 END) AS error_count FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
    1,
    TRUE
FROM rule_master WHERE rule_name = 'VALIDITY_PHONE_FORMAT';

-- Date Format
INSERT INTO rule_sql_template (rule_id, sql_template, template_version, is_active)
SELECT 
    rule_id,
    'SELECT COUNT(*) AS total_count, SUM(CASE WHEN {{COLUMN}} IS NULL THEN 0 WHEN TRY_CAST({{COLUMN}} AS DATE) IS NULL THEN 1 ELSE 0 END) AS error_count FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
    1,
    TRUE
FROM rule_master WHERE rule_name = 'VALIDITY_DATE_FORMAT';

-- Numeric Format
INSERT INTO rule_sql_template (rule_id, sql_template, template_version, is_active)
SELECT 
    rule_id,
    'SELECT COUNT(*) AS total_count, SUM(CASE WHEN {{COLUMN}} IS NULL THEN 0 WHEN TRY_CAST({{COLUMN}} AS NUMBER) IS NULL THEN 1 ELSE 0 END) AS error_count FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
    1,
    TRUE
FROM rule_master WHERE rule_name = 'VALIDITY_NUMERIC_FORMAT';

-- Positive Number
INSERT INTO rule_sql_template (rule_id, sql_template, template_version, is_active)
SELECT 
    rule_id,
    'SELECT COUNT(*) AS total_count, SUM(CASE WHEN {{COLUMN}} IS NULL THEN 0 WHEN TRY_CAST({{COLUMN}} AS NUMBER) IS NULL THEN 1 WHEN TRY_CAST({{COLUMN}} AS NUMBER) <= 0 THEN 1 ELSE 0 END) AS error_count FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
    1,
    TRUE
FROM rule_master WHERE rule_name = 'VALIDITY_POSITIVE_NUMBER';

-- Non-Negative Number
INSERT INTO rule_sql_template (rule_id, sql_template, template_version, is_active)
SELECT 
    rule_id,
    'SELECT COUNT(*) AS total_count, SUM(CASE WHEN {{COLUMN}} IS NULL THEN 0 WHEN TRY_CAST({{COLUMN}} AS NUMBER) IS NULL THEN 1 WHEN TRY_CAST({{COLUMN}} AS NUMBER) < 0 THEN 1 ELSE 0 END) AS error_count FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
    1,
    TRUE
FROM rule_master WHERE rule_name = 'VALIDITY_NON_NEGATIVE_NUMBER';

-- Allowed Values
INSERT INTO rule_sql_template (rule_id, sql_template, template_version, is_active)
SELECT 
    rule_id,
    'SELECT COUNT(*) AS total_count, SUM(CASE WHEN {{COLUMN}} IS NULL THEN 0 WHEN {{COLUMN}} NOT IN ({{ALLOWED_VALUES}}) THEN 1 ELSE 0 END) AS error_count FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
    1,
    TRUE
FROM rule_master WHERE rule_name = 'VALIDITY_ALLOWED_VALUES';

-- Date Range
INSERT INTO rule_sql_template (rule_id, sql_template, template_version, is_active)
SELECT 
    rule_id,
    'SELECT COUNT(*) AS total_count, SUM(CASE WHEN {{COLUMN}} IS NULL THEN 0 WHEN TRY_CAST({{COLUMN}} AS DATE) IS NULL THEN 1 WHEN TRY_CAST({{COLUMN}} AS DATE) > CURRENT_DATE() THEN 1 WHEN TRY_CAST({{COLUMN}} AS DATE) < ''1900-01-01'' THEN 1 ELSE 0 END) AS error_count FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
    1,
    TRUE
FROM rule_master WHERE rule_name = 'VALIDITY_DATE_RANGE';

-- Country Code
INSERT INTO rule_sql_template (rule_id, sql_template, template_version, is_active)
SELECT 
    rule_id,
    'SELECT COUNT(*) AS total_count, SUM(CASE WHEN {{COLUMN}} IS NULL THEN 0 WHEN {{COLUMN}} NOT IN (''USA'', ''Canada'', ''UK'', ''Mexico'', ''India'', ''Australia'', ''Germany'', ''France'', ''Japan'', ''China'') THEN 1 ELSE 0 END) AS error_count FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
    1,
    TRUE
FROM rule_master WHERE rule_name = 'VALIDITY_COUNTRY_CODE';

-- Currency Code
INSERT INTO rule_sql_template (rule_id, sql_template, template_version, is_active)
SELECT 
    rule_id,
    'SELECT COUNT(*) AS total_count, SUM(CASE WHEN {{COLUMN}} IS NULL THEN 0 WHEN {{COLUMN}} NOT IN (''USD'', ''EUR'', ''GBP'', ''CAD'', ''JPY'', ''AUD'', ''CHF'', ''CNY'', ''INR'', ''MXN'') THEN 1 ELSE 0 END) AS error_count FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
    1,
    TRUE
FROM rule_master WHERE rule_name = 'VALIDITY_CURRENCY_CODE';

-- Freshness
INSERT INTO rule_sql_template (rule_id, sql_template, template_version, is_active)
SELECT 
    rule_id,
    'SELECT COUNT(*) AS total_count, SUM(CASE WHEN TRY_CAST({{COLUMN}} AS DATE) < DATEADD(DAY, -{{THRESHOLD}}, CURRENT_DATE()) THEN 1 ELSE 0 END) AS error_count FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
    1,
    TRUE
FROM rule_master WHERE rule_name = 'FRESHNESS_DATA_RECENCY';

-- Foreign Key
INSERT INTO rule_sql_template (rule_id, sql_template, template_version, is_active)
SELECT 
    rule_id,
    'SELECT COUNT(*) AS total_count, COUNT(*) - COUNT(p.{{PARENT_KEY}}) AS error_count FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}} c LEFT JOIN {{PARENT_DATABASE}}.{{PARENT_SCHEMA}}.{{PARENT_TABLE}} p ON c.{{COLUMN}} = p.{{PARENT_KEY}} WHERE c.{{COLUMN}} IS NOT NULL',
    1,
    TRUE
FROM rule_master WHERE rule_name = 'CONSISTENCY_FOREIGN_KEY';

-- Volume
INSERT INTO rule_sql_template (rule_id, sql_template, template_version, is_active)
SELECT 
    rule_id,
    'SELECT COUNT(*) AS total_count, CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS error_count FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}',
    1,
    TRUE
FROM rule_master WHERE rule_name = 'VOLUME_ROW_COUNT_THRESHOLD';

-- =====================================================================================
-- STEP 4: POPULATE DATASET_CONFIG
-- =====================================================================================

INSERT INTO dataset_config (dataset_id, source_database, source_schema, source_table, business_domain, criticality, is_active)
VALUES
    ('DS_CUSTOMER', 'BANKING_DW', 'BRONZE', 'STG_CUSTOMER', 'CUSTOMER_MANAGEMENT', 'CRITICAL', TRUE),
    ('DS_ACCOUNT', 'BANKING_DW', 'BRONZE', 'STG_ACCOUNT', 'ACCOUNT_MANAGEMENT', 'CRITICAL', TRUE),
    ('DS_TRANSACTION', 'BANKING_DW', 'BRONZE', 'STG_TRANSACTION', 'TRANSACTION_PROCESSING', 'HIGH', TRUE),
    ('DS_DAILY_BALANCE', 'BANKING_DW', 'BRONZE', 'STG_DAILY_BALANCE', 'FINANCIAL_REPORTING', 'HIGH', TRUE),
    ('DS_FX_RATE', 'BANKING_DW', 'BRONZE', 'STG_FX_RATE', 'MARKET_DATA', 'MEDIUM', TRUE);

-- =====================================================================================
-- STEP 5: POPULATE DATASET_RULE_CONFIG
-- =====================================================================================

-- CUSTOMER TABLE
INSERT INTO dataset_rule_config (dataset_id, rule_id, column_name, threshold_value, is_active)
SELECT 'DS_CUSTOMER', rule_id, 'customer_id', 100.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_CUSTOMER', rule_id, 'customer_id', 100.00, TRUE FROM rule_master WHERE rule_name = 'UNIQUENESS_PRIMARY_KEY'
UNION ALL
SELECT 'DS_CUSTOMER', rule_id, 'customer_name', 95.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_CUSTOMER', rule_id, 'email', 90.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_CUSTOMER', rule_id, 'email', 95.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_EMAIL_FORMAT'
UNION ALL
SELECT 'DS_CUSTOMER', rule_id, 'dob', 90.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_CUSTOMER', rule_id, 'dob', 95.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_DATE_FORMAT'
UNION ALL
SELECT 'DS_CUSTOMER', rule_id, 'dob', 98.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_DATE_RANGE'
UNION ALL
SELECT 'DS_CUSTOMER', rule_id, 'phone', 85.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_CUSTOMER', rule_id, 'phone', 90.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_PHONE_FORMAT'
UNION ALL
SELECT 'DS_CUSTOMER', rule_id, 'kyc_status', 100.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_CUSTOMER', rule_id, 'kyc_status', 98.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_ALLOWED_VALUES'
UNION ALL
SELECT 'DS_CUSTOMER', rule_id, 'country', 95.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_CUSTOMER', rule_id, 'country', 98.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_COUNTRY_CODE'
UNION ALL
SELECT 'DS_CUSTOMER', rule_id, 'created_date', 100.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_CUSTOMER', rule_id, 'created_date', 98.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_DATE_FORMAT'
UNION ALL
SELECT 'DS_CUSTOMER', rule_id, NULL, 100.00, TRUE FROM rule_master WHERE rule_name = 'VOLUME_ROW_COUNT_THRESHOLD';

-- ACCOUNT TABLE
INSERT INTO dataset_rule_config (dataset_id, rule_id, column_name, threshold_value, is_active)
SELECT 'DS_ACCOUNT', rule_id, 'account_id', 100.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_ACCOUNT', rule_id, 'account_id', 100.00, TRUE FROM rule_master WHERE rule_name = 'UNIQUENESS_PRIMARY_KEY'
UNION ALL
SELECT 'DS_ACCOUNT', rule_id, 'customer_id', 100.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_ACCOUNT', rule_id, 'customer_id', 98.00, TRUE FROM rule_master WHERE rule_name = 'CONSISTENCY_FOREIGN_KEY'
UNION ALL
SELECT 'DS_ACCOUNT', rule_id, 'account_type', 100.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_ACCOUNT', rule_id, 'account_type', 98.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_ALLOWED_VALUES'
UNION ALL
SELECT 'DS_ACCOUNT', rule_id, 'account_status', 100.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_ACCOUNT', rule_id, 'account_status', 98.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_ALLOWED_VALUES'
UNION ALL
SELECT 'DS_ACCOUNT', rule_id, 'opening_balance', 95.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_ACCOUNT', rule_id, 'opening_balance', 98.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_NUMERIC_FORMAT'
UNION ALL
SELECT 'DS_ACCOUNT', rule_id, 'opening_balance', 98.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_NON_NEGATIVE_NUMBER'
UNION ALL
SELECT 'DS_ACCOUNT', rule_id, 'opened_date', 100.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_ACCOUNT', rule_id, 'opened_date', 98.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_DATE_FORMAT'
UNION ALL
SELECT 'DS_ACCOUNT', rule_id, 'opened_date', 98.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_DATE_RANGE'
UNION ALL
SELECT 'DS_ACCOUNT', rule_id, NULL, 100.00, TRUE FROM rule_master WHERE rule_name = 'VOLUME_ROW_COUNT_THRESHOLD';

-- TRANSACTION TABLE
INSERT INTO dataset_rule_config (dataset_id, rule_id, column_name, threshold_value, is_active)
SELECT 'DS_TRANSACTION', rule_id, 'transaction_id', 100.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_TRANSACTION', rule_id, 'transaction_id', 100.00, TRUE FROM rule_master WHERE rule_name = 'UNIQUENESS_PRIMARY_KEY'
UNION ALL
SELECT 'DS_TRANSACTION', rule_id, 'account_id', 100.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_TRANSACTION', rule_id, 'account_id', 95.00, TRUE FROM rule_master WHERE rule_name = 'CONSISTENCY_FOREIGN_KEY'
UNION ALL
SELECT 'DS_TRANSACTION', rule_id, 'transaction_type', 100.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_TRANSACTION', rule_id, 'transaction_type', 98.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_ALLOWED_VALUES'
UNION ALL
SELECT 'DS_TRANSACTION', rule_id, 'transaction_amount', 100.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_TRANSACTION', rule_id, 'transaction_amount', 98.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_NUMERIC_FORMAT'
UNION ALL
SELECT 'DS_TRANSACTION', rule_id, 'transaction_amount', 95.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_POSITIVE_NUMBER'
UNION ALL
SELECT 'DS_TRANSACTION', rule_id, 'transaction_date', 100.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_TRANSACTION', rule_id, 'transaction_date', 98.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_DATE_FORMAT'
UNION ALL
SELECT 'DS_TRANSACTION', rule_id, 'transaction_date', 98.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_DATE_RANGE'
UNION ALL
SELECT 'DS_TRANSACTION', rule_id, NULL, 100.00, TRUE FROM rule_master WHERE rule_name = 'VOLUME_ROW_COUNT_THRESHOLD';

-- DAILY BALANCE TABLE
INSERT INTO dataset_rule_config (dataset_id, rule_id, column_name, threshold_value, is_active)
SELECT 'DS_DAILY_BALANCE', rule_id, 'account_id', 100.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_DAILY_BALANCE', rule_id, 'account_id', 95.00, TRUE FROM rule_master WHERE rule_name = 'CONSISTENCY_FOREIGN_KEY'
UNION ALL
SELECT 'DS_DAILY_BALANCE', rule_id, 'balance_date', 100.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_DAILY_BALANCE', rule_id, 'balance_date', 98.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_DATE_FORMAT'
UNION ALL
SELECT 'DS_DAILY_BALANCE', rule_id, 'closing_balance', 100.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_DAILY_BALANCE', rule_id, 'closing_balance', 98.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_NUMERIC_FORMAT'
UNION ALL
SELECT 'DS_DAILY_BALANCE', rule_id, NULL, 100.00, TRUE FROM rule_master WHERE rule_name = 'VOLUME_ROW_COUNT_THRESHOLD';

-- FX RATE TABLE
INSERT INTO dataset_rule_config (dataset_id, rule_id, column_name, threshold_value, is_active)
SELECT 'DS_FX_RATE', rule_id, 'currency_code', 100.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_FX_RATE', rule_id, 'currency_code', 98.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_CURRENCY_CODE'
UNION ALL
SELECT 'DS_FX_RATE', rule_id, 'fx_rate_to_usd', 100.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_FX_RATE', rule_id, 'fx_rate_to_usd', 98.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_NUMERIC_FORMAT'
UNION ALL
SELECT 'DS_FX_RATE', rule_id, 'fx_rate_to_usd', 98.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_POSITIVE_NUMBER'
UNION ALL
SELECT 'DS_FX_RATE', rule_id, 'rate_date', 100.00, TRUE FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK'
UNION ALL
SELECT 'DS_FX_RATE', rule_id, 'rate_date', 98.00, TRUE FROM rule_master WHERE rule_name = 'VALIDITY_DATE_FORMAT'
UNION ALL
SELECT 'DS_FX_RATE', rule_id, NULL, 100.00, TRUE FROM rule_master WHERE rule_name = 'VOLUME_ROW_COUNT_THRESHOLD';

-- =====================================================================================
-- STEP 6: POPULATE WEIGHTS_MAPPING
-- =====================================================================================

INSERT INTO weights_mapping (rule_type, business_domain, weight, priority, effective_date, is_active)
VALUES
    ('COMPLETENESS', 'CUSTOMER_MANAGEMENT', 2.50, 1, CURRENT_DATE(), TRUE),
    ('UNIQUENESS', 'CUSTOMER_MANAGEMENT', 3.00, 1, CURRENT_DATE(), TRUE),
    ('VALIDITY', 'CUSTOMER_MANAGEMENT', 2.00, 2, CURRENT_DATE(), TRUE),
    ('CONSISTENCY', 'CUSTOMER_MANAGEMENT', 1.50, 3, CURRENT_DATE(), TRUE),
    ('FRESHNESS', 'CUSTOMER_MANAGEMENT', 1.00, 3, CURRENT_DATE(), TRUE),
    ('VOLUME', 'CUSTOMER_MANAGEMENT', 1.00, 4, CURRENT_DATE(), TRUE),
    
    ('COMPLETENESS', 'ACCOUNT_MANAGEMENT', 2.50, 1, CURRENT_DATE(), TRUE),
    ('UNIQUENESS', 'ACCOUNT_MANAGEMENT', 3.00, 1, CURRENT_DATE(), TRUE),
    ('VALIDITY', 'ACCOUNT_MANAGEMENT', 2.50, 1, CURRENT_DATE(), TRUE),
    ('CONSISTENCY', 'ACCOUNT_MANAGEMENT', 2.00, 2, CURRENT_DATE(), TRUE),
    ('FRESHNESS', 'ACCOUNT_MANAGEMENT', 1.00, 3, CURRENT_DATE(), TRUE),
    ('VOLUME', 'ACCOUNT_MANAGEMENT', 1.00, 4, CURRENT_DATE(), TRUE),
    
    ('COMPLETENESS', 'TRANSACTION_PROCESSING', 3.00, 1, CURRENT_DATE(), TRUE),
    ('UNIQUENESS', 'TRANSACTION_PROCESSING', 3.00, 1, CURRENT_DATE(), TRUE),
    ('VALIDITY', 'TRANSACTION_PROCESSING', 2.50, 1, CURRENT_DATE(), TRUE),
    ('CONSISTENCY', 'TRANSACTION_PROCESSING', 2.00, 2, CURRENT_DATE(), TRUE),
    ('FRESHNESS', 'TRANSACTION_PROCESSING', 2.00, 2, CURRENT_DATE(), TRUE),
    ('VOLUME', 'TRANSACTION_PROCESSING', 1.50, 3, CURRENT_DATE(), TRUE),
    
    ('COMPLETENESS', 'FINANCIAL_REPORTING', 3.00, 1, CURRENT_DATE(), TRUE),
    ('UNIQUENESS', 'FINANCIAL_REPORTING', 2.00, 2, CURRENT_DATE(), TRUE),
    ('VALIDITY', 'FINANCIAL_REPORTING', 2.50, 1, CURRENT_DATE(), TRUE),
    ('CONSISTENCY', 'FINANCIAL_REPORTING', 2.50, 1, CURRENT_DATE(), TRUE),
    ('FRESHNESS', 'FINANCIAL_REPORTING', 2.50, 1, CURRENT_DATE(), TRUE),
    ('VOLUME', 'FINANCIAL_REPORTING', 1.50, 3, CURRENT_DATE(), TRUE),
    
    ('COMPLETENESS', 'MARKET_DATA', 2.50, 1, CURRENT_DATE(), TRUE),
    ('UNIQUENESS', 'MARKET_DATA', 2.00, 2, CURRENT_DATE(), TRUE),
    ('VALIDITY', 'MARKET_DATA', 3.00, 1, CURRENT_DATE(), TRUE),
    ('CONSISTENCY', 'MARKET_DATA', 1.50, 3, CURRENT_DATE(), TRUE),
    ('FRESHNESS', 'MARKET_DATA', 3.00, 1, CURRENT_DATE(), TRUE),
    ('VOLUME', 'MARKET_DATA', 1.00, 4, CURRENT_DATE(), TRUE);

-- =====================================================================================
-- STEP 7: VERIFY CONFIGURATION
-- =====================================================================================

-- Verify Rule Master
SELECT 'Rule Master' AS table_name, COUNT(*) AS row_count FROM rule_master
UNION ALL
SELECT 'Rule SQL Templates', COUNT(*) FROM rule_sql_template
UNION ALL
SELECT 'Dataset Config', COUNT(*) FROM dataset_config
UNION ALL
SELECT 'Dataset Rule Config', COUNT(*) FROM dataset_rule_config
UNION ALL
SELECT 'Weights Mapping', COUNT(*) FROM weights_mapping;

-- Show sample configuration
SELECT 
    dc.dataset_id,
    dc.source_table,
    COUNT(DISTINCT drc.rule_id) AS total_rules,
    COUNT(DISTINCT rm.rule_type) AS rule_types
FROM dataset_config dc
INNER JOIN dataset_rule_config drc ON dc.dataset_id = drc.dataset_id
INNER JOIN rule_master rm ON drc.rule_id = rm.rule_id
WHERE dc.is_active = TRUE
GROUP BY dc.dataset_id, dc.source_table
ORDER BY dc.dataset_id;

SELECT '=== CONFIGURATION SETUP COMPLETE ===' AS status;










