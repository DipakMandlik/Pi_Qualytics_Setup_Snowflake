
-- =====================================================================================
-- This script populates all 5 configuration tables with comprehensive DQ rules
-- for the 5 banking tables (Customer, Account, Transaction, Daily Balance, FX Rate)
-- =====================================================================================

USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_CONFIG;
USE WAREHOUSE DQ_ANALYTICS_WH;

-- =====================================================================================
-- STEP 1: POPULATE RULE_MASTER
-- Define all available DQ rules across 6 dimensions
-- =====================================================================================

INSERT INTO rule_master (rule_name, rule_type, rule_level, description, is_active)
VALUES
    -- ========== COMPLETENESS RULES (Column Level) ==========
    ('COMPLETENESS_CHECK', 'COMPLETENESS', 'COLUMN', 
     'Check for NULL values in a specific column', TRUE),
    
    ('COMPLETENESS_CRITICAL_COLUMNS', 'COMPLETENESS', 'TABLE', 
     'Check completeness across multiple critical columns', TRUE),
    
    -- ========== UNIQUENESS RULES (Column Level) ==========
    ('UNIQUENESS_PRIMARY_KEY', 'UNIQUENESS', 'COLUMN', 
     'Check for duplicate values in primary key column', TRUE),
    
    ('UNIQUENESS_COMPOSITE_KEY', 'UNIQUENESS', 'TABLE', 
     'Check for duplicates across composite key columns', TRUE),
    
    -- ========== VALIDITY RULES (Column Level) ==========
    ('VALIDITY_EMAIL_FORMAT', 'VALIDITY', 'COLUMN', 
     'Validate email format using regex pattern', TRUE),
    
    ('VALIDITY_PHONE_FORMAT', 'VALIDITY', 'COLUMN', 
     'Validate phone number format', TRUE),
    
    ('VALIDITY_DATE_FORMAT', 'VALIDITY', 'COLUMN', 
     'Validate date format and parse-ability', TRUE),
    
    ('VALIDITY_NUMERIC_FORMAT', 'VALIDITY', 'COLUMN', 
     'Validate numeric format and parse-ability', TRUE),
    
    ('VALIDITY_POSITIVE_NUMBER', 'VALIDITY', 'COLUMN', 
     'Check that numeric values are positive', TRUE),
    
    ('VALIDITY_NON_NEGATIVE_NUMBER', 'VALIDITY', 'COLUMN', 
     'Check that numeric values are non-negative (>= 0)', TRUE),
    
    ('VALIDITY_ALLOWED_VALUES', 'VALIDITY', 'COLUMN', 
     'Check if column values are in allowed list', TRUE),
    
    ('VALIDITY_DATE_RANGE', 'VALIDITY', 'COLUMN', 
     'Check if dates are within acceptable range (not future, not too old)', TRUE),
    
    ('VALIDITY_COUNTRY_CODE', 'VALIDITY', 'COLUMN', 
     'Validate country codes against standard list', TRUE),
    
    ('VALIDITY_CURRENCY_CODE', 'VALIDITY', 'COLUMN', 
     'Validate currency codes (ISO 4217)', TRUE),
    
    -- ========== FRESHNESS RULES (Column/Table Level) ==========
    ('FRESHNESS_DATA_RECENCY', 'FRESHNESS', 'COLUMN', 
     'Check if data is recent (within SLA threshold)', TRUE),
    
    ('FRESHNESS_LOAD_TIMESTAMP', 'FRESHNESS', 'TABLE', 
     'Check when data was last loaded', TRUE),
    
    -- ========== CONSISTENCY RULES (Column Level) ==========
    ('CONSISTENCY_FOREIGN_KEY', 'CONSISTENCY', 'COLUMN', 
     'Check referential integrity - foreign key exists in parent table', TRUE),
    
    ('CONSISTENCY_LOGICAL_RELATIONSHIP', 'CONSISTENCY', 'TABLE', 
     'Check logical relationships between columns (e.g., start_date < end_date)', TRUE),
    
    -- ========== VOLUME RULES (Table Level) ==========
    ('VOLUME_ROW_COUNT_THRESHOLD', 'VOLUME', 'TABLE', 
     'Check if row count is within expected range', TRUE),
    
    ('VOLUME_ANOMALY_DETECTION', 'VOLUME', 'TABLE', 
     'Detect anomalies in row count using moving average', TRUE);



-- =====================================================================================
-- STEP 2: POPULATE RULE_SQL_TEMPLATE
-- Create dynamic SQL templates with placeholders
-- Placeholders: {{DATABASE}}, {{SCHEMA}}, {{TABLE}}, {{COLUMN}}, {{THRESHOLD}}
-- =====================================================================================

-- Get rule_id values for foreign key reference
SET rule_id_completeness = (SELECT rule_id FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK');
SET rule_id_uniqueness_pk = (SELECT rule_id FROM rule_master WHERE rule_name = 'UNIQUENESS_PRIMARY_KEY');
SET rule_id_email = (SELECT rule_id FROM rule_master WHERE rule_name = 'VALIDITY_EMAIL_FORMAT');
SET rule_id_phone = (SELECT rule_id FROM rule_master WHERE rule_name = 'VALIDITY_PHONE_FORMAT');
SET rule_id_date_format = (SELECT rule_id FROM rule_master WHERE rule_name = 'VALIDITY_DATE_FORMAT');
SET rule_id_numeric = (SELECT rule_id FROM rule_master WHERE rule_name = 'VALIDITY_NUMERIC_FORMAT');
SET rule_id_positive = (SELECT rule_id FROM rule_master WHERE rule_name = 'VALIDITY_POSITIVE_NUMBER');
SET rule_id_non_negative = (SELECT rule_id FROM rule_master WHERE rule_name = 'VALIDITY_NON_NEGATIVE_NUMBER');
SET rule_id_allowed_values = (SELECT rule_id FROM rule_master WHERE rule_name = 'VALIDITY_ALLOWED_VALUES');
SET rule_id_date_range = (SELECT rule_id FROM rule_master WHERE rule_name = 'VALIDITY_DATE_RANGE');
SET rule_id_freshness = (SELECT rule_id FROM rule_master WHERE rule_name = 'FRESHNESS_DATA_RECENCY');
SET rule_id_fk = (SELECT rule_id FROM rule_master WHERE rule_name = 'CONSISTENCY_FOREIGN_KEY');
SET rule_id_volume = (SELECT rule_id FROM rule_master WHERE rule_name = 'VOLUME_ROW_COUNT_THRESHOLD');
SET rule_id_country = (SELECT rule_id FROM rule_master WHERE rule_name = 'VALIDITY_COUNTRY_CODE');
SET rule_id_currency = (SELECT rule_id FROM rule_master WHERE rule_name = 'VALIDITY_CURRENCY_CODE');



INSERT INTO rule_sql_template (rule_id, sql_template, template_version, is_active)
VALUES
    -- ========== COMPLETENESS CHECK ==========
    ($rule_id_completeness,
     'SELECT 
        COUNT(*) AS total_count,
        COUNT({{COLUMN}}) AS valid_count,
        COUNT(*) - COUNT({{COLUMN}}) AS error_count
      FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}
      WHERE is_current = TRUE',
     1, TRUE),
    
    -- ========== UNIQUENESS PRIMARY KEY CHECK ==========
    ($rule_id_uniqueness_pk,
     'SELECT 
        COUNT(*) AS total_count,
        COUNT(*) - COUNT(DISTINCT {{COLUMN}}) AS error_count
      FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}
      WHERE is_current = TRUE',
     1, TRUE),
    
    -- ========== EMAIL FORMAT VALIDATION ==========
    ($rule_id_email,
     'SELECT 
        COUNT(*) AS total_count,
        SUM(CASE 
          WHEN {{COLUMN}} IS NULL THEN 0
          WHEN {{COLUMN}} NOT RLIKE ''^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'' THEN 1
          ELSE 0
        END) AS error_count
      FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}
      WHERE is_current = TRUE',
     1, TRUE),
    
    -- ========== PHONE FORMAT VALIDATION ==========
    ($rule_id_phone,
     'SELECT 
        COUNT(*) AS total_count,
        SUM(CASE 
          WHEN {{COLUMN}} IS NULL THEN 0
          WHEN {{COLUMN}} NOT RLIKE ''^\\+?[0-9]{1,4}[-.\\s]?[0-9]{3,4}[-.\\s]?[0-9]{4}$'' THEN 1
          ELSE 0
        END) AS error_count
      FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}
      WHERE is_current = TRUE',
     1, TRUE),
    
    -- ========== DATE FORMAT VALIDATION ==========
    ($rule_id_date_format,
     'SELECT 
        COUNT(*) AS total_count,
        SUM(CASE 
          WHEN {{COLUMN}} IS NULL THEN 0
          WHEN TRY_CAST({{COLUMN}} AS DATE) IS NULL THEN 1
          ELSE 0
        END) AS error_count
      FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}
      WHERE is_current = TRUE',
     1, TRUE),
    
    -- ========== NUMERIC FORMAT VALIDATION ==========
    ($rule_id_numeric,
     'SELECT 
        COUNT(*) AS total_count,
        SUM(CASE 
          WHEN {{COLUMN}} IS NULL THEN 0
          WHEN TRY_CAST({{COLUMN}} AS NUMBER) IS NULL THEN 1
          ELSE 0
        END) AS error_count
      FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}
      WHERE is_current = TRUE',
     1, TRUE),
    
    -- ========== POSITIVE NUMBER VALIDATION ==========
    ($rule_id_positive,
     'SELECT 
        COUNT(*) AS total_count,
        SUM(CASE 
          WHEN {{COLUMN}} IS NULL THEN 0
          WHEN TRY_CAST({{COLUMN}} AS NUMBER) IS NULL THEN 1
          WHEN TRY_CAST({{COLUMN}} AS NUMBER) <= 0 THEN 1
          ELSE 0
        END) AS error_count
      FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}
      WHERE is_current = TRUE',
     1, TRUE),
    
    -- ========== NON-NEGATIVE NUMBER VALIDATION ==========
    ($rule_id_non_negative,
     'SELECT 
        COUNT(*) AS total_count,
        SUM(CASE 
          WHEN {{COLUMN}} IS NULL THEN 0
          WHEN TRY_CAST({{COLUMN}} AS NUMBER) IS NULL THEN 1
          WHEN TRY_CAST({{COLUMN}} AS NUMBER) < 0 THEN 1
          ELSE 0
        END) AS error_count
      FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}
      WHERE is_current = TRUE',
     1, TRUE),
    
    -- ========== ALLOWED VALUES VALIDATION (KYC Status) ==========
    ($rule_id_allowed_values,
     'SELECT 
        COUNT(*) AS total_count,
        SUM(CASE 
          WHEN {{COLUMN}} IS NULL THEN 0
          WHEN {{COLUMN}} NOT IN ({{ALLOWED_VALUES}}) THEN 1
          ELSE 0
        END) AS error_count
      FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}
      WHERE is_current = TRUE',
     1, TRUE),
    
    -- ========== DATE RANGE VALIDATION ==========
    ($rule_id_date_range,
     'SELECT 
        COUNT(*) AS total_count,
        SUM(CASE 
          WHEN {{COLUMN}} IS NULL THEN 0
          WHEN TRY_CAST({{COLUMN}} AS DATE) IS NULL THEN 1
          WHEN TRY_CAST({{COLUMN}} AS DATE) > CURRENT_DATE() THEN 1
          WHEN TRY_CAST({{COLUMN}} AS DATE) < ''1900-01-01'' THEN 1
          ELSE 0
        END) AS error_count
      FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}
      WHERE is_current = TRUE',
     1, TRUE),
    
    -- ========== FRESHNESS CHECK ==========
    ($rule_id_freshness,
     'SELECT 
        COUNT(*) AS total_count,
        SUM(CASE 
          WHEN TRY_CAST({{COLUMN}} AS DATE) < DATEADD(DAY, -{{THRESHOLD}}, CURRENT_DATE()) THEN 1
          ELSE 0
        END) AS error_count
      FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}
      WHERE is_current = TRUE',
     1, TRUE),
    
    -- ========== FOREIGN KEY CONSISTENCY CHECK ==========
    ($rule_id_fk,
     'SELECT 
        COUNT(*) AS total_count,
        COUNT(*) - COUNT(p.{{PARENT_KEY}}) AS error_count
      FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}} c
      LEFT JOIN {{PARENT_DATABASE}}.{{PARENT_SCHEMA}}.{{PARENT_TABLE}} p
        ON c.{{COLUMN}} = p.{{PARENT_KEY}}
        AND p.is_current = TRUE
      WHERE c.is_current = TRUE
        AND c.{{COLUMN}} IS NOT NULL',
     1, TRUE),
    
    -- ========== VOLUME CHECK ==========
    ($rule_id_volume,
     'SELECT 
        COUNT(*) AS total_count,
        CASE 
          WHEN COUNT(*) = 0 THEN 1
          ELSE 0
        END AS error_count
      FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}
      WHERE is_current = TRUE',
     1, TRUE),
    
    -- ========== COUNTRY CODE VALIDATION ==========
    ($rule_id_country,
     'SELECT 
        COUNT(*) AS total_count,
        SUM(CASE 
          WHEN {{COLUMN}} IS NULL THEN 0
          WHEN {{COLUMN}} NOT IN (''USA'', ''Canada'', ''UK'', ''Mexico'', ''India'', ''Australia'', ''Germany'', ''France'', ''Japan'', ''China'') THEN 1
          ELSE 0
        END) AS error_count
      FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}
      WHERE is_current = TRUE',
     1, TRUE),
    
    -- ========== CURRENCY CODE VALIDATION ==========
    ($rule_id_currency,
     'SELECT 
        COUNT(*) AS total_count,
        SUM(CASE 
          WHEN {{COLUMN}} IS NULL THEN 0
          WHEN {{COLUMN}} NOT IN (''USD'', ''EUR'', ''GBP'', ''CAD'', ''JPY'', ''AUD'', ''CHF'', ''CNY'', ''INR'', ''MXN'', ''BRL'', ''NZD'', ''SEK'') THEN 1
          ELSE 0
        END) AS error_count
      FROM {{DATABASE}}.{{SCHEMA}}.{{TABLE}}
      WHERE is_current = TRUE',
     1, TRUE);



-- =====================================================================================
-- STEP 3: POPULATE DATASET_CONFIG
-- Register all 5 tables for DQ monitoring
-- =====================================================================================

INSERT INTO dataset_config (
    dataset_id,
    source_database,
    source_schema,
    source_table,
    business_domain,
    criticality,
    is_active
)
VALUES
    ('DS_CUSTOMER', 'BANKING_DW', 'BRONZE', 'STG_CUSTOMER', 
     'CUSTOMER_MANAGEMENT', 'CRITICAL', TRUE),
    
    ('DS_ACCOUNT', 'BANKING_DW', 'BRONZE', 'STG_ACCOUNT', 
     'ACCOUNT_MANAGEMENT', 'CRITICAL', TRUE),
    
    ('DS_TRANSACTION', 'BANKING_DW', 'BRONZE', 'STG_TRANSACTION', 
     'TRANSACTION_PROCESSING', 'HIGH', TRUE),
    
    ('DS_DAILY_BALANCE', 'BANKING_DW', 'BRONZE', 'STG_DAILY_BALANCE', 
     'FINANCIAL_REPORTING', 'HIGH', TRUE),
    
    ('DS_FX_RATE', 'BANKING_DW', 'BRONZE', 'STG_FX_RATE', 
     'MARKET_DATA', 'MEDIUM', TRUE);


-- =====================================================================================
-- STEP 4: POPULATE DATASET_RULE_CONFIG
-- Map rules to specific columns with thresholds (MAXIMUM COVERAGE)
-- =====================================================================================

-- Get rule IDs for reference
SET rule_completeness = (SELECT rule_id FROM rule_master WHERE rule_name = 'COMPLETENESS_CHECK');
SET rule_uniqueness = (SELECT rule_id FROM rule_master WHERE rule_name = 'UNIQUENESS_PRIMARY_KEY');
SET rule_email = (SELECT rule_id FROM rule_master WHERE rule_name = 'VALIDITY_EMAIL_FORMAT');
SET rule_phone = (SELECT rule_id FROM rule_master WHERE rule_name = 'VALIDITY_PHONE_FORMAT');
SET rule_date_format = (SELECT rule_id FROM rule_master WHERE rule_name = 'VALIDITY_DATE_FORMAT');
SET rule_numeric = (SELECT rule_id FROM rule_master WHERE rule_name = 'VALIDITY_NUMERIC_FORMAT');
SET rule_positive = (SELECT rule_id FROM rule_master WHERE rule_name = 'VALIDITY_POSITIVE_NUMBER');
SET rule_non_negative = (SELECT rule_id FROM rule_master WHERE rule_name = 'VALIDITY_NON_NEGATIVE_NUMBER');
SET rule_allowed = (SELECT rule_id FROM rule_master WHERE rule_name = 'VALIDITY_ALLOWED_VALUES');
SET rule_date_range = (SELECT rule_id FROM rule_master WHERE rule_name = 'VALIDITY_DATE_RANGE');
SET rule_freshness = (SELECT rule_id FROM rule_master WHERE rule_name = 'FRESHNESS_DATA_RECENCY');
SET rule_fk = (SELECT rule_id FROM rule_master WHERE rule_name = 'CONSISTENCY_FOREIGN_KEY');
SET rule_volume = (SELECT rule_id FROM rule_master WHERE rule_name = 'VOLUME_ROW_COUNT_THRESHOLD');
SET rule_country = (SELECT rule_id FROM rule_master WHERE rule_name = 'VALIDITY_COUNTRY_CODE');
SET rule_currency = (SELECT rule_id FROM rule_master WHERE rule_name = 'VALIDITY_CURRENCY_CODE');



INSERT INTO dataset_rule_config (dataset_id, rule_id, column_name, threshold_value, is_active)
VALUES
    -- ========== CUSTOMER TABLE RULES ==========
    
    -- Customer ID: Completeness + Uniqueness
    ('DS_CUSTOMER', $rule_completeness, 'customer_id', 100.00, TRUE),
    ('DS_CUSTOMER', $rule_uniqueness, 'customer_id', 100.00, TRUE),
    
    -- Customer Name: Completeness
    ('DS_CUSTOMER', $rule_completeness, 'customer_name', 95.00, TRUE),
    
    -- Email: Completeness + Email Format
    ('DS_CUSTOMER', $rule_completeness, 'email', 90.00, TRUE),
    ('DS_CUSTOMER', $rule_email, 'email', 95.00, TRUE),
    
    -- DOB: Completeness + Date Format + Date Range
    ('DS_CUSTOMER', $rule_completeness, 'dob', 90.00, TRUE),
    ('DS_CUSTOMER', $rule_date_format, 'dob', 95.00, TRUE),
    ('DS_CUSTOMER', $rule_date_range, 'dob', 98.00, TRUE),
    
    -- Phone: Completeness + Phone Format
    ('DS_CUSTOMER', $rule_completeness, 'phone', 85.00, TRUE),
    ('DS_CUSTOMER', $rule_phone, 'phone', 90.00, TRUE),
    
    -- KYC Status: Completeness + Allowed Values
    ('DS_CUSTOMER', $rule_completeness, 'kyc_status', 100.00, TRUE),
    ('DS_CUSTOMER', $rule_allowed, 'kyc_status', 98.00, TRUE),
    
    -- Country: Completeness + Country Code Validation
    ('DS_CUSTOMER', $rule_completeness, 'country', 95.00, TRUE),
    ('DS_CUSTOMER', $rule_country, 'country', 98.00, TRUE),
    
    -- Created Date: Completeness + Date Format + Freshness
    ('DS_CUSTOMER', $rule_completeness, 'created_date', 100.00, TRUE),
    ('DS_CUSTOMER', $rule_date_format, 'created_date', 98.00, TRUE),
    ('DS_CUSTOMER', $rule_freshness, 'created_date', 90.00, TRUE),
    
    -- ========== ACCOUNT TABLE RULES ==========
    
    -- Account ID: Completeness + Uniqueness
    ('DS_ACCOUNT', $rule_completeness, 'account_id', 100.00, TRUE),
    ('DS_ACCOUNT', $rule_uniqueness, 'account_id', 100.00, TRUE),
    
    -- Customer ID (FK): Completeness + Foreign Key Consistency
    ('DS_ACCOUNT', $rule_completeness, 'customer_id', 100.00, TRUE),
    ('DS_ACCOUNT', $rule_fk, 'customer_id', 98.00, TRUE),
    
    -- Account Type: Completeness + Allowed Values
    ('DS_ACCOUNT', $rule_completeness, 'account_type', 100.00, TRUE),
    ('DS_ACCOUNT', $rule_allowed, 'account_type', 98.00, TRUE),
    
    -- Account Status: Completeness + Allowed Values
    ('DS_ACCOUNT', $rule_completeness, 'account_status', 100.00, TRUE),
    ('DS_ACCOUNT', $rule_allowed, 'account_status', 98.00, TRUE),
    
    -- Opening Balance: Completeness + Numeric Format + Non-Negative
    ('DS_ACCOUNT', $rule_completeness, 'opening_balance', 95.00, TRUE),
    ('DS_ACCOUNT', $rule_numeric, 'opening_balance', 98.00, TRUE),
    ('DS_ACCOUNT', $rule_non_negative, 'opening_balance', 98.00, TRUE),
    
    -- Opened Date: Completeness + Date Format + Date Range
    ('DS_ACCOUNT', $rule_completeness, 'opened_date', 100.00, TRUE),
    ('DS_ACCOUNT', $rule_date_format, 'opened_date', 98.00, TRUE),
    ('DS_ACCOUNT', $rule_date_range, 'opened_date', 98.00, TRUE),
    
    -- ========== TRANSACTION TABLE RULES ==========
    
    -- Transaction ID: Completeness + Uniqueness
    ('DS_TRANSACTION', $rule_completeness, 'transaction_id', 100.00, TRUE),
    ('DS_TRANSACTION', $rule_uniqueness, 'transaction_id', 100.00, TRUE),
    
    -- Account ID (FK): Completeness + Foreign Key Consistency
    ('DS_TRANSACTION', $rule_completeness, 'account_id', 100.00, TRUE),
    ('DS_TRANSACTION', $rule_fk, 'account_id', 95.00, TRUE),
    
    -- Transaction Type: Completeness + Allowed Values
    ('DS_TRANSACTION', $rule_completeness, 'transaction_type', 100.00, TRUE),
    ('DS_TRANSACTION', $rule_allowed, 'transaction_type', 98.00, TRUE),
    
    -- Transaction Amount: Completeness + Numeric Format + Positive
    ('DS_TRANSACTION', $rule_completeness, 'transaction_amount', 100.00, TRUE),
    ('DS_TRANSACTION', $rule_numeric, 'transaction_amount', 98.00, TRUE),
    ('DS_TRANSACTION', $rule_positive, 'transaction_amount', 95.00, TRUE),
    
    -- Transaction Date: Completeness + Date Format + Date Range + Freshness
    ('DS_TRANSACTION', $rule_completeness, 'transaction_date', 100.00, TRUE),
    ('DS_TRANSACTION', $rule_date_format, 'transaction_date', 98.00, TRUE),
    ('DS_TRANSACTION', $rule_date_range, 'transaction_date', 98.00, TRUE),
    ('DS_TRANSACTION', $rule_freshness, 'transaction_date', 90.00, TRUE),
    
    -- ========== DAILY BALANCE TABLE RULES ==========
    
    -- Account ID (FK): Completeness + Foreign Key Consistency
    ('DS_DAILY_BALANCE', $rule_completeness, 'account_id', 100.00, TRUE),
    ('DS_DAILY_BALANCE', $rule_fk, 'account_id', 95.00, TRUE),
    
    -- Balance Date: Completeness + Date Format + Freshness
    ('DS_DAILY_BALANCE', $rule_completeness, 'balance_date', 100.00, TRUE),
    ('DS_DAILY_BALANCE', $rule_date_format, 'balance_date', 98.00, TRUE),
    ('DS_DAILY_BALANCE', $rule_freshness, 'balance_date', 90.00, TRUE),
    
    -- Closing Balance: Completeness + Numeric Format
    ('DS_DAILY_BALANCE', $rule_completeness, 'closing_balance', 100.00, TRUE),
    ('DS_DAILY_BALANCE', $rule_numeric, 'closing_balance', 98.00, TRUE),
    
    -- ========== FX RATE TABLE RULES ==========
    
    -- Currency Code: Completeness + Currency Code Validation
    ('DS_FX_RATE', $rule_completeness, 'currency_code', 100.00, TRUE),
    ('DS_FX_RATE', $rule_currency, 'currency_code', 98.00, TRUE),
    
    -- FX Rate: Completeness + Numeric Format + Positive
    ('DS_FX_RATE', $rule_completeness, 'fx_rate_to_usd', 100.00, TRUE),
    ('DS_FX_RATE', $rule_numeric, 'fx_rate_to_usd', 98.00, TRUE),
    ('DS_FX_RATE', $rule_positive, 'fx_rate_to_usd', 98.00, TRUE),
    
    -- Rate Date: Completeness + Date Format + Freshness
    ('DS_FX_RATE', $rule_completeness, 'rate_date', 100.00, TRUE),
    ('DS_FX_RATE', $rule_date_format, 'rate_date', 98.00, TRUE),
    ('DS_FX_RATE', $rule_freshness, 'rate_date', 85.00, TRUE),
    
    -- Table-level Volume checks for all datasets
    ('DS_CUSTOMER', $rule_volume, NULL, 100.00, TRUE),
    ('DS_ACCOUNT', $rule_volume, NULL, 100.00, TRUE),
    ('DS_TRANSACTION', $rule_volume, NULL, 100.00, TRUE),
    ('DS_DAILY_BALANCE', $rule_volume, NULL, 100.00, TRUE),
    ('DS_FX_RATE', $rule_volume, NULL, 100.00, TRUE);



-- =====================================================================================
-- STEP 5: POPULATE WEIGHTS_MAPPING
-- Define weights for DQ score calculation by business domain and rule type
-- =====================================================================================

INSERT INTO weights_mapping (
    rule_type,
    business_domain,
    weight,
    priority,
    effective_date,
    expiry_date,
    is_active
)
VALUES
    -- ========== CUSTOMER MANAGEMENT DOMAIN ==========
    ('COMPLETENESS', 'CUSTOMER_MANAGEMENT', 2.50, 1, CURRENT_DATE(), NULL, TRUE),
    ('UNIQUENESS', 'CUSTOMER_MANAGEMENT', 3.00, 1, CURRENT_DATE(), NULL, TRUE),
    ('VALIDITY', 'CUSTOMER_MANAGEMENT', 2.00, 2, CURRENT_DATE(), NULL, TRUE),
    ('CONSISTENCY', 'CUSTOMER_MANAGEMENT', 1.50, 3, CURRENT_DATE(), NULL, TRUE),
    ('FRESHNESS', 'CUSTOMER_MANAGEMENT', 1.00, 3, CURRENT_DATE(), NULL, TRUE),
    ('VOLUME', 'CUSTOMER_MANAGEMENT', 1.00, 4, CURRENT_DATE(), NULL, TRUE),
    
    -- ========== ACCOUNT MANAGEMENT DOMAIN ==========
    ('COMPLETENESS', 'ACCOUNT_MANAGEMENT', 2.50, 1, CURRENT_DATE(), NULL, TRUE),
    ('UNIQUENESS', 'ACCOUNT_MANAGEMENT', 3.00, 1, CURRENT_DATE(), NULL, TRUE),
    ('VALIDITY', 'ACCOUNT_MANAGEMENT', 2.50, 1, CURRENT_DATE(), NULL, TRUE),
    ('CONSISTENCY', 'ACCOUNT_MANAGEMENT', 2.00, 2, CURRENT_DATE(), NULL, TRUE),
    ('FRESHNESS', 'ACCOUNT_MANAGEMENT', 1.00, 3, CURRENT_DATE(), NULL, TRUE),
    ('VOLUME', 'ACCOUNT_MANAGEMENT', 1.00, 4, CURRENT_DATE(), NULL, TRUE),
    
    -- ========== TRANSACTION PROCESSING DOMAIN ==========
    ('COMPLETENESS', 'TRANSACTION_PROCESSING', 3.00, 1, CURRENT_DATE(), NULL, TRUE),
    ('UNIQUENESS', 'TRANSACTION_PROCESSING', 3.00, 1, CURRENT_DATE(), NULL, TRUE),
    ('VALIDITY', 'TRANSACTION_PROCESSING', 2.50, 1, CURRENT_DATE(), NULL, TRUE),
    ('CONSISTENCY', 'TRANSACTION_PROCESSING', 2.00, 2, CURRENT_DATE(), NULL, TRUE),
    ('FRESHNESS', 'TRANSACTION_PROCESSING', 2.00, 2, CURRENT_DATE(), NULL, TRUE),
    ('VOLUME', 'TRANSACTION_PROCESSING', 1.50, 3, CURRENT_DATE(), NULL, TRUE),
    
    -- ========== FINANCIAL REPORTING DOMAIN ==========
    ('COMPLETENESS', 'FINANCIAL_REPORTING', 3.00, 1, CURRENT_DATE(), NULL, TRUE),
    ('UNIQUENESS', 'FINANCIAL_REPORTING', 2.00, 2, CURRENT_DATE(), NULL, TRUE),
    ('VALIDITY', 'FINANCIAL_REPORTING', 2.50, 1, CURRENT_DATE(), NULL, TRUE),
    ('CONSISTENCY', 'FINANCIAL_REPORTING', 2.50, 1, CURRENT_DATE(), NULL, TRUE),
    ('FRESHNESS', 'FINANCIAL_REPORTING', 2.50, 1, CURRENT_DATE(), NULL, TRUE),
    ('VOLUME', 'FINANCIAL_REPORTING', 1.50, 3, CURRENT_DATE(), NULL, TRUE),
    
    -- ========== MARKET DATA DOMAIN ==========
    ('COMPLETENESS', 'MARKET_DATA', 2.50, 1, CURRENT_DATE(), NULL, TRUE),
    ('UNIQUENESS', 'MARKET_DATA', 2.00, 2, CURRENT_DATE(), NULL, TRUE),
    ('VALIDITY', 'MARKET_DATA', 3.00, 1, CURRENT_DATE(), NULL, TRUE),
    ('CONSISTENCY', 'MARKET_DATA', 1.50, 3, CURRENT_DATE(), NULL, TRUE),
    ('FRESHNESS', 'MARKET_DATA', 3.00, 1, CURRENT_DATE(), NULL, TRUE),
    ('VOLUME', 'MARKET_DATA', 1.00, 4, CURRENT_DATE(), NULL, TRUE);

