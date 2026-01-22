-- ============================================================================
-- PRODUCTION-READY ENVIRONMENT SETUP
-- Pi-Qualytics Data Quality Platform
-- ============================================================================
-- Purpose: Initialize Snowflake environment with 3-layer architecture
-- Layers: Bronze (Raw) → Silver (Cleansed) → Gold (Analytics)
-- Version: 1.0.0
-- Author: Pi-Qualytics Team
-- Date: 2026-01-22
-- ============================================================================

-- ============================================================================
-- SECTION 1: ROLE & WAREHOUSE SETUP
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Ingestion Warehouse (for ETL and data loading)
CREATE WAREHOUSE IF NOT EXISTS DQ_INGESTION_WH
WITH
    WAREHOUSE_SIZE      = 'X-SMALL'
    AUTO_SUSPEND        = 60        -- Suspend after 1 minute of inactivity
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    MIN_CLUSTER_COUNT   = 1
    MAX_CLUSTER_COUNT   = 2
    SCALING_POLICY      = 'STANDARD'
COMMENT = 'Warehouse for data ingestion, ETL operations, and Bronze layer processing';

-- Analytics Warehouse (for DQ checks and reporting)
CREATE WAREHOUSE IF NOT EXISTS DQ_ANALYTICS_WH
WITH
    WAREHOUSE_SIZE      = 'SMALL'
    AUTO_SUSPEND        = 300       -- Suspend after 5 minutes of inactivity
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    MIN_CLUSTER_COUNT   = 1
    MAX_CLUSTER_COUNT   = 3
    SCALING_POLICY      = 'STANDARD'
COMMENT = 'Warehouse for analytics, data quality checks, and Gold layer aggregations';

-- ============================================================================
-- SECTION 2: DATABASE STRUCTURE (MEDALLION ARCHITECTURE)
-- ============================================================================

-- Banking Data Warehouse (Multi-layered architecture)
CREATE DATABASE IF NOT EXISTS BANKING_DW
    DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Banking Data Warehouse - 3-layer medallion architecture (Bronze/Silver/Gold)';

USE DATABASE BANKING_DW;

-- Layer 1: BRONZE (Raw/Landing Zone)
-- Purpose: Store raw, unprocessed data exactly as received from source systems
CREATE SCHEMA IF NOT EXISTS BRONZE
    DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Bronze Layer - Raw data from source systems (untransformed, schema-on-read)';

-- Layer 2: SILVER (Cleansed/Validated)
-- Purpose: Store validated, cleansed, and standardized data
CREATE SCHEMA IF NOT EXISTS SILVER
    DATA_RETENTION_TIME_IN_DAYS = 14
COMMENT = 'Silver Layer - Cleansed and validated data (type-safe, quality-checked)';

-- Layer 3: GOLD (Curated/Analytics-Ready)
-- Purpose: Store business-ready, aggregated datasets for analytics
CREATE SCHEMA IF NOT EXISTS GOLD
    DATA_RETENTION_TIME_IN_DAYS = 30
COMMENT = 'Gold Layer - Curated analytics datasets (business-ready, aggregated)';

-- ============================================================================
-- SECTION 3: DATA QUALITY FRAMEWORK DATABASE
-- ============================================================================

CREATE DATABASE IF NOT EXISTS DATA_QUALITY_DB
    DATA_RETENTION_TIME_IN_DAYS = 30
COMMENT = 'Data Quality Framework - Configuration, metrics, and observability';

USE DATABASE DATA_QUALITY_DB;

-- Configuration Schema (metadata and rules)
CREATE SCHEMA IF NOT EXISTS DQ_CONFIG
COMMENT = 'DQ Configuration - Dataset configs, rules, schedules, and mappings';

-- Metrics Schema (results and logs)
CREATE SCHEMA IF NOT EXISTS DQ_METRICS
COMMENT = 'DQ Metrics - Check results, profiling data, run logs, and failed records';

-- Engine Schema (stored procedures)
CREATE SCHEMA IF NOT EXISTS DQ_ENGINE
COMMENT = 'DQ Engine - Stored procedures and functions for quality checks';

-- Observability Schema (AI-driven insights)
CREATE SCHEMA IF NOT EXISTS DB_METRICS
COMMENT = 'DQ Observability - AI-driven metrics, insights, and schema registry';

-- ============================================================================
-- SECTION 4: FILE FORMAT & STAGE SETUP
-- ============================================================================

USE DATABASE BANKING_DW;
USE SCHEMA BRONZE;
USE WAREHOUSE DQ_INGESTION_WH;

-- Standard CSV File Format
CREATE OR REPLACE FILE FORMAT CSV_FILE_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ESCAPE = 'NONE'
    ESCAPE_UNENCLOSED_FIELD = '\134'
    DATE_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO'
    NULL_IF = ('NULL', 'null', '', 'N/A', 'NA', 'n/a', '-')
    EMPTY_FIELD_AS_NULL = TRUE
COMMENT = 'Standard CSV file format for data ingestion with flexible error handling';

-- JSON File Format (for future use)
CREATE OR REPLACE FILE FORMAT JSON_FILE_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    STRIP_NULL_VALUES = FALSE
    IGNORE_UTF8_ERRORS = TRUE
COMMENT = 'Standard JSON file format for semi-structured data ingestion';

-- Internal Stage for CSV uploads
CREATE OR REPLACE STAGE CSV_STAGE
    FILE_FORMAT = CSV_FILE_FORMAT
    DIRECTORY = (ENABLE = TRUE)
COMMENT = 'Internal stage for uploading CSV files from local/external sources';

-- ============================================================================
-- SECTION 5: BRONZE LAYER - RAW DATA TABLES
-- ============================================================================

USE SCHEMA BRONZE;

-- Customer Staging Table (Raw)
CREATE OR REPLACE TABLE STG_CUSTOMER (
    -- Business Keys
    customer_id                 STRING          COMMENT 'Customer unique identifier (raw)',
    
    -- Customer Attributes (all STRING to capture data quality issues)
    customer_name               STRING          COMMENT 'Customer full name',
    email                       STRING          COMMENT 'Customer email address',
    dob                         STRING          COMMENT 'Date of birth (raw format)',
    phone                       STRING          COMMENT 'Phone number',
    kyc_status                  STRING          COMMENT 'KYC verification status',
    country                     STRING          COMMENT 'Country of residence',
    created_date                STRING          COMMENT 'Account creation date (raw format)',
    
    -- Metadata Columns (Audit Trail)
    load_timestamp              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record load timestamp',
    source_file                 STRING          COMMENT 'Source file name',
    load_batch_id               STRING          COMMENT 'Batch identifier for tracking'
)
COMMENT = 'Bronze - Raw customer data (schema-on-read, all columns as STRING)';

-- Account Staging Table (Raw)
CREATE OR REPLACE TABLE STG_ACCOUNT (
    -- Business Keys
    account_id                  STRING          COMMENT 'Account unique identifier (raw)',
    customer_id                 STRING          COMMENT 'Foreign key to customer (raw)',
    
    -- Account Attributes
    account_type                STRING          COMMENT 'Account type (Savings/Checking/etc)',
    account_status              STRING          COMMENT 'Account status (Active/Inactive/etc)',
    opening_balance             STRING          COMMENT 'Opening balance (raw, may contain invalid numbers)',
    opened_date                 STRING          COMMENT 'Account opening date (raw format)',
    
    -- Metadata Columns
    load_timestamp              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record load timestamp',
    source_file                 STRING          COMMENT 'Source file name',
    load_batch_id               STRING          COMMENT 'Batch identifier for tracking'
)
COMMENT = 'Bronze - Raw account data (schema-on-read, all columns as STRING)';

-- Transaction Staging Table (Raw)
CREATE OR REPLACE TABLE STG_TRANSACTION (
    -- Business Keys
    transaction_id              STRING          COMMENT 'Transaction unique identifier (raw)',
    account_id                  STRING          COMMENT 'Foreign key to account (raw)',
    
    -- Transaction Attributes
    transaction_type            STRING          COMMENT 'Transaction type (Deposit/Withdrawal/etc)',
    transaction_amount          STRING          COMMENT 'Transaction amount (raw, may contain invalid numbers)',
    transaction_date            STRING          COMMENT 'Transaction date (raw format)',
    
    -- Metadata Columns
    load_timestamp              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record load timestamp',
    source_file                 STRING          COMMENT 'Source file name',
    load_batch_id               STRING          COMMENT 'Batch identifier for tracking'
)
COMMENT = 'Bronze - Raw transaction data (schema-on-read, all columns as STRING)';

-- Daily Balance Staging Table (Raw)
CREATE OR REPLACE TABLE STG_DAILY_BALANCE (
    -- Business Keys
    account_id                  STRING          COMMENT 'Foreign key to account (raw)',
    balance_date                STRING          COMMENT 'Balance snapshot date (raw format)',
    
    -- Balance Attributes
    closing_balance             STRING          COMMENT 'Closing balance (raw, may contain invalid numbers)',
    
    -- Metadata Columns
    load_timestamp              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record load timestamp',
    source_file                 STRING          COMMENT 'Source file name',
    load_batch_id               STRING          COMMENT 'Batch identifier for tracking'
)
COMMENT = 'Bronze - Raw daily balance snapshots (schema-on-read, all columns as STRING)';

-- FX Rate Staging Table (Raw)
CREATE OR REPLACE TABLE STG_FX_RATE (
    -- Business Keys
    currency_code               STRING          COMMENT 'Currency code (USD/EUR/GBP/etc)',
    rate_date                   STRING          COMMENT 'Exchange rate date (raw format)',
    
    -- Rate Attributes
    fx_rate_to_usd              STRING          COMMENT 'Exchange rate to USD (raw, may contain invalid numbers)',
    
    -- Metadata Columns
    load_timestamp              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record load timestamp',
    source_file                 STRING          COMMENT 'Source file name',
    load_batch_id               STRING          COMMENT 'Batch identifier for tracking'
)
COMMENT = 'Bronze - Raw FX rate data (schema-on-read, all columns as STRING)';

-- ============================================================================
-- SECTION 6: VERIFICATION QUERIES
-- ============================================================================

-- Verify databases
SHOW DATABASES LIKE '%BANKING_DW%';
SHOW DATABASES LIKE '%DATA_QUALITY_DB%';

-- Verify schemas
SHOW SCHEMAS IN DATABASE BANKING_DW;
SHOW SCHEMAS IN DATABASE DATA_QUALITY_DB;

-- Verify warehouses
SHOW WAREHOUSES LIKE '%DQ_%';

-- Verify tables in Bronze layer
SHOW TABLES IN SCHEMA BANKING_DW.BRONZE;

-- Verify file formats and stages
SHOW FILE FORMATS IN SCHEMA BANKING_DW.BRONZE;
SHOW STAGES IN SCHEMA BANKING_DW.BRONZE;

-- ============================================================================
-- SECTION 7: GRANT PERMISSIONS
-- ============================================================================

-- Grant usage on warehouses
GRANT USAGE ON WAREHOUSE DQ_INGESTION_WH TO ROLE ACCOUNTADMIN;
GRANT USAGE ON WAREHOUSE DQ_ANALYTICS_WH TO ROLE ACCOUNTADMIN;

-- Grant database permissions
GRANT ALL ON DATABASE BANKING_DW TO ROLE ACCOUNTADMIN;
GRANT ALL ON DATABASE DATA_QUALITY_DB TO ROLE ACCOUNTADMIN;

-- Grant schema permissions
GRANT ALL ON ALL SCHEMAS IN DATABASE BANKING_DW TO ROLE ACCOUNTADMIN;
GRANT ALL ON ALL SCHEMAS IN DATABASE DATA_QUALITY_DB TO ROLE ACCOUNTADMIN;

-- Grant table permissions
GRANT ALL ON ALL TABLES IN SCHEMA BANKING_DW.BRONZE TO ROLE ACCOUNTADMIN;
GRANT ALL ON ALL TABLES IN SCHEMA BANKING_DW.SILVER TO ROLE ACCOUNTADMIN;
GRANT ALL ON ALL TABLES IN SCHEMA BANKING_DW.GOLD TO ROLE ACCOUNTADMIN;

-- ============================================================================
-- SETUP COMPLETE
-- ============================================================================
-- Next Steps:
-- 1. Upload CSV files to @CSV_STAGE using SnowSQL or Snowflake UI
-- 2. Execute 02_Data_Loading.sql to load data into Bronze layer
-- 3. Execute 03_Silver_Layer_Setup.sql to create cleansed tables
-- 4. Execute 04_Gold_Layer_Setup.sql to create analytics views
-- 5. Execute 05_DQ_Framework_Setup.sql to initialize quality checks
-- ============================================================================
