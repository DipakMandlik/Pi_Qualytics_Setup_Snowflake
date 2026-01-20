USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS DQ_INGESTION_WH
WITH
    WAREHOUSE_SIZE      = 'X-SMALL'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'Warehouse for data ingestion and ETL operations';

CREATE WAREHOUSE IF NOT EXISTS DQ_ANALYTICS_WH
WITH
    WAREHOUSE_SIZE      = 'SMALL'
    AUTO_SUSPEND        = 300
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'Warehouse for analytics and data quality checks';




-- =====================================================================================
--               DATABASE & SCHEMA STRUCTURE (MEDALLION ARCHITECTURE)
-- =====================================================================================

-- Main database for the banking data
CREATE DATABASE IF NOT EXISTS BANKING_DW
COMMENT = 'Banking Data Warehouse - Multi-layered architecture';

USE DATABASE BANKING_DW;

-- Layer 1: BRONZE (Raw/Landing Zone)
CREATE SCHEMA IF NOT EXISTS BRONZE
COMMENT = 'Raw data layer - untransformed data from source systems';

-- Layer 2: SILVER (Cleansed/Validated)
CREATE SCHEMA IF NOT EXISTS SILVER
COMMENT = 'Cleansed data layer - validated and standardized data';

-- Layer 3: GOLD (Curated/Analytics-Ready)
CREATE SCHEMA IF NOT EXISTS GOLD
COMMENT = 'Curated data layer - business-ready analytics datasets';






-- Data Quality Framework schemas
CREATE DATABASE IF NOT EXISTS DATA_QUALITY_DB
COMMENT = 'Data Quality Framework database';

USE DATABASE DATA_QUALITY_DB;

CREATE SCHEMA IF NOT EXISTS DQ_CONFIG
COMMENT = 'Data Quality configuration and metadata';

CREATE SCHEMA IF NOT EXISTS DQ_METRICS
COMMENT = 'Data Quality metrics, logs, and results';

CREATE SCHEMA IF NOT EXISTS DQ_ENGINE
COMMENT = 'Data Quality stored procedures and functions';





-- =====================================================================================
--                   FILE FORMAT & STAGE SETUP FOR CSV INGESTION
-- =====================================================================================

USE DATABASE BANKING_DW;
USE SCHEMA BRONZE;
USE WAREHOUSE DQ_INGESTION_WH;

-- Create file format for CSV files
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
    NULL_IF = ('NULL', 'null', '', 'N/A', 'NA', 'n/a')
COMMENT = 'Standard CSV file format for data ingestion';


-- Create internal stage for file uploads
CREATE OR REPLACE STAGE CSV_STAGE
    FILE_FORMAT = CSV_FILE_FORMAT
COMMENT = 'Internal stage for uploading CSV files';




-- =====================================================================================
--                           BRONZE LAYER - RAW DATA TABLES 
-- =====================================================================================


USE SCHEMA BRONZE;

CREATE OR REPLACE TABLE STG_CUSTOMER (
    -- Business Keys
    customer_id                 STRING,
    
    -- Customer Attributes
    customer_name               STRING,
    email                       STRING,
    dob                         STRING,      -- Kept as STRING to capture invalid dates
    phone                       STRING,
    kyc_status                  STRING,
    country                     STRING,
    created_date                STRING,      -- Kept as STRING to capture format variations
    
    -- Metadata Columns (Audit Trail)
    load_timestamp              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)


CREATE OR REPLACE TABLE STG_ACCOUNT (
    -- Business Keys
    account_id                  STRING,
    customer_id                 STRING,      -- Foreign key to customer
    
    -- Account Attributes
    account_type                STRING,
    account_status              STRING,
    opening_balance             STRING,      -- Kept as STRING to capture invalid numbers
    opened_date                 STRING,
    
    -- Metadata Columns
    load_timestamp              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()

)


CREATE OR REPLACE TABLE STG_TRANSACTION (
    -- Business Keys
    transaction_id              STRING,
    account_id                  STRING,
    
    -- Transaction Attributes
    transaction_type            STRING,
    transaction_amount          STRING,      -- Kept as STRING to capture invalid amounts
    transaction_date            STRING,
    
    -- Metadata Columns
    load_timestamp              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)


CREATE OR REPLACE TABLE STG_DAILY_BALANCE (
    -- Business Keys
    account_id                  STRING,
    balance_date                STRING,
    
    -- Balance Attributes
    closing_balance             STRING,      -- Kept as STRING to capture invalid balances
    
    -- Metadata Columns
    load_timestamp              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    
)


CREATE OR REPLACE TABLE STG_FX_RATE (
    -- Business Keys
    currency_code               STRING,
    rate_date                   STRING,
    
    -- Rate Attributes
    fx_rate_to_usd              STRING,      -- Kept as STRING to capture invalid rates
    
    -- Metadata Columns
    load_timestamp              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)


-- =====================================================================================
--                        DATA LOADING 
-- =====================================================================================

use database banking_dw;
use schema bronze;


-- Load Customer data
COPY INTO STG_CUSTOMER (
    customer_id,
    customer_name,
    email,
    dob,
    phone,
    kyc_status,
    country,
    created_date
)
FROM (
    SELECT
        $1,                                     -- customer_id
        $2,                                     -- customer_name
        $3,                                     -- email
        $4,                                     -- dob
        $5,                                     -- phone
        $6,                                     -- kyc_status
        $7,                                     -- country
        $8,                                     -- created_date
    FROM @CSV_STAGE/customer_low_quality_last_next_90_days.csv
)
FILE_FORMAT = (FORMAT_NAME = CSV_FILE_FORMAT)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;




-- Load Account data
COPY INTO STG_ACCOUNT (
    account_id,
    customer_id,
    account_type,
    account_status,
    opening_balance,
    opened_date
)
FROM (
    SELECT
        $1,                                     -- account_id
        $2,                                     -- customer_id
        $3,                                     -- account_type
        $4,                                     -- account_status
        $5,                                     -- opening_balance
        $6                                     -- opened_date
    FROM @CSV_STAGE/STG_ACCOUNT.csv
)
FILE_FORMAT = (FORMAT_NAME = CSV_FILE_FORMAT)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;



-- Load Transaction data
COPY INTO STG_TRANSACTION (
    transaction_id,
    account_id,
    transaction_type,
    transaction_amount,
    transaction_date
)
FROM (
    SELECT
        $1,                                     -- transaction_id
        $2,                                     -- account_id
        $3,                                     -- transaction_type
        $4,                                     -- transaction_amount
        $5                                     -- transaction_date
    FROM @CSV_STAGE/Transaction_03-01-2026.csv
)
FILE_FORMAT = (FORMAT_NAME = CSV_FILE_FORMAT)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;




-- Load Daily Balance data
COPY INTO STG_DAILY_BALANCE (
    account_id,
    balance_date,
    closing_balance
)
FROM (
    SELECT
        $1,                                     -- account_id
        $2,                                     -- balance_date
        $3                                     -- closing_balance
    FROM @CSV_STAGE/Daily_Balance_03-01-2026.csv
)
FILE_FORMAT = (FORMAT_NAME = CSV_FILE_FORMAT)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;




-- Load FX Rate data
COPY INTO STG_FX_RATE (
    currency_code,
    fx_rate_to_usd,
    rate_date
)
FROM (
    SELECT
        $1,                                     -- currency_code
        $2,                                     -- fx_rate_to_usd
        $3                                     -- rate_date
    FROM @CSV_STAGE/fx_rate_low_quality_last_next_90_days.csv
)
FILE_FORMAT = (FORMAT_NAME = CSV_FILE_FORMAT)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;



select * from stg_account;
select * from stg_customer;

select * from stg_daily_balance;


