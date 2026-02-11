-- ============================================================================
-- REGISTER SILVER LAYER TABLES & DQ RULES
-- Pi-Qualytics Data Quality Platform
-- ============================================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_CONFIG;
USE WAREHOUSE DQ_ANALYTICS_WH;
-- ============================================================================
-- 1. REGISTER DATASETS (DATASET_CONFIG)
-- ============================================================================
INSERT INTO DATASET_CONFIG (DATASET_ID, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, BUSINESS_DOMAIN, CRITICALITY, IS_ACTIVE)
VALUES
    ('DS_SILVER_CUSTOMER', 'BANKING_DW', 'SILVER', 'CUSTOMER', 'CUSTOMER_MANAGEMENT', 'CRITICAL', TRUE),
    ('DS_SILVER_ACCOUNT', 'BANKING_DW', 'SILVER', 'ACCOUNT', 'ACCOUNT_MANAGEMENT', 'CRITICAL', TRUE),
    ('DS_SILVER_TRANSACTION', 'BANKING_DW', 'SILVER', 'TRANSACTION', 'TRANSACTION_PROCESSING', 'HIGH', TRUE),
    ('DS_SILVER_DAILY_BALANCE', 'BANKING_DW', 'SILVER', 'DAILY_BALANCE', 'FINANCIAL_REPORTING', 'HIGH', TRUE),
    ('DS_SILVER_FX_RATE', 'BANKING_DW', 'SILVER', 'FX_RATE', 'MARKET_DATA', 'MEDIUM', TRUE);
-- ============================================================================
-- 2. GET RULE IDs (For Mapping)
-- ============================================================================
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
-- ============================================================================
-- 3. MAP RULES TO DATASETS (DATASET_RULE_CONFIG)
-- ============================================================================
INSERT INTO DATASET_RULE_CONFIG (DATASET_ID, RULE_ID, COLUMN_NAME, THRESHOLD_VALUE, IS_ACTIVE)
VALUES
    -- ========== CUSTOMER TABLE ==========
    ('DS_SILVER_CUSTOMER', $rule_completeness, 'customer_id', 100.00, TRUE),
    ('DS_SILVER_CUSTOMER', $rule_uniqueness, 'customer_id', 100.00, TRUE),
    ('DS_SILVER_CUSTOMER', $rule_completeness, 'first_name', 99.00, TRUE),
    ('DS_SILVER_CUSTOMER', $rule_completeness, 'last_name', 99.00, TRUE),
    ('DS_SILVER_CUSTOMER', $rule_completeness, 'email', 95.00, TRUE),
    ('DS_SILVER_CUSTOMER', $rule_email, 'email', 95.00, TRUE),
    ('DS_SILVER_CUSTOMER', $rule_completeness, 'phone', 90.00, TRUE),
    ('DS_SILVER_CUSTOMER', $rule_phone, 'phone', 90.00, TRUE),
    ('DS_SILVER_CUSTOMER', $rule_completeness, 'date_of_birth', 95.00, TRUE),
    ('DS_SILVER_CUSTOMER', $rule_date_format, 'date_of_birth', 95.00, TRUE),
    ('DS_SILVER_CUSTOMER', $rule_completeness, 'kyc_status', 100.00, TRUE),
    ('DS_SILVER_CUSTOMER', $rule_allowed, 'kyc_status', 100.00, TRUE),
    ('DS_SILVER_CUSTOMER', $rule_country, 'country', 98.00, TRUE),
    ('DS_SILVER_CUSTOMER', $rule_volume, NULL, 100.00, TRUE),
    -- ========== ACCOUNT TABLE ==========
    ('DS_SILVER_ACCOUNT', $rule_completeness, 'account_id', 100.00, TRUE),
    ('DS_SILVER_ACCOUNT', $rule_uniqueness, 'account_id', 100.00, TRUE),
    ('DS_SILVER_ACCOUNT', $rule_completeness, 'customer_id', 100.00, TRUE),
    -- Assuming Check against CUSTOMER table for FK
    ('DS_SILVER_ACCOUNT', $rule_fk, 'customer_id', 98.00, TRUE), 
    ('DS_SILVER_ACCOUNT', $rule_completeness, 'account_type', 100.00, TRUE),
    ('DS_SILVER_ACCOUNT', $rule_allowed, 'account_type', 100.00, TRUE),
    ('DS_SILVER_ACCOUNT', $rule_completeness, 'status', 100.00, TRUE),
    ('DS_SILVER_ACCOUNT', $rule_allowed, 'status', 100.00, TRUE),
    ('DS_SILVER_ACCOUNT', $rule_completeness, 'balance', 100.00, TRUE),
    ('DS_SILVER_ACCOUNT', $rule_numeric, 'balance', 100.00, TRUE),
    ('DS_SILVER_ACCOUNT', $rule_completeness, 'currency', 100.00, TRUE),
    ('DS_SILVER_ACCOUNT', $rule_currency, 'currency', 100.00, TRUE),
    ('DS_SILVER_ACCOUNT', $rule_volume, NULL, 100.00, TRUE),
    -- ========== TRANSACTION TABLE ==========
    ('DS_SILVER_TRANSACTION', $rule_completeness, 'transaction_id', 100.00, TRUE),
    ('DS_SILVER_TRANSACTION', $rule_uniqueness, 'transaction_id', 100.00, TRUE),
    ('DS_SILVER_TRANSACTION', $rule_completeness, 'account_id', 100.00, TRUE),
    ('DS_SILVER_TRANSACTION', $rule_fk, 'account_id', 98.00, TRUE),
    ('DS_SILVER_TRANSACTION', $rule_completeness, 'transaction_type', 100.00, TRUE),
    ('DS_SILVER_TRANSACTION', $rule_allowed, 'transaction_type', 100.00, TRUE),
    ('DS_SILVER_TRANSACTION', $rule_completeness, 'amount', 100.00, TRUE),
    ('DS_SILVER_TRANSACTION', $rule_numeric, 'amount', 100.00, TRUE),
    ('DS_SILVER_TRANSACTION', $rule_completeness, 'transaction_date', 100.00, TRUE),
    ('DS_SILVER_TRANSACTION', $rule_freshness, 'transaction_date', 95.00, TRUE),
    ('DS_SILVER_TRANSACTION', $rule_volume, NULL, 100.00, TRUE),
    -- ========== DAILY BALANCE TABLE ==========
    ('DS_SILVER_DAILY_BALANCE', $rule_completeness, 'balance_id', 100.00, TRUE),
    ('DS_SILVER_DAILY_BALANCE', $rule_uniqueness, 'balance_id', 100.00, TRUE),
    ('DS_SILVER_DAILY_BALANCE', $rule_completeness, 'account_id', 100.00, TRUE),
    ('DS_SILVER_DAILY_BALANCE', $rule_fk, 'account_id', 98.00, TRUE),
    ('DS_SILVER_DAILY_BALANCE', $rule_completeness, 'balance_date', 100.00, TRUE),
    ('DS_SILVER_DAILY_BALANCE', $rule_completeness, 'closing_balance', 100.00, TRUE),
    ('DS_SILVER_DAILY_BALANCE', $rule_numeric, 'closing_balance', 100.00, TRUE),
    ('DS_SILVER_DAILY_BALANCE', $rule_volume, NULL, 100.00, TRUE),
    -- ========== FX RATE TABLE ==========
    ('DS_SILVER_FX_RATE', $rule_completeness, 'rate_id', 100.00, TRUE),
    ('DS_SILVER_FX_RATE', $rule_uniqueness, 'rate_id', 100.00, TRUE),
    ('DS_SILVER_FX_RATE', $rule_completeness, 'to_currency', 100.00, TRUE),
    ('DS_SILVER_FX_RATE', $rule_currency, 'to_currency', 100.00, TRUE),
    ('DS_SILVER_FX_RATE', $rule_completeness, 'exchange_rate', 100.00, TRUE),
    ('DS_SILVER_FX_RATE', $rule_positive, 'exchange_rate', 100.00, TRUE),
    ('DS_SILVER_FX_RATE', $rule_completeness, 'rate_date', 100.00, TRUE),
    ('DS_SILVER_FX_RATE', $rule_freshness, 'rate_date', 95.00, TRUE),
    ('DS_SILVER_FX_RATE', $rule_volume, NULL, 100.00, TRUE);
-- ============================================================================
-- 4. CONFIGURE ALLOWED VALUES
-- ============================================================================
INSERT INTO ALLOWED_VALUES_CONFIG (DATASET_ID, COLUMN_NAME, ALLOWED_VALUES, IS_ACTIVE)
VALUES
    ('DS_SILVER_CUSTOMER', 'kyc_status', '''VERIFIED'',''PENDING'',''REJECTED'',''EXPIRED'',''NOT_STARTED''', TRUE),
    ('DS_SILVER_ACCOUNT', 'account_type', '''SAVINGS'',''CHECKING'',''CREDIT'',''INVESTMENT'',''LOAN''', TRUE),
    ('DS_SILVER_ACCOUNT', 'status', '''ACTIVE'',''CLOSED'',''SUSPENDED'',''PENDING''', TRUE),
    ('DS_SILVER_TRANSACTION', 'transaction_type', '''DEPOSIT'',''WITHDRAWAL'',''TRANSFER'',''PAYMENT'',''FEE'',''INTEREST''', TRUE);
SELECT 'Silver Layer Registration Complete!' as Status;




select * from dataset_config;

select * from dataset_rule_config;



select * from data_quality_db.dq_metrics.dq_daily_summary;



ALTER TABLE DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
ADD COLUMN RUN_TYPE STRING COMMENT 'FULL | INCREMENTAL | CDC';


ALTER TABLE DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
ADD COLUMN SCAN_SCOPE STRING COMMENT 'FULL_TABLE | PARTIAL_DATA';

ALTER TABLE DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
ADD COLUMN SCAN_REASON STRING COMMENT 'BASELINE | SCHEDULED | MANUAL';


-- =================================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE DQ_ANALYTICS_WH;
USE DATABASE BANKING_DW;


USE SCHEMA BRONZE;

CREATE OR REPLACE TABLE TESTING (
    customer_id STRING,
    email STRING,
    created_at STRING
);



INSERT INTO TESTING VALUES
('C001', 'a@test.com', '2025-01-01'),
('C002', 'b@test.com', '2025-01-01'),
('C003', NULL, '2025-01-01'),          -- NULL email
('C004', 'd@test.com', '2025-01-01'),
('C005', NULL, '2025-01-01'),          -- NULL email
('C006', 'f@test.com', '2025-01-01'),
('C007', 'g@test.com', '2025-01-01'),
('C008', 'h@test.com', '2025-01-01'),
('C009', 'i@test.com', '2025-01-01'),
('C010', 'j@test.com', '2025-01-01');



INSERT INTO TESTING VALUES
('C011', '0@test.com', '2025-01-01'),
('C012', 'a$test.com', '2025-01-01');






USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_CONFIG;

INSERT INTO DATASET_CONFIG (
    DATASET_ID,
    SOURCE_DATABASE,
    SOURCE_SCHEMA,
    SOURCE_TABLE,
    BUSINESS_DOMAIN,
    CRITICALITY,
    IS_ACTIVE
)
VALUES (
    'DS_BRONZE_TESTING',
    'BANKING_DW',
    'BRONZE',
    'TESTING',
    'TESTING_DOMAIN',
    'CRITICAL',
    TRUE
);


SET rule_completeness = (
    SELECT RULE_ID
    FROM DATA_QUALITY_DB.DQ_CONFIG.RULE_MASTER
    WHERE RULE_NAME = 'COMPLETENESS_CHECK'
      AND IS_ACTIVE = TRUE
    ORDER BY RULE_ID DESC
    LIMIT 1
);


SET rule_validity = (
    SELECT RULE_ID
    FROM DATA_QUALITY_DB.DQ_CONFIG.RULE_MASTER
    WHERE RULE_NAME = 'VALIDITY_EMAIL_FORMAT'
      AND IS_ACTIVE = TRUE
    ORDER BY RULE_ID DESC
    LIMIT 1
);

INSERT INTO DATASET_RULE_CONFIG (
    DATASET_ID,
    RULE_ID,
    COLUMN_NAME,
    THRESHOLD_VALUE,
    IS_ACTIVE
)
VALUES (
    'DS_BRONZE_TESTING',
    $rule_completeness,
    'EMAIL',
    95.00,
    TRUE
);




INSERT INTO DATA_QUALITY_DB.DQ_CONFIG.DATASET_RULE_CONFIG (
    DATASET_ID,
    RULE_ID,
    COLUMN_NAME,
    THRESHOLD_VALUE,
    IS_ACTIVE
)
VALUES (
    'DS_BRONZE_TESTING',
    $rule_validity,
    'EMAIL',
    95.00,
    TRUE
);




CALL DATA_QUALITY_DB.DQ_ENGINE.SP_EXECUTE_DQ_CHECKS(
    'DS_BRONZE_TESTING',
    NULL,
    'FULL'
);




SELECT
    RUN_ID,
    START_TS,
    END_TS,
    RUN_STATUS,
    TOTAL_CHECKS,
    FAILED_CHECKS
FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
ORDER BY START_TS DESC
LIMIT 1;





SELECT
    DATASET_ID,
    DATABASE_NAME,
    SCHEMA_NAME,
    TABLE_NAME,
    COLUMN_NAME,
    TOTAL_RECORDS,
    VALID_RECORDS,
    INVALID_RECORDS,
    PASS_RATE,
    THRESHOLD,
    CHECK_STATUS
FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
WHERE DATASET_ID = 'DS_BRONZE_TESTING'
ORDER BY CHECK_TIMESTAMP DESC;







-- Testing Schema Implementation 
-- ==================================================================================================================================



USE ROLE ACCOUNTADMIN;
USE DATABASE BANKING_DW;

USE SCHEMA TESTING;

CREATE SCHEMA BANKING_DW.TESTING
COMMENT = 'Controlled testing schema for DQ incremental validation. Production-like but isolated.';


CREATE OR REPLACE TABLE BANKING_DW.TESTING.STG_ACCOUNT_TEST (
    ACCOUNT_ID VARCHAR,
    CUSTOMER_ID VARCHAR,
    ACCOUNT_TYPE VARCHAR,
    ACCOUNT_STATUS VARCHAR,
    OPENING_BALANCE VARCHAR,
    OPENED_DATE VARCHAR,
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_FILE VARCHAR,
    LOAD_BATCH_ID VARCHAR
)
COMMENT = 'Testing table with incremental watermark column';



CREATE OR REPLACE TABLE BANKING_DW.TESTING.STG_CUSTOMER_TEST (
    CUSTOMER_ID VARCHAR,
    CUSTOMER_NAME VARCHAR,
    EMAIL VARCHAR,
    PHONE VARCHAR,
    CREATED_DATE VARCHAR
)
COMMENT = 'Testing table without incremental column (forces FULL scan)';


-- We freeze Bronze/Silver immediately.


UPDATE data_quality_db.dq_config.dataset_config
SET IS_ACTIVE = FALSE,
    MODIFIED_BY=CURRENT_USER(),
    MODIFIED_TS=CURRENT_TIMESTAMP();



-- Inserting into data_config_Table
INSERT INTO DATA_QUALITY_DB.DQ_CONFIG.DATASET_CONFIG (
    DATASET_ID,
    SOURCE_DATABASE,
    SOURCE_SCHEMA,
    SOURCE_TABLE,
    BUSINESS_DOMAIN,
    CRITICALITY,
    IS_ACTIVE,
    INCREMENTAL_COLUMN
)
VALUES (
    'DS_STG_ACCOUNT_TEST',
    'BANKING_DW',
    'TESTING',
    'STG_ACCOUNT_TEST',
    'BANKING',
    'MEDIUM',
    TRUE,
    'LOAD_TIMESTAMP'
);


INSERT INTO DATA_QUALITY_DB.DQ_CONFIG.DATASET_CONFIG (
    DATASET_ID,
    SOURCE_DATABASE,
    SOURCE_SCHEMA,
    SOURCE_TABLE,
    BUSINESS_DOMAIN,
    CRITICALITY,
    IS_ACTIVE,
    INCREMENTAL_COLUMN
)
VALUES (
    'DS_STG_CUSTOMER_TEST',
    'BANKING_DW',
    'TESTING',
    'STG_CUSTOMER_TEST',
    'BANKING',
    'MEDIUM',
    TRUE,
    NULL
);


SELECT DATASET_ID, SOURCE_SCHEMA, SOURCE_TABLE, IS_ACTIVE, INCREMENTAL_COLUMN
FROM DATA_QUALITY_DB.DQ_CONFIG.DATASET_CONFIG
WHERE IS_ACTIVE = TRUE;

use database data_quality_db;
use schema dq_config;

-- Get Only Required Rule IDs
SET rule_completeness = (
  SELECT RULE_ID FROM RULE_MASTER 
  WHERE RULE_NAME = 'COMPLETENESS_CHECK' 
  AND IS_ACTIVE = TRUE LIMIT 1
);

SET rule_uniqueness = (
  SELECT RULE_ID FROM RULE_MASTER 
  WHERE RULE_NAME = 'UNIQUENESS_PRIMARY_KEY' 
  AND IS_ACTIVE = TRUE LIMIT 1
);

SET rule_numeric = (
  SELECT RULE_ID FROM RULE_MASTER 
  WHERE RULE_NAME = 'VALIDITY_NUMERIC_FORMAT' 
  AND IS_ACTIVE = TRUE LIMIT 1
);

SET rule_volume = (
  SELECT RULE_ID FROM RULE_MASTER 
  WHERE RULE_NAME = 'VOLUME_ROW_COUNT_THRESHOLD' 
  AND IS_ACTIVE = TRUE LIMIT 1
);


-- Configure STG_ACCOUNT_TEST (Incremental)

INSERT INTO DATASET_RULE_CONFIG 
(DATASET_ID, RULE_ID, COLUMN_NAME, THRESHOLD_VALUE, IS_ACTIVE)
VALUES
-- Primary key completeness
('DS_STG_ACCOUNT_TEST', $rule_completeness, 'ACCOUNT_ID', 100.00, TRUE),

-- Primary key uniqueness
('DS_STG_ACCOUNT_TEST', $rule_uniqueness, 'ACCOUNT_ID', 100.00, TRUE),

-- Numeric validation
('DS_STG_ACCOUNT_TEST', $rule_numeric, 'OPENING_BALANCE', 95.00, TRUE),

-- Volume check
('DS_STG_ACCOUNT_TEST', $rule_volume, NULL, 100.00, TRUE);


-- Configure STG_CUSTOMER_TEST (Full Scan)

INSERT INTO DATASET_RULE_CONFIG 
(DATASET_ID, RULE_ID, COLUMN_NAME, THRESHOLD_VALUE, IS_ACTIVE)
VALUES
('DS_STG_CUSTOMER_TEST', $rule_completeness, 'CUSTOMER_ID', 100.00, TRUE),
('DS_STG_CUSTOMER_TEST', $rule_uniqueness, 'CUSTOMER_ID', 100.00, TRUE),
('DS_STG_CUSTOMER_TEST', $rule_completeness, 'EMAIL', 95.00, TRUE),
('DS_STG_CUSTOMER_TEST', $rule_volume, NULL, 100.00, TRUE);




-- Loading data for the testing purpose 

INSERT INTO BANKING_DW.TESTING.STG_ACCOUNT_TEST
(
    ACCOUNT_ID,
    CUSTOMER_ID,
    ACCOUNT_TYPE,
    ACCOUNT_STATUS,
    OPENING_BALANCE,
    OPENED_DATE,
    LOAD_TIMESTAMP,
    SOURCE_FILE,
    LOAD_BATCH_ID
)
VALUES
('ACC3001','CUST701','SAVINGS','ACTIVE','15000.00','2023-05-10',DATEADD(DAY,-1,CURRENT_TIMESTAMP()),'acc_yesterday.csv','BATCH_Y_01'),
('ACC3002','CUST702','CHECKING','ACTIVE','8200.00','2022-09-11',DATEADD(DAY,-1,CURRENT_TIMESTAMP()),'acc_yesterday.csv','BATCH_Y_01'),
('ACC3003','CUST703','SAVINGS','ACTIVE','1200.00','2021-01-01',DATEADD(DAY,-1,CURRENT_TIMESTAMP()),'acc_yesterday.csv','BATCH_Y_01'),
('ACC3004','CUST704','SAVINGS','ACTIVE','5400.00','2023-03-15',DATEADD(DAY,-1,CURRENT_TIMESTAMP()),'acc_yesterday.csv','BATCH_Y_01'),
('ACC3005','CUST705','CHECKING','ACTIVE','7500.00','2023-08-19',DATEADD(DAY,-1,CURRENT_TIMESTAMP()),'acc_yesterday.csv','BATCH_Y_01'),
('ACC3006','CUST706','SAVINGS','ACTIVE','3000.00','2024-02-01',DATEADD(DAY,-1,CURRENT_TIMESTAMP()),'acc_yesterday.csv','BATCH_Y_01'),
('ACC3007','CUST707','SAVINGS','ACTIVE','4500.00','2024-01-01',DATEADD(DAY,-1,CURRENT_TIMESTAMP()),'acc_yesterday.csv','BATCH_Y_01'),
('ACC3008','CUST708','CHECKING','ACTIVE','9800.00','2023-09-09',DATEADD(DAY,-1,CURRENT_TIMESTAMP()),'acc_yesterday.csv','BATCH_Y_01'),
('ACC3009','CUST709','SAVINGS','ACTIVE','6700.00','2022-12-12',DATEADD(DAY,-1,CURRENT_TIMESTAMP()),'acc_yesterday.csv','BATCH_Y_01'),
('ACC3010','CUST710','SAVINGS','ACTIVE','11200.00','2023-07-07',DATEADD(DAY,-1,CURRENT_TIMESTAMP()),'acc_yesterday.csv','BATCH_Y_01');




INSERT INTO BANKING_DW.TESTING.STG_ACCOUNT_TEST
(
    ACCOUNT_ID,
    CUSTOMER_ID,
    ACCOUNT_TYPE,
    ACCOUNT_STATUS,
    OPENING_BALANCE,
    OPENED_DATE,
    LOAD_TIMESTAMP,
    SOURCE_FILE,
    LOAD_BATCH_ID
)
VALUES
('ACC3011','CUST711','SAVINGS','ACTIVE','20000.00','2023-04-10',CURRENT_TIMESTAMP(),'acc_today.csv','BATCH_T_01'),

-- NULL account_id
(NULL,'CUST712','CHECKING','ACTIVE','8000.00','2023-10-11',CURRENT_TIMESTAMP(),'acc_today.csv','BATCH_T_01'),

-- Duplicate
('ACC3013','CUST713','SAVINGS','ACTIVE','1200.00','2022-01-01',CURRENT_TIMESTAMP(),'acc_today.csv','BATCH_T_01'),
('ACC3013','CUST714','SAVINGS','ACTIVE','1500.00','2022-01-01',CURRENT_TIMESTAMP(),'acc_today.csv','BATCH_T_01'),

-- Invalid type
('ACC3015','CUST715','CURRENT','ACTIVE','5400.00','2023-03-15',CURRENT_TIMESTAMP(),'acc_today.csv','BATCH_T_01'),

-- Invalid status
('ACC3016','CUST716','SAVINGS','BLOCKED','7500.00','2023-08-19',CURRENT_TIMESTAMP(),'acc_today.csv','BATCH_T_01'),

-- Non numeric balance
('ACC3017','CUST717','CHECKING','ACTIVE','XYZ','2024-02-01',CURRENT_TIMESTAMP(),'acc_today.csv','BATCH_T_01'),

-- Negative balance
('ACC3018','CUST718','SAVINGS','ACTIVE','-4500.00','2024-01-01',CURRENT_TIMESTAMP(),'acc_today.csv','BATCH_T_01'),

-- Invalid date
('ACC3019','CUST719','SAVINGS','ACTIVE','3000.00','31-02-2023',CURRENT_TIMESTAMP(),'acc_today.csv','BATCH_T_01'),

-- NULL customer
('ACC3020',NULL,'CHECKING','ACTIVE','9800.00','2023-09-09',CURRENT_TIMESTAMP(),'acc_today.csv','BATCH_T_01');





-- loading for the customer

INSERT INTO BANKING_DW.TESTING.STG_CUSTOMER_TEST
VALUES
('CUST701','Rahul Mehta','rahul.mehta@gmail.com','9876501001',DATEADD(DAY,-1,CURRENT_TIMESTAMP())),
('CUST702','Neha Sharma','neha.sharma@gmail.com','9876501002',DATEADD(DAY,-1,CURRENT_TIMESTAMP())),
('CUST703','Amit Kapoor','amit.kapoor@gmail.com','9876501003',DATEADD(DAY,-1,CURRENT_TIMESTAMP())),
('CUST704','Pooja Nair','pooja.nair@gmail.com','9876501004',DATEADD(DAY,-1,CURRENT_TIMESTAMP())),
('CUST705','Ravi Iyer','ravi.iyer@gmail.com','9876501005',DATEADD(DAY,-1,CURRENT_TIMESTAMP())),
('CUST706','Sneha Das','sneha.das@gmail.com','9876501006',DATEADD(DAY,-1,CURRENT_TIMESTAMP())),
('CUST707','Kiran Rao','kiran.rao@gmail.com','9876501007',DATEADD(DAY,-1,CURRENT_TIMESTAMP())),
('CUST708','Anjali Gupta','anjali.gupta@gmail.com','9876501008',DATEADD(DAY,-1,CURRENT_TIMESTAMP())),
('CUST709','Varun Joshi','varun.joshi@gmail.com','9876501009',DATEADD(DAY,-1,CURRENT_TIMESTAMP())),
('CUST710','Divya Menon','divya.menon@gmail.com','9876501010',DATEADD(DAY,-1,CURRENT_TIMESTAMP()));


INSERT INTO BANKING_DW.TESTING.STG_CUSTOMER_TEST
VALUES
('CUST711','Rohan Malik','rohan.malik@gmail.com','9876502001',CURRENT_TIMESTAMP()),

-- NULL first name
('CUST712',NULL,'kapoor@gmail.com','9876502002',CURRENT_TIMESTAMP()),

-- Invalid email
('CUST713','Anita Shah','anita.shahgmail.com','9876502003',CURRENT_TIMESTAMP()),

-- Invalid phone
('CUST714','Kunal Mehra','kunal.mehra@gmail.com','12345',CURRENT_TIMESTAMP()),

-- Duplicate
('CUST715','Deepa Rao','deepa.rao@gmail.com','9876502005',CURRENT_TIMESTAMP()),
('CUST715','Deepa Rao','deepa.rao@gmail.com','9876502005',CURRENT_TIMESTAMP()),

-- NULL email
('CUST716','Sahil Jain',NULL,'9876502006',CURRENT_TIMESTAMP()),

-- Blank fields
('CUST717','','invalid@email','',CURRENT_TIMESTAMP()),

-- Future timestamp
('CUST718','Future User','future.user@gmail.com','9876502008',DATEADD(DAY,1,CURRENT_TIMESTAMP())),

-- All null except id
('CUST719',NULL,NULL,NULL,CURRENT_TIMESTAMP());















-- Just check which tables have data with today's LOAD_TIMESTAMP
SELECT 
    'STG_CUSTOMER' AS TABLE_NAME,
    COUNT(*) AS ROWS_WITH_TODAY_TIMESTAMP
FROM BANKING_DW.BRONZE.STG_CUSTOMER
WHERE LOAD_TIMESTAMP >= CURRENT_DATE()::TIMESTAMP_NTZ

UNION ALL

SELECT 
    'STG_ACCOUNT',
    COUNT(*)
FROM BANKING_DW.BRONZE.STG_ACCOUNT
WHERE LOAD_TIMESTAMP >= CURRENT_DATE()::TIMESTAMP_NTZ

UNION ALL

SELECT 
    'STG_TRANSACTION',
    COUNT(*)
FROM BANKING_DW.BRONZE.STG_TRANSACTION
WHERE LOAD_TIMESTAMP >= CURRENT_DATE()::TIMESTAMP_NTZ;





SELECT 
    RUN_ID,
    START_TS,
    ROW_LEVEL_RECORDS_PROCESSED,
    METADATA_RECORDS_PROCESSED,
    INCREMENTAL_ROWS_LOADED
FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
WHERE RUN_TYPE = 'INCREMENTAL'
  AND START_TS >= CURRENT_DATE()
ORDER BY START_TS DESC;




-- What does the engine consider "today"?
SELECT 
    CURRENT_DATE()::TIMESTAMP_NTZ AS TODAY_START,
    CURRENT_TIMESTAMP() AS RIGHT_NOW;

-- How many rows match the EXACT filter the engine uses?
SELECT COUNT(*) 
FROM BANKING_DW.BRONZE.STG_ACCOUNT
WHERE LOAD_TIMESTAMP IS NOT NULL
  AND LOAD_TIMESTAMP >= CURRENT_DATE()::TIMESTAMP;



  SELECT 
    DATASET_ID,
    TABLE_NAME,
    SCAN_SCOPE,
    COUNT(*) AS CHECK_COUNT,
    SUM(TOTAL_RECORDS) AS TOTAL_RECORDS_PROCESSED
FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
WHERE RUN_ID = 'DQ_INC_20260210_034306_20e3d9cd'
GROUP BY DATASET_ID, TABLE_NAME, SCAN_SCOPE
ORDER BY TOTAL_RECORDS_PROCESSED DESC;






-- Check what the stored procedure is actually seeing
SELECT 
    COUNT(*) AS TOTAL_ROWS,
    COUNT(CASE WHEN LOAD_TIMESTAMP IS NOT NULL AND LOAD_TIMESTAMP >= CURRENT_DATE()::TIMESTAMP_NTZ THEN 1 END) AS TODAY_ROWS,
    MIN(LOAD_TIMESTAMP) AS EARLIEST_LOAD,
    MAX(LOAD_TIMESTAMP) AS LATEST_LOAD
FROM BANKING_DW.BRONZE.STG_ACCOUNT;




select *from banking_dw.silver.account ;


select * from data_quality_db.dq_metrics.dq_run_control;


select * from data_quality_db.dq_metrics.dq_check_results;


-- Get the 3 datasets that were processed in your last run
SELECT 
    DATASET_ID,
    TABLE_NAME,
    SCAN_SCOPE,
    SCAN_REASON,
    COUNT(DISTINCT RULE_ID) AS RULES_APPLIED,
    SUM(TOTAL_RECORDS) AS TOTAL_VALIDATIONS,
    MAX(TOTAL_RECORDS) AS ROWS_PER_CHECK
FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
WHERE RUN_ID = 'DQ_INC_20260210_051044_9377e5dc'
GROUP BY DATASET_ID, TABLE_NAME, SCAN_SCOPE, SCAN_REASON
ORDER BY TOTAL_VALIDATIONS DESC;


select * from dq_check_results;




select * from dq_run_control;




select * from data_quality_db.dq_config.dataset_config;



-- =========================================================================================

select * from data_quality_db.dq_metrics.dq_check_results where run_id='DQ_INC_20260210_233917_e06b9b28';


select * from data_quality_db.dq_metrics.dq_daily_summary;