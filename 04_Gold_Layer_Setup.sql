-- ============================================================================
-- GOLD LAYER SETUP - ANALYTICS & REPORTING VIEWS
-- Pi-Qualytics Data Quality Platform
-- ============================================================================
-- Purpose: Create business-ready analytics views and aggregations
-- Prerequisites: 03_Silver_Layer_Setup.sql executed and populated
-- Version: 1.0.0
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE BANKING_DW;
USE SCHEMA GOLD;
USE WAREHOUSE DQ_ANALYTICS_WH;

-- ============================================================================
-- SECTION 1: CUSTOMER ANALYTICS VIEWS
-- ============================================================================

-- Customer 360 View
CREATE OR REPLACE VIEW VW_CUSTOMER_360 AS
SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.dob,
    DATEDIFF(YEAR, c.dob, CURRENT_DATE()) AS age,
    c.phone,
    c.kyc_status,
    c.country,
    c.created_date,
    DATEDIFF(DAY, c.created_date, CURRENT_DATE()) AS customer_tenure_days,
    
    -- Account Metrics
    COUNT(DISTINCT a.account_id) AS total_accounts,
    SUM(CASE WHEN a.account_status = 'Active' THEN 1 ELSE 0 END) AS active_accounts,
    SUM(a.opening_balance) AS total_opening_balance,
    
    -- Transaction Metrics
    COUNT(DISTINCT t.transaction_id) AS total_transactions,
    SUM(t.transaction_amount) AS total_transaction_volume,
    AVG(t.transaction_amount) AS avg_transaction_amount,
    MAX(t.transaction_date) AS last_transaction_date,
    
    -- Data Quality Metrics
    c.dq_score AS customer_dq_score,
    c.is_valid AS customer_is_valid,
    
    -- Metadata
    c.load_timestamp,
    c.updated_timestamp
FROM BANKING_DW.SILVER.CUSTOMER c
LEFT JOIN BANKING_DW.SILVER.ACCOUNT a ON c.customer_id = a.customer_id
LEFT JOIN BANKING_DW.SILVER.TRANSACTION t ON a.account_id = t.account_id
GROUP BY
    c.customer_id, c.customer_name, c.email, c.dob, c.phone, c.kyc_status,
    c.country, c.created_date, c.dq_score, c.is_valid, c.load_timestamp, c.updated_timestamp;

-- Customer Segmentation View
CREATE OR REPLACE VIEW VW_CUSTOMER_SEGMENTATION AS
SELECT
    customer_id,
    customer_name,
    country,
    kyc_status,
    total_accounts,
    total_transaction_volume,
    
    -- Segmentation Logic
    CASE
        WHEN total_transaction_volume >= 100000 THEN 'Premium'
        WHEN total_transaction_volume >= 50000 THEN 'Gold'
        WHEN total_transaction_volume >= 10000 THEN 'Silver'
        ELSE 'Bronze'
    END AS customer_segment,
    
    CASE
        WHEN total_transactions >= 100 THEN 'High Activity'
        WHEN total_transactions >= 20 THEN 'Medium Activity'
        ELSE 'Low Activity'
    END AS activity_level,
    
    CASE
        WHEN customer_tenure_days >= 365 THEN 'Loyal'
        WHEN customer_tenure_days >= 90 THEN 'Established'
        ELSE 'New'
    END AS tenure_category
    
FROM VW_CUSTOMER_360;

-- ============================================================================
-- SECTION 2: ACCOUNT ANALYTICS VIEWS
-- ============================================================================

-- Account Summary View
CREATE OR REPLACE VIEW VW_ACCOUNT_SUMMARY AS
SELECT
    a.account_id,
    a.customer_id,
    c.customer_name,
    a.account_type,
    a.account_status,
    a.opening_balance,
    a.opened_date,
    DATEDIFF(DAY, a.opened_date, CURRENT_DATE()) AS account_age_days,
    
    -- Transaction Metrics
    COUNT(DISTINCT t.transaction_id) AS total_transactions,
    SUM(CASE WHEN t.transaction_type = 'Deposit' THEN t.transaction_amount ELSE 0 END) AS total_deposits,
    SUM(CASE WHEN t.transaction_type = 'Withdrawal' THEN t.transaction_amount ELSE 0 END) AS total_withdrawals,
    SUM(t.transaction_amount) AS net_transaction_amount,
    
    -- Latest Balance
    (SELECT closing_balance 
     FROM BANKING_DW.SILVER.DAILY_BALANCE db 
     WHERE db.account_id = a.account_id 
     ORDER BY balance_date DESC 
     LIMIT 1) AS current_balance,
    
    -- Data Quality
    a.dq_score AS account_dq_score,
    a.is_valid AS account_is_valid
    
FROM BANKING_DW.SILVER.ACCOUNT a
LEFT JOIN BANKING_DW.SILVER.CUSTOMER c ON a.customer_id = c.customer_id
LEFT JOIN BANKING_DW.SILVER.TRANSACTION t ON a.account_id = t.account_id
GROUP BY
    a.account_id, a.customer_id, c.customer_name, a.account_type, a.account_status,
    a.opening_balance, a.opened_date, a.dq_score, a.is_valid;

-- ============================================================================
-- SECTION 3: TRANSACTION ANALYTICS VIEWS
-- ============================================================================

-- Daily Transaction Summary
CREATE OR REPLACE VIEW VW_DAILY_TRANSACTION_SUMMARY AS
SELECT
    t.transaction_date,
    t.transaction_type,
    COUNT(DISTINCT t.transaction_id) AS transaction_count,
    COUNT(DISTINCT t.account_id) AS unique_accounts,
    SUM(t.transaction_amount) AS total_amount,
    AVG(t.transaction_amount) AS avg_amount,
    MIN(t.transaction_amount) AS min_amount,
    MAX(t.transaction_amount) AS max_amount,
    STDDEV(t.transaction_amount) AS stddev_amount
FROM BANKING_DW.SILVER.TRANSACTION t
WHERE t.is_valid = TRUE
GROUP BY t.transaction_date, t.transaction_type
ORDER BY t.transaction_date DESC, t.transaction_type;

-- Transaction Trend Analysis
CREATE OR REPLACE VIEW VW_TRANSACTION_TRENDS AS
SELECT
    DATE_TRUNC('MONTH', transaction_date) AS month,
    transaction_type,
    COUNT(*) AS transaction_count,
    SUM(transaction_amount) AS total_volume,
    AVG(transaction_amount) AS avg_transaction_size,
    COUNT(DISTINCT account_id) AS active_accounts
FROM BANKING_DW.SILVER.TRANSACTION
WHERE is_valid = TRUE
GROUP BY DATE_TRUNC('MONTH', transaction_date), transaction_type
ORDER BY month DESC, transaction_type;

-- ============================================================================
-- SECTION 4: DATA QUALITY ANALYTICS VIEWS
-- ============================================================================

-- Data Quality Scorecard
CREATE OR REPLACE VIEW VW_DATA_QUALITY_SCORECARD AS
SELECT
    'CUSTOMER' AS entity_type,
    COUNT(*) AS total_records,
    SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END) AS valid_records,
    SUM(CASE WHEN is_valid = FALSE THEN 1 ELSE 0 END) AS invalid_records,
    ROUND(AVG(dq_score), 2) AS avg_dq_score,
    ROUND((SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) AS validity_percentage
FROM BANKING_DW.SILVER.CUSTOMER

UNION ALL

SELECT
    'ACCOUNT',
    COUNT(*),
    SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_valid = FALSE THEN 1 ELSE 0 END),
    ROUND(AVG(dq_score), 2),
    ROUND((SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2)
FROM BANKING_DW.SILVER.ACCOUNT

UNION ALL

SELECT
    'TRANSACTION',
    COUNT(*),
    SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_valid = FALSE THEN 1 ELSE 0 END),
    ROUND(AVG(dq_score), 2),
    ROUND((SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2)
FROM BANKING_DW.SILVER.TRANSACTION

UNION ALL

SELECT
    'DAILY_BALANCE',
    COUNT(*),
    SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_valid = FALSE THEN 1 ELSE 0 END),
    ROUND(AVG(dq_score), 2),
    ROUND((SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2)
FROM BANKING_DW.SILVER.DAILY_BALANCE

UNION ALL

SELECT
    'FX_RATE',
    COUNT(*),
    SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_valid = FALSE THEN 1 ELSE 0 END),
    ROUND(AVG(dq_score), 2),
    ROUND((SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2)
FROM BANKING_DW.SILVER.FX_RATE;

-- Data Quality Issues View
CREATE OR REPLACE VIEW VW_DATA_QUALITY_ISSUES AS
SELECT
    'CUSTOMER' AS entity_type,
    customer_id AS entity_id,
    customer_name AS entity_name,
    dq_score,
    validation_errors,
    load_timestamp
FROM BANKING_DW.SILVER.CUSTOMER
WHERE is_valid = FALSE

UNION ALL

SELECT
    'ACCOUNT',
    account_id,
    account_type,
    dq_score,
    validation_errors,
    load_timestamp
FROM BANKING_DW.SILVER.ACCOUNT
WHERE is_valid = FALSE

UNION ALL

SELECT
    'TRANSACTION',
    transaction_id,
    transaction_type,
    dq_score,
    validation_errors,
    load_timestamp
FROM BANKING_DW.SILVER.TRANSACTION
WHERE is_valid = FALSE

ORDER BY load_timestamp DESC;

-- ============================================================================
-- SECTION 5: EXECUTIVE DASHBOARD VIEWS
-- ============================================================================

-- Executive KPI Dashboard
CREATE OR REPLACE VIEW VW_EXECUTIVE_DASHBOARD AS
SELECT
    -- Customer Metrics
    (SELECT COUNT(*) FROM BANKING_DW.SILVER.CUSTOMER WHERE is_valid = TRUE) AS total_customers,
    (SELECT COUNT(*) FROM BANKING_DW.SILVER.CUSTOMER WHERE kyc_status = 'Verified') AS verified_customers,
    
    -- Account Metrics
    (SELECT COUNT(*) FROM BANKING_DW.SILVER.ACCOUNT WHERE is_valid = TRUE) AS total_accounts,
    (SELECT COUNT(*) FROM BANKING_DW.SILVER.ACCOUNT WHERE account_status = 'Active') AS active_accounts,
    (SELECT SUM(opening_balance) FROM BANKING_DW.SILVER.ACCOUNT WHERE is_valid = TRUE) AS total_deposits,
    
    -- Transaction Metrics
    (SELECT COUNT(*) FROM BANKING_DW.SILVER.TRANSACTION WHERE is_valid = TRUE) AS total_transactions,
    (SELECT SUM(transaction_amount) FROM BANKING_DW.SILVER.TRANSACTION WHERE is_valid = TRUE) AS total_transaction_volume,
    (SELECT AVG(transaction_amount) FROM BANKING_DW.SILVER.TRANSACTION WHERE is_valid = TRUE) AS avg_transaction_amount,
    
    -- Data Quality Metrics
    (SELECT AVG(dq_score) FROM BANKING_DW.SILVER.CUSTOMER) AS avg_customer_dq_score,
    (SELECT AVG(dq_score) FROM BANKING_DW.SILVER.ACCOUNT) AS avg_account_dq_score,
    (SELECT AVG(dq_score) FROM BANKING_DW.SILVER.TRANSACTION) AS avg_transaction_dq_score,
    
    -- Timestamp
    CURRENT_TIMESTAMP() AS report_generated_at;

-- ============================================================================
-- SECTION 6: VERIFICATION QUERIES
-- ============================================================================

-- Verify all Gold views created
SHOW VIEWS IN SCHEMA BANKING_DW.GOLD;

-- Test key views
SELECT * FROM VW_CUSTOMER_360 LIMIT 10;
SELECT * FROM VW_DATA_QUALITY_SCORECARD;
SELECT * FROM VW_EXECUTIVE_DASHBOARD;

-- ============================================================================
-- GOLD LAYER SETUP COMPLETE
-- ============================================================================
-- Next Steps:
-- 1. Execute 05_DQ_Framework_Integration.sql to integrate with DQ framework
-- 2. Set up automated refreshes for materialized views (if needed)
-- 3. Grant appropriate permissions to business users
-- ============================================================================
