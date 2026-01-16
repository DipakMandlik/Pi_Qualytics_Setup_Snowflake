-- =====================================================================================
--                   TEST & VERIFY DATA QUALITY ENGINE
-- =====================================================================================
-- This script verifies configuration and tests the DQ execution engine
-- =====================================================================================

USE DATABASE DATA_QUALITY_DB;
USE WAREHOUSE DQ_ANALYTICS_WH;

-- =====================================================================================
-- SECTION 1: VERIFY CONFIGURATION TABLES ARE POPULATED
-- =====================================================================================

SELECT '========== CONFIGURATION VERIFICATION ==========' AS section;

-- Check 1: Rule Master
SELECT 
    '1. Rule Master' AS check_name,
    COUNT(*) AS total_rules,
    SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) AS active_rules,
    COUNT(DISTINCT rule_type) AS unique_rule_types
FROM DATA_QUALITY_DB.DQ_CONFIG.rule_master;

-- Check 2: SQL Templates
SELECT 
    '2. SQL Templates' AS check_name,
    COUNT(*) AS total_templates,
    SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) AS active_templates,
    MIN(LENGTH(sql_template)) AS min_template_length,
    MAX(LENGTH(sql_template)) AS max_template_length
FROM DATA_QUALITY_DB.DQ_CONFIG.rule_sql_template;

-- Check 3: Datasets
SELECT 
    '3. Dataset Config' AS check_name,
    COUNT(*) AS total_datasets,
    SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) AS active_datasets,
    COUNT(DISTINCT business_domain) AS unique_domains
FROM DATA_QUALITY_DB.DQ_CONFIG.dataset_config;

-- Check 4: Dataset Rules Mapping
SELECT 
    '4. Dataset Rule Config' AS check_name,
    COUNT(*) AS total_mappings,
    SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) AS active_mappings,
    COUNT(DISTINCT dataset_id) AS datasets_with_rules
FROM DATA_QUALITY_DB.DQ_CONFIG.dataset_rule_config;

-- Check 5: Weights
SELECT 
    '5. Weights Mapping' AS check_name,
    COUNT(*) AS total_weights,
    SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) AS active_weights,
    COUNT(DISTINCT business_domain) AS domains_with_weights
FROM DATA_QUALITY_DB.DQ_CONFIG.weights_mapping;

-- =====================================================================================
-- SECTION 2: DETAILED CONFIGURATION REVIEW
-- =====================================================================================

SELECT '========== DETAILED CONFIGURATION ==========' AS section;

-- Show all rules by type
SELECT 
    rule_type,
    COUNT(*) AS rule_count,
    LISTAGG(rule_name, ', ') WITHIN GROUP (ORDER BY rule_name) AS rules
FROM DATA_QUALITY_DB.DQ_CONFIG.rule_master
WHERE is_active = TRUE
GROUP BY rule_type
ORDER BY rule_type;

-- Show dataset configurations
SELECT 
    dataset_id,
    source_database || '.' || source_schema || '.' || source_table AS full_table_name,
    business_domain,
    criticality,
    is_active
FROM DATA_QUALITY_DB.DQ_CONFIG.dataset_config
ORDER BY 
    CASE criticality 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        ELSE 4 
    END;

-- Show rules per dataset
SELECT 
    dc.dataset_id,
    dc.source_table,
    rm.rule_type,
    COUNT(*) AS rule_count,
    AVG(drc.threshold_value) AS avg_threshold
FROM DATA_QUALITY_DB.DQ_CONFIG.dataset_config dc
INNER JOIN DATA_QUALITY_DB.DQ_CONFIG.dataset_rule_config drc 
    ON dc.dataset_id = drc.dataset_id
INNER JOIN DATA_QUALITY_DB.DQ_CONFIG.rule_master rm 
    ON drc.rule_id = rm.rule_id
WHERE dc.is_active = TRUE AND drc.is_active = TRUE
GROUP BY dc.dataset_id, dc.source_table, rm.rule_type
ORDER BY dc.dataset_id, rm.rule_type;

-- =====================================================================================
-- SECTION 3: TEST SQL TEMPLATE GENERATION
-- =====================================================================================

SELECT '========== TEST SQL GENERATION ==========' AS section;

-- Test 1: Show a sample completeness check SQL
SELECT 
    'COMPLETENESS CHECK' AS test_name,
    REPLACE(REPLACE(REPLACE(REPLACE(
        rst.sql_template,
        '{{DATABASE}}', 'BANKING_DW'),
        '{{SCHEMA}}', 'BRONZE'),
        '{{TABLE}}', 'STG_CUSTOMER'),
        '{{COLUMN}}', 'customer_id') AS generated_sql
FROM DATA_QUALITY_DB.DQ_CONFIG.rule_sql_template rst
INNER JOIN DATA_QUALITY_DB.DQ_CONFIG.rule_master rm 
    ON rst.rule_id = rm.rule_id
WHERE rm.rule_name = 'COMPLETENESS_CHECK'
LIMIT 1;

-- Test 2: Show a sample email validation SQL
SELECT 
    'EMAIL VALIDATION' AS test_name,
    REPLACE(REPLACE(REPLACE(REPLACE(
        rst.sql_template,
        '{{DATABASE}}', 'BANKING_DW'),
        '{{SCHEMA}}', 'BRONZE'),
        '{{TABLE}}', 'STG_CUSTOMER'),
        '{{COLUMN}}', 'email') AS generated_sql
FROM DATA_QUALITY_DB.DQ_CONFIG.rule_sql_template rst
INNER JOIN DATA_QUALITY_DB.DQ_CONFIG.rule_master rm 
    ON rst.rule_id = rm.rule_id
WHERE rm.rule_name = 'VALIDITY_EMAIL_FORMAT'
LIMIT 1;

-- =====================================================================================
-- SECTION 4: VERIFY SOURCE DATA EXISTS
-- =====================================================================================

SELECT '========== VERIFY SOURCE DATA ==========' AS section;

-- Check if tables exist and have data
SELECT 'STG_CUSTOMER' AS table_name, COUNT(*) AS row_count 
FROM BANKING_DW.BRONZE.STG_CUSTOMER
UNION ALL
SELECT 'STG_ACCOUNT', COUNT(*) FROM BANKING_DW.BRONZE.STG_ACCOUNT
UNION ALL
SELECT 'STG_TRANSACTION', COUNT(*) FROM BANKING_DW.BRONZE.STG_TRANSACTION
UNION ALL
SELECT 'STG_DAILY_BALANCE', COUNT(*) FROM BANKING_DW.BRONZE.STG_DAILY_BALANCE
UNION ALL
SELECT 'STG_FX_RATE', COUNT(*) FROM BANKING_DW.BRONZE.STG_FX_RATE;

-- =====================================================================================
-- SECTION 5: CLEAR PREVIOUS RUN RESULTS (Optional)
-- =====================================================================================

SELECT '========== CLEAR PREVIOUS RESULTS ==========' AS section;

-- Uncomment to clear previous results
-- TRUNCATE TABLE DATA_QUALITY_DB.DQ_METRICS.dq_run_control;
-- TRUNCATE TABLE DATA_QUALITY_DB.DQ_METRICS.dq_check_results;
-- TRUNCATE TABLE DATA_QUALITY_DB.DQ_METRICS.dq_failed_records;
-- TRUNCATE TABLE DATA_QUALITY_DB.DQ_METRICS.dq_daily_summary;

SELECT 'Previous results cleared (if uncommented)' AS status;

-- =====================================================================================
-- SECTION 6: EXECUTE DQ CHECKS
-- =====================================================================================

SELECT '========== EXECUTING DQ CHECKS ==========' AS section;

-- Test 1: Run checks for a single dataset (CUSTOMER)
CALL DATA_QUALITY_DB.DQ_ENGINE.sp_execute_dq_checks('DS_CUSTOMER', NULL, 'FULL');

-- Test 2: Run all completeness checks across all datasets
-- CALL DATA_QUALITY_DB.DQ_ENGINE.sp_execute_dq_checks(NULL, 'COMPLETENESS', 'FULL');

-- Test 3: Run all checks for all datasets
-- CALL DATA_QUALITY_DB.DQ_ENGINE.sp_execute_dq_checks(NULL, NULL, 'FULL');

-- Test 4: Run only critical datasets
-- CALL DATA_QUALITY_DB.DQ_ENGINE.sp_execute_dq_checks(NULL, NULL, 'CRITICAL_ONLY');

-- =====================================================================================
-- SECTION 7: VIEW RESULTS
-- =====================================================================================

SELECT '========== VIEW EXECUTION RESULTS ==========' AS section;

-- View run summary
SELECT 
    run_id,
    triggered_by,
    start_ts,
    end_ts,
    duration_seconds,
    run_status,
    total_datasets,
    total_checks,
    passed_checks,
    failed_checks,
    warning_checks,
    ROUND((passed_checks::FLOAT / NULLIF(total_checks, 0) * 100), 2) AS pass_rate_pct
FROM DATA_QUALITY_DB.DQ_METRICS.dq_run_control
ORDER BY start_ts DESC
LIMIT 5;

-- View detailed check results
SELECT 
    run_id,
    dataset_id,
    table_name,
    column_name,
    rule_type,
    rule_name,
    total_records,
    valid_records,
    invalid_records,
    pass_rate,
    threshold,
    check_status,
    ROUND(execution_time_ms, 2) AS exec_time_ms
FROM DATA_QUALITY_DB.DQ_METRICS.dq_check_results
WHERE run_id = (SELECT MAX(run_id) FROM DATA_QUALITY_DB.DQ_METRICS.dq_run_control)
ORDER BY 
    CASE check_status 
        WHEN 'FAILED' THEN 1 
        WHEN 'WARNING' THEN 2 
        WHEN 'PASSED' THEN 3 
        ELSE 4 
    END,
    dataset_id,
    column_name;

-- View failed checks only
SELECT 
    dataset_id,
    table_name,
    column_name,
    rule_type,
    rule_name,
    total_records,
    invalid_records,
    pass_rate,
    threshold,
    (threshold - pass_rate) AS shortfall
FROM DATA_QUALITY_DB.DQ_METRICS.dq_check_results
WHERE check_status = 'FAILED'
AND run_id = (SELECT MAX(run_id) FROM DATA_QUALITY_DB.DQ_METRICS.dq_run_control)
ORDER BY shortfall DESC;

-- View summary by rule type
SELECT 
    rule_type,
    COUNT(*) AS total_checks,
    SUM(CASE WHEN check_status = 'PASSED' THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN check_status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN check_status = 'WARNING' THEN 1 ELSE 0 END) AS warnings,
    ROUND(AVG(pass_rate), 2) AS avg_pass_rate
FROM DATA_QUALITY_DB.DQ_METRICS.dq_check_results
WHERE run_id = (SELECT MAX(run_id) FROM DATA_QUALITY_DB.DQ_METRICS.dq_run_control)
GROUP BY rule_type
ORDER BY rule_type;

-- View summary by dataset
SELECT 
    dataset_id,
    table_name,
    COUNT(*) AS total_checks,
    SUM(CASE WHEN check_status = 'PASSED' THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN check_status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN check_status = 'WARNING' THEN 1 ELSE 0 END) AS warnings,
    ROUND(AVG(pass_rate), 2) AS dq_score,
    CASE 
        WHEN AVG(pass_rate) >= 95 THEN '✓ EXCELLENT'
        WHEN AVG(pass_rate) >= 90 THEN '✓ GOOD'
        WHEN AVG(pass_rate) >= 80 THEN '⚠ FAIR'
        ELSE '✗ POOR'
    END AS quality_grade
FROM DATA_QUALITY_DB.DQ_METRICS.dq_check_results
WHERE run_id = (SELECT MAX(run_id) FROM DATA_QUALITY_DB.DQ_METRICS.dq_run_control)
GROUP BY dataset_id, table_name
ORDER BY dq_score DESC;

-- =====================================================================================
-- SECTION 8: EXPORT RESULTS FOR REPORTING
-- =====================================================================================

SELECT '========== EXPORT READY RESULTS ==========' AS section;

-- Complete DQ Report
SELECT 
    rc.run_id,
    rc.triggered_by,
    rc.start_ts,
    rc.duration_seconds,
    rc.run_status,
    cr.dataset_id,
    cr.table_name,
    cr.column_name,
    cr.rule_type,
    cr.rule_name,
    cr.total_records,
    cr.valid_records,
    cr.invalid_records,
    cr.pass_rate,
    cr.threshold,
    cr.check_status,
    CASE 
        WHEN cr.check_status = 'FAILED' THEN 'Action Required'
        WHEN cr.check_status = 'WARNING' THEN 'Monitor'
        ELSE 'OK'
    END AS recommendation
FROM DATA_QUALITY_DB.DQ_METRICS.dq_run_control rc
INNER JOIN DATA_QUALITY_DB.DQ_METRICS.dq_check_results cr 
    ON rc.run_id = cr.run_id
WHERE rc.run_id = (SELECT MAX(run_id) FROM DATA_QUALITY_DB.DQ_METRICS.dq_run_control)
ORDER BY 
    CASE cr.check_status 
        WHEN 'FAILED' THEN 1 
        WHEN 'WARNING' THEN 2 
        ELSE 3 
    END,
    cr.dataset_id;

SELECT '========== TEST COMPLETE ==========' AS final_message;




select * from data_quality_db.dq_metrics.dq_check_results;
select * from data_quality_db.dq_metrics.dq_daily_summary;
select * from data_quality_db.dq_metrics.dq_failed_records;
select * from data_quality_db.dq_metrics.dq_run_control;










select * from banking_dw.bronze.stg_account;
SELECT * FROM banking_dw.bronze.stg_customer;
SELECT * FROM banking_dw.bronze.stg_daily_balance;
SELECT * FROM banking_dw.bronze.stg_fx_rate;
SELECT * FROM banking_dw.bronze.stg_transaction;


select * from dq_config.dataset_config;
select * from dq_config.dataset_rule_config;
select * from dq_config.rule_master;
select * from dq_config.rule_sql_template;
select * from dq_config.weights_mapping;



select * from dq_metrics.dq_daily_summary;




CREATE OR REPLACE NETWORK POLICY ALLOW_ALL_IPS
  ALLOWED_IP_LIST = ('0.0.0.0/0')
  COMMENT = 'Allow all IPs - Development only';

  SHOW NETWORK POLICIES;

DESC NETWORK POLICY ALLOW_ALL_IPS;

ALTER USER ChetanT18
SET NETWORK_POLICY = ALLOW_ALL_IPS;







-----------------Fake Records for huge sumary -----------------------------

USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_METRICS;


-- Clear existing data if needed (optional)
-- DELETE FROM CHECK_RESULTS WHERE CHECK_TIMESTAMP >= '2026-01-01';

CREATE OR REPLACE FILE FORMAT FF_DQ_CSV
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL');



  CREATE OR REPLACE STAGE STG_DQ_SUMMARY
  FILE_FORMAT = FF_DQ_CSV;



  INSERT INTO dq_daily_summary (
    SUMMARY_ID,
    SUMMARY_DATE,
    DATASET_ID,
    DATABASE_NAME,
    SCHEMA_NAME,
    TABLE_NAME,
    BUSINESS_DOMAIN,
    TOTAL_CHECKS,
    PASSED_CHECKS,
    FAILED_CHECKS,
    WARNING_CHECKS,
    SKIPPED_CHECKS,
    DQ_SCORE,
    PREV_DAY_SCORE,
    SCORE_TREND,
    COMPLETENESS_SCORE,
    UNIQUENESS_SCORE,
    VALIDITY_SCORE,
    CONSISTENCY_SCORE,
    FRESHNESS_SCORE,
    VOLUME_SCORE,
    TRUST_LEVEL,
    QUALITY_GRADE,
    IS_SLA_MET,
    TOTAL_RECORDS,
    FAILED_RECORDS_COUNT,
    FAILURE_RATE,
    TOTAL_EXECUTION_TIME_SEC,
    TOTAL_CREDITS_CONSUMED,
    LAST_RUN_ID,
    LAST_RUN_TS,
    LAST_RUN_STATUS,
    CREATED_TS,
    UPDATED_TS
)
SELECT
    TO_NUMBER($1)          AS SUMMARY_ID,
    TO_DATE($2)            AS SUMMARY_DATE,
    $3                     AS DATASET_ID,
    $4                     AS DATABASE_NAME,
    $5                     AS SCHEMA_NAME,
    $6                     AS TABLE_NAME,
    $7                     AS BUSINESS_DOMAIN,
    TO_NUMBER($8)          AS TOTAL_CHECKS,
    TO_NUMBER($9)          AS PASSED_CHECKS,
    TO_NUMBER($10)         AS FAILED_CHECKS,
    TO_NUMBER($11)         AS WARNING_CHECKS,
    TO_NUMBER($12)         AS SKIPPED_CHECKS,
    TO_DOUBLE($13)         AS DQ_SCORE,
    TO_DOUBLE(NULLIF($14,'')) AS PREV_DAY_SCORE,
    TO_DOUBLE(NULLIF($15,'')) AS SCORE_TREND,
    TO_DOUBLE(NULLIF($16,'')) AS COMPLETENESS_SCORE,
    TO_DOUBLE(NULLIF($17,'')) AS UNIQUENESS_SCORE,
    TO_DOUBLE(NULLIF($18,'')) AS VALIDITY_SCORE,
    TO_DOUBLE(NULLIF($19,'')) AS CONSISTENCY_SCORE,
    TO_DOUBLE(NULLIF($20,'')) AS FRESHNESS_SCORE,
    TO_DOUBLE(NULLIF($21,'')) AS VOLUME_SCORE,
    $22                    AS TRUST_LEVEL,
    $23                    AS QUALITY_GRADE,
    IFF(LOWER($24) = 'true', TRUE, FALSE) AS IS_SLA_MET,
    TO_NUMBER($25)         AS TOTAL_RECORDS,
    TO_NUMBER($26)         AS FAILED_RECORDS_COUNT,
    TO_DOUBLE($27)         AS FAILURE_RATE,
    TO_DOUBLE($28)         AS TOTAL_EXECUTION_TIME_SEC,
    TO_DOUBLE($29)         AS TOTAL_CREDITS_CONSUMED,
    $30                    AS LAST_RUN_ID,
    TO_TIMESTAMP_NTZ($31)  AS LAST_RUN_TS,
    $32                    AS LAST_RUN_STATUS,
    TO_TIMESTAMP_NTZ($33)  AS CREATED_TS,
    TO_TIMESTAMP_NTZ(NULLIF($34,'')) AS UPDATED_TS
FROM @STG_DQ_SUMMARY/Testing_2026-01-05-0917_synthetic_next_365_days.csv.gz;



-- drop stage banking_dw.bronze.csv_stage;




SELECT
        SUMMARY_DATE,
        ROUND(AVG(DQ_SCORE), 2) AS overall_dq_score
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      WHERE SUMMARY_DATE = CURRENT_DATE
      GROUP BY SUMMARY_DATE;








-- truncate table banking_dw.bronze.stg_account;
-- truncate table banking_dw.bronze.stg_customer;
-- truncate table banking_dw.bronze.stg_daily_balance;
-- truncate table banking_dw.bronze.stg_fx_rate;
-- truncate table banking_dw.bronze.stg_transaction;


truncate table dq_check_results;
truncate table dq_daily_summary;
truncate table dq_failed_records;
truncate table dq_run_control;

show warehouses;




--------------------------------------------------------------

CREATE TABLE last_30_days AS (
    SELECT
        ds.SUMMARY_DATE,
        ds.DATASET_ID,
        ds.DQ_SCORE,
        ds.PREV_DAY_SCORE,
        ds.TOTAL_CHECKS,
        ds.PASSED_CHECKS,
        ds.FAILED_CHECKS,
        ds.TOTAL_RECORDS,
        ds.FAILED_RECORDS_COUNT,
        ds.IS_SLA_MET,
        ds.QUALITY_GRADE,
        ds.LAST_RUN_ID
    FROM DQ_DAILY_SUMMARY ds
    WHERE ds.SUMMARY_DATE >= DATEADD(DAY, -30, CURRENT_DATE)
);


select * from dq_daily_summary where summary_date= current_date();



select * from dq_check_results where rule_name='COMPLETENESS_CHECK';



SELECT
    COUNT(DISTINCT DATASET_ID)                         AS TOTAL_DATASETS,

    ROUND(AVG(DQ_SCORE), 2)                             AS ENTERPRISE_DATA_TRUST_SCORE,

    ROUND(
        (AVG(DQ_SCORE) - AVG(PREV_DAY_SCORE))
        / NULLIF(AVG(PREV_DAY_SCORE), 0) * 100, 2
    )                                                   AS TRUST_SCORE_CHANGE_PERCENT,

    SUM(TOTAL_CHECKS)                                   AS TOTAL_CHECKS_EXECUTED,

    SUM(PASSED_CHECKS)                                  AS PASSED_CHECKS,

    ROUND(
        SUM(PASSED_CHECKS) / NULLIF(SUM(TOTAL_CHECKS), 0) * 100, 2
    )                                                   AS DATA_QUALITY_PASS_RATE_PERCENT,

    SUM(TOTAL_RECORDS)                                  AS TOTAL_RECORDS_PROCESSED,

    SUM(FAILED_RECORDS_COUNT)                           AS TOTAL_FAILED_RECORDS,

    ROUND(
        SUM(FAILED_RECORDS_COUNT) / NULLIF(SUM(TOTAL_RECORDS), 0) * 100, 2
    )                                                   AS RECORD_FAILURE_RATE_PERCENT,

    COUNT_IF(IS_SLA_MET = FALSE)                         AS SLA_BREACHED_DATASETS

FROM last_30_days;




select * from last_30_days;

SELECT
    DATASET_ID,
    ROUND(AVG(DQ_SCORE), 2)        AS AVG_TRUST_SCORE_30D,
    SUM(FAILED_RECORDS_COUNT)      AS FAILED_RECORDS_30D,
    COUNT_IF(IS_SLA_MET = FALSE)   AS SLA_BREACH_COUNT
FROM last_30_days
GROUP BY DATASET_ID
ORDER BY AVG_TRUST_SCORE_30D DESC;


SELECT 
    summary_date,
    AVG(dq_score) AS avg_dq_score
FROM DATA_QUALITY_DB.DQ_METRICS.dq_daily_summary
WHERE summary_date BETWEEN DATEADD(days, -30, CURRENT_DATE()) AND CURRENT_DATE()
GROUP BY summary_date
ORDER BY summary_date DESC;

select * from banking_dw.bronze.stg_daily_balance;


create database Demo;

drop database demo;





SELECT *
FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
ORDER BY start_ts DESC
LIMIT 10;








SELECT
  run_id,
  triggered_by,
  start_ts,
  end_ts,
  duration_seconds,
  run_status,
  total_datasets,
  total_checks,
  passed_checks,
  failed_checks,
  warning_checks,
  skipped_checks,
  error_message
FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
WHERE run_id = 'DQ_CUSTOM_20260108_031656_89390536';


create database Nivedha;


SELECT
  CURRENT_TIMESTAMP()          AS current_ts,
  CURRENT_TIMESTAMP()::DATE    AS current_date;


DROP TABLE IF EXISTS DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES;


select * from DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES;