-- ============================================================================
-- MASTER SETUP GUIDE - MANUAL EXECUTION INSTRUCTIONS
-- Pi-Qualytics Data Quality Platform - Production Environment
-- ============================================================================
-- Purpose: Guide for executing all setup scripts in the correct order
-- Estimated Time: 5-10 minutes
-- Prerequisites: 
--   1. Snowflake account with ACCOUNTADMIN role
--   2. CSV files uploaded to @CSV_STAGE (see SETUP_GUIDE.md)
-- ============================================================================
-- 
-- IMPORTANT: This is a GUIDE, not an executable script.
-- Execute each script MANUALLY in Snowflake in the order listed below.
-- 
-- ============================================================================

-- ============================================================================
-- EXECUTION ORDER (Execute each file separately in Snowflake)
-- ============================================================================
-- 
-- Step 01: Open and execute 01_Environment_Setup.sql
--          Creates: Warehouses, Databases, Schemas, Stages, Bronze Tables
--          Time: ~1 minute
-- 
-- Step 02: Open and execute 02_Data_Loading.sql
--          Creates: Loads CSV data into Bronze layer
--          Time: ~1-2 minutes (depends on data volume)
--          NOTE: Ensure CSV files are uploaded to @CSV_STAGE first!
-- 
-- Step 03: Open and execute 03_Silver_Layer_Setup.sql
--          Creates: Type-safe Silver tables with DQ scoring
--          Time: ~1 minute
-- 
-- Step 04: Open and execute 04_Gold_Layer_Setup.sql
--          Creates: Analytics views and dashboards
--          Time: ~30 seconds
-- 
-- Step 05: Open and execute 05_Config_Tables.sql
--          Creates: DQ configuration tables
--          Time: ~30 seconds
-- 
-- Step 06: Open and execute 06_Metrics_Tables.sql
--          Creates: Metrics tracking tables
--          Time: ~30 seconds
-- 
-- Step 07: Open and execute 07_Populate_Configuration.sql
--          Creates: Populates 65+ rule mappings
--          Time: ~30 seconds
-- 
-- Step 08: Open and execute 08_Execution_Engine.sql
--          Creates: Main DQ check processor
--          Time: ~30 seconds
-- 
-- Step 09: Open and execute 09_Profiling_Custom_Scanning.sql
--          Creates: Profiling and custom scanning procedures
--          Time: ~30 seconds
-- 
-- Step 10: Open and execute 10_Scheduling_Tasks.sql
--          Creates: Automated scheduling system
--          Time: ~30 seconds
-- 
-- Step 11: Open and execute 11_Observability_AI_Insights.sql
--          Creates: AI-driven observability layer
--          Time: ~30 seconds
-- 
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- FINAL VERIFICATION (Run after all scripts are executed)
-- ============================================================================

USE ROLE ACCOUNTADMIN;

SELECT '>>> Running final verification checks...' AS STATUS;

-- Verify databases
USE DATABASE SNOWFLAKE;
SELECT 'Databases:' AS CHECK_TYPE, COUNT(*) AS COUNT 
FROM INFORMATION_SCHEMA.DATABASES 
WHERE DATABASE_NAME IN ('BANKING_DW', 'DATA_QUALITY_DB');

-- Verify schemas
USE DATABASE DATA_QUALITY_DB;
SELECT 'Schemas:' AS CHECK_TYPE, COUNT(*) AS COUNT 
FROM INFORMATION_SCHEMA.SCHEMATA 
WHERE SCHEMA_NAME IN ('BRONZE', 'SILVER', 'GOLD', 'DQ_CONFIG', 'DQ_METRICS', 'DQ_ENGINE', 'DB_METRICS');

-- Verify Bronze tables
USE DATABASE BANKING_DW;
USE SCHEMA BRONZE;
SELECT 'Bronze Tables:' AS CHECK_TYPE, COUNT(*) AS COUNT 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'BRONZE' AND TABLE_TYPE = 'BASE TABLE';

-- Verify Silver tables
USE SCHEMA SILVER;
SELECT 'Silver Tables:' AS CHECK_TYPE, COUNT(*) AS COUNT 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'SILVER' AND TABLE_TYPE = 'BASE TABLE';

-- Verify Gold views
USE SCHEMA GOLD;
SELECT 'Gold Views:' AS CHECK_TYPE, COUNT(*) AS COUNT 
FROM INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'GOLD';

-- Verify DQ Config
USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_CONFIG;
SELECT 'Config Tables:' AS CHECK_TYPE, COUNT(*) AS COUNT 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'DQ_CONFIG';

SELECT 'Rules Configured:' AS CHECK_TYPE, COUNT(*) AS COUNT 
FROM RULE_MASTER WHERE IS_ACTIVE = TRUE;

SELECT 'Datasets Configured:' AS CHECK_TYPE, COUNT(*) AS COUNT 
FROM DATASET_CONFIG WHERE IS_ACTIVE = TRUE;

SELECT 'Rule Mappings:' AS CHECK_TYPE, COUNT(*) AS COUNT 
FROM DATASET_RULE_CONFIG WHERE IS_ACTIVE = TRUE;

-- Verify DQ Metrics
USE SCHEMA DQ_METRICS;
SELECT 'Metrics Tables:' AS CHECK_TYPE, COUNT(*) AS COUNT 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'DQ_METRICS';

-- Verify Procedures
USE SCHEMA DQ_ENGINE;
SELECT 'DQ Engine Procedures:' AS CHECK_TYPE, COUNT(*) AS COUNT 
FROM INFORMATION_SCHEMA.PROCEDURES 
WHERE PROCEDURE_SCHEMA = 'DQ_ENGINE';

-- Verify Tasks
USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_METRICS;
SHOW TASKS LIKE 'DQ_SCHEDULE_PROCESSOR_TASK';
-- Note: If task exists and is running, you'll see STATE = 'started' in the results

-- ============================================================================
-- EXPECTED VERIFICATION RESULTS
-- ============================================================================
-- Databases: 2 (BANKING_DW, DATA_QUALITY_DB)
-- Schemas: 7 (BRONZE, SILVER, GOLD, DQ_CONFIG, DQ_METRICS, DQ_ENGINE, DB_METRICS)
-- Bronze Tables: 5 (STG_CUSTOMER, STG_ACCOUNT, STG_TRANSACTION, STG_DAILY_BALANCE, STG_FX_RATE)
-- Silver Tables: 5 (CUSTOMER, ACCOUNT, TRANSACTION, DAILY_BALANCE, FX_RATE)
-- Gold Views: 7 (VW_CUSTOMER_360, VW_CUSTOMER_SEGMENTATION, etc.)
-- Config Tables: 6 (RULE_MASTER, RULE_SQL_TEMPLATE, etc.)
-- Rules Configured: 20
-- Datasets Configured: 5
-- Rule Mappings: 65+
-- Metrics Tables: 10+
-- DQ Engine Procedures: 6+
-- Active Tasks: 1 (DQ_SCHEDULE_PROCESSOR_TASK)
-- ============================================================================

-- ============================================================================
-- SETUP COMPLETE!
-- ============================================================================
SELECT '
╔════════════════════════════════════════════════════════════════════════════╗
║                                                                            ║
║              ✓ PI-QUALYTICS PRODUCTION SETUP COMPLETE!                     ║
║                                                                            ║
╚════════════════════════════════════════════════════════════════════════════╝
' AS COMPLETION_MESSAGE;

SELECT CURRENT_TIMESTAMP() AS END_TIME;

-- ============================================================================
-- NEXT STEPS - TESTING YOUR DEPLOYMENT
-- ============================================================================

-- 1. Run your first DQ check
CALL DATA_QUALITY_DB.DQ_ENGINE.SP_EXECUTE_DQ_CHECKS(NULL, NULL, 'FULL');

-- 2. Profile a dataset
CALL DATA_QUALITY_DB.DQ_ENGINE.SP_PROFILE_DATASET('DS_BRONZE_CUSTOMER', NULL, 'FULL');

-- 3. View results
SELECT * FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS 
ORDER BY CHECK_TIMESTAMP DESC LIMIT 20;

-- 4. Check daily summary
SELECT * FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY 
WHERE SUMMARY_DATE = CURRENT_DATE();

-- 5. Generate AI insights
CALL DATA_QUALITY_DB.DB_METRICS.SP_BACKFILL_METRICS(30);
CALL DATA_QUALITY_DB.DB_METRICS.SP_GENERATE_AI_INSIGHTS(NULL);

-- 6. View Gold layer analytics
SELECT * FROM BANKING_DW.GOLD.VW_DATA_QUALITY_SCORECARD;

-- 7. Monitor scheduled tasks
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) 
WHERE NAME = 'DQ_SCHEDULE_PROCESSOR_TASK' 
ORDER BY SCHEDULED_TIME DESC LIMIT 10;

-- ============================================================================
-- END OF MASTER SETUP GUIDE
-- ============================================================================
