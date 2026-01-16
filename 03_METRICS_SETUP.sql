-- =====================================================================================
--                   FIX DATA QUALITY METRICS TABLES
-- =====================================================================================
-- This script fixes column size issues in the metrics tables
-- =====================================================================================

USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_METRICS;
USE WAREHOUSE DQ_ANALYTICS_WH;

-- =====================================================================================
-- Drop and recreate tables with correct column sizes
-- =====================================================================================

-- Drop existing tables (in correct order due to foreign keys)
DROP TABLE IF EXISTS dq_failed_records;
DROP TABLE IF EXISTS dq_check_results;
DROP TABLE IF EXISTS dq_daily_summary;
DROP TABLE IF EXISTS dq_run_control;

-- -----------------------------------------------------------------------------
-- Table 1: dq_run_control (FIXED)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE dq_run_control (
    run_id                      VARCHAR(100) PRIMARY KEY,
    triggered_by                VARCHAR(100),
    start_ts                    TIMESTAMP_NTZ,
    end_ts                      TIMESTAMP_NTZ,
    duration_seconds            NUMBER(10,2),
    run_status                  VARCHAR(50),  -- FIXED: Changed from VARCHAR(20) to VARCHAR(50)
    total_datasets              NUMBER,
    total_checks                NUMBER,
    passed_checks               NUMBER,
    failed_checks               NUMBER,
    warning_checks              NUMBER,
    skipped_checks              NUMBER,
    error_message               VARCHAR(4000),
    created_ts                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Run-level control and orchestration tracking for DQ executions';

ALTER TABLE dq_run_control CLUSTER BY (start_ts);

-- -----------------------------------------------------------------------------
-- Table 2: dq_check_results (FIXED)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE dq_check_results (
    check_id                    NUMBER AUTOINCREMENT PRIMARY KEY,
    run_id                      VARCHAR(100) NOT NULL,
    check_timestamp             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Dataset Information
    dataset_id                  VARCHAR(100),
    database_name               VARCHAR(100),
    schema_name                 VARCHAR(100),
    table_name                  VARCHAR(100),
    column_name                 VARCHAR(100),
    
    -- Rule Information
    rule_id                     NUMBER,
    rule_name                   VARCHAR(100),
    rule_type                   VARCHAR(50),
    rule_level                  VARCHAR(20),
    
    -- Metrics (Atomic Level)
    total_records               NUMBER,
    valid_records               NUMBER,
    invalid_records             NUMBER,
    null_records                NUMBER,
    duplicate_records           NUMBER,
    pass_rate                   NUMBER(10,2),
    threshold                   NUMBER(10,2),
    
    -- Status and Performance
    check_status                VARCHAR(50),  -- FIXED: Changed from VARCHAR(20) to VARCHAR(50)
    execution_time_ms           NUMBER,
    execution_credits           NUMBER(10,6),
    
    -- Failure Details
    failure_reason              VARCHAR(500),
    sample_invalid_values       VARIANT,
    
    -- Metadata
    created_ts                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    CONSTRAINT fk_run_id FOREIGN KEY (run_id) REFERENCES dq_run_control(run_id)
)
COMMENT = 'Atomic-level metrics log for each individual DQ check execution';

ALTER TABLE dq_check_results CLUSTER BY (check_timestamp, table_name);

-- -----------------------------------------------------------------------------
-- Table 3: dq_failed_records (FIXED)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE dq_failed_records (
    failure_id                  NUMBER AUTOINCREMENT PRIMARY KEY,
    check_id                    NUMBER NOT NULL,
    run_id                      VARCHAR(100) NOT NULL,
    
    -- Source Information
    dataset_id                  VARCHAR(100),
    table_name                  VARCHAR(100),
    column_name                 VARCHAR(100),
    
    -- Rule Information
    rule_name                   VARCHAR(100),
    rule_type                   VARCHAR(50),
    failure_type                VARCHAR(100),
    
    -- Failed Record Details (Sample - up to 100 records per check)
    failed_record_pk            VARCHAR(500),
    failed_column_value         VARCHAR(4000),
    expected_pattern            VARCHAR(500),
    actual_value_type           VARCHAR(50),
    
    -- Context (Additional columns for debugging)
    related_columns             VARIANT,
    row_context                 VARIANT,
    
    -- Failure Classification
    failure_category            VARCHAR(50),
    is_critical                 BOOLEAN DEFAULT FALSE,
    can_auto_remediate          BOOLEAN DEFAULT FALSE,
    remediation_suggestion      VARCHAR(1000),
    
    -- SQL for Investigation
    debug_sql                   VARCHAR(4000),
    
    -- Timestamps
    detected_ts                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    created_ts                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    CONSTRAINT fk_check_id FOREIGN KEY (check_id) REFERENCES dq_check_results(check_id)
)
COMMENT = 'Sample failed records with details for debugging and analysis';

ALTER TABLE dq_failed_records CLUSTER BY (detected_ts, table_name);

-- -----------------------------------------------------------------------------
-- Table 4: dq_daily_summary (FIXED)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE dq_daily_summary (
    summary_id                  NUMBER AUTOINCREMENT PRIMARY KEY,
    summary_date                DATE,
    
    -- Dataset Information
    dataset_id                  VARCHAR(100),
    database_name               VARCHAR(100),
    schema_name                 VARCHAR(100),
    table_name                  VARCHAR(100),
    business_domain             VARCHAR(100),
    
    -- Aggregated Metrics
    total_checks                NUMBER,
    passed_checks               NUMBER,
    failed_checks               NUMBER,
    warning_checks              NUMBER,
    skipped_checks              NUMBER,
    
    -- Score Calculation
    dq_score                    NUMBER(10,2),
    prev_day_score              NUMBER(10,2),
    score_trend                 VARCHAR(20),
    
    -- Rule Type Breakdown
    completeness_score          NUMBER(10,2),
    uniqueness_score            NUMBER(10,2),
    validity_score              NUMBER(10,2),
    consistency_score           NUMBER(10,2),
    freshness_score             NUMBER(10,2),
    volume_score                NUMBER(10,2),
    
    -- Status Classification
    trust_level                 VARCHAR(20),
    quality_grade               VARCHAR(10),
    is_sla_met                  BOOLEAN,
    
    -- Volume Metrics
    total_records               NUMBER,
    failed_records_count        NUMBER,
    failure_rate                NUMBER(10,2),
    
    -- Execution Metrics
    total_execution_time_sec    NUMBER(10,2),
    total_credits_consumed      NUMBER(10,6),
    
    -- Last Run Info
    last_run_id                 VARCHAR(100),
    last_run_ts                 TIMESTAMP_NTZ,
    last_run_status             VARCHAR(50),  -- FIXED: Changed from VARCHAR(20) to VARCHAR(50)
    
    -- Metadata
    created_ts                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_ts                  TIMESTAMP_NTZ
)
COMMENT = 'Daily aggregated DQ summary by dataset with trend analysis';

ALTER TABLE dq_daily_summary CLUSTER BY (summary_date, table_name);




CREATE OR REPLACE TABLE dq_weekly_summary (
    week_id                     VARCHAR(10),        -- Format: 2026-W01
    week_start_date             DATE,
    week_end_date               DATE,
    dataset_id                  VARCHAR(100),
    table_name                  VARCHAR(100),
    business_domain             VARCHAR(100),
    
    -- Aggregated Metrics
    total_runs                  NUMBER,
    total_checks                NUMBER,
    passed_checks               NUMBER,
    failed_checks               NUMBER,
    warning_checks              NUMBER,
    
    -- Average Scores
    avg_dq_score                NUMBER(10,2),
    avg_completeness_score      NUMBER(10,2),
    avg_uniqueness_score        NUMBER(10,2),
    avg_validity_score          NUMBER(10,2),
    avg_consistency_score       NUMBER(10,2),
    avg_freshness_score         NUMBER(10,2),
    avg_volume_score            NUMBER(10,2),
    
    -- Trend Indicators
    score_trend                 VARCHAR(20),        -- IMPROVING, STABLE, DEGRADING
    score_change_pct            NUMBER(10,2),
    prev_week_score             NUMBER(10,2),
    
    -- Quality Classification
    quality_grade               VARCHAR(10),
    trust_level                 VARCHAR(20),
    weeks_below_sla             NUMBER,
    
    -- Volume Metrics
    total_records_checked       NUMBER,
    total_invalid_records       NUMBER,
    avg_failure_rate            NUMBER(10,2),
    
    -- Performance Metrics
    total_execution_time_sec    NUMBER(10,2),
    total_credits_consumed      NUMBER(10,6),
    
    created_ts                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_ts                  TIMESTAMP_NTZ
)
COMMENT = 'Weekly aggregated DQ metrics for trend analysis';



-- Monthly Summary Table
CREATE OR REPLACE TABLE dq_monthly_summary (
    month_id                    VARCHAR(7),         -- Format: 2026-01
    month_start_date            DATE,
    month_end_date              DATE,
    dataset_id                  VARCHAR(100),
    table_name                  VARCHAR(100),
    business_domain             VARCHAR(100),
    
    -- Aggregated Metrics
    total_runs                  NUMBER,
    total_checks                NUMBER,
    passed_checks               NUMBER,
    failed_checks               NUMBER,
    warning_checks              NUMBER,
    
    -- Average Scores
    avg_dq_score                NUMBER(10,2),
    avg_completeness_score      NUMBER(10,2),
    avg_uniqueness_score        NUMBER(10,2),
    avg_validity_score          NUMBER(10,2),
    avg_consistency_score       NUMBER(10,2),
    avg_freshness_score         NUMBER(10,2),
    avg_volume_score            NUMBER(10,2),
    
    -- Min/Max Scores (for volatility)
    min_dq_score                NUMBER(10,2),
    max_dq_score                NUMBER(10,2),
    score_volatility            NUMBER(10,2),       -- Std deviation
    
    -- Trend Indicators
    score_trend                 VARCHAR(20),
    score_change_pct            NUMBER(10,2),
    prev_month_score            NUMBER(10,2),
    
    -- Quality Classification
    quality_grade               VARCHAR(10),
    trust_level                 VARCHAR(20),
    sla_compliance_pct          NUMBER(10,2),       -- % of days meeting SLA
    
    -- Issue Tracking
    total_critical_issues       NUMBER,
    top_failure_types           VARCHAR(500),
    
    -- Volume Metrics
    total_records_checked       NUMBER,
    total_invalid_records       NUMBER,
    avg_failure_rate            NUMBER(10,2),
    
    -- Performance Metrics
    total_execution_time_sec    NUMBER(10,2),
    total_credits_consumed      NUMBER(10,6),
    
    created_ts                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_ts                  TIMESTAMP_NTZ
)
COMMENT = 'Monthly aggregated DQ metrics for long-term trend analysis';



-- Quarterly Summary Table
CREATE OR REPLACE TABLE dq_quarterly_summary (
    quarter_id                  VARCHAR(7),         -- Format: 2026-Q1
    quarter_start_date          DATE,
    quarter_end_date            DATE,
    dataset_id                  VARCHAR(100),
    table_name                  VARCHAR(100),
    business_domain             VARCHAR(100),
    
    -- Aggregated Metrics
    total_runs                  NUMBER,
    total_checks                NUMBER,
    passed_checks               NUMBER,
    failed_checks               NUMBER,
    
    -- Average Scores
    avg_dq_score                NUMBER(10,2),
    avg_completeness_score      NUMBER(10,2),
    avg_uniqueness_score        NUMBER(10,2),
    avg_validity_score          NUMBER(10,2),
    avg_consistency_score       NUMBER(10,2),
    
    -- Trend Analysis
    score_trend                 VARCHAR(20),
    score_improvement_pct       NUMBER(10,2),
    best_month                  VARCHAR(7),
    worst_month                 VARCHAR(7),
    
    -- Quality Metrics
    quality_grade               VARCHAR(10),
    sla_compliance_pct          NUMBER(10,2),
    
    created_ts                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Quarterly aggregated DQ metrics for executive reporting';




CREATE OR REPLACE TABLE DATA_QUALITY_DB.DQ_CONFIG.ALLOWED_VALUES_CONFIG (
    dataset_id      VARCHAR(100),
    column_name     VARCHAR(100),
    allowed_values  VARCHAR(2000),   -- "'A','B','C'"
    is_active       BOOLEAN,
    created_ts      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================================================
-- VERIFICATION
-- =====================================================================================

-- Verify tables exist
SELECT 
    table_name,
    row_count,
    bytes
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'DQ_METRICS'
AND table_name IN ('DQ_RUN_CONTROL', 'DQ_CHECK_RESULTS', 'DQ_FAILED_RECORDS', 'DQ_DAILY_SUMMARY')
ORDER BY table_name;

-- Show column details for run_control
SELECT 
    column_name,
    data_type,
    character_maximum_length,
    is_nullable
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'DQ_METRICS'
AND table_name = 'DQ_RUN_CONTROL'
ORDER BY ordinal_position;

SELECT '✓ All DQ Metrics tables recreated with correct column sizes' AS status;





CREATE OR REPLACE TABLE DATA_QUALITY_DB.DQ_METRICS.DQ_COLUMN_PROFILE (
    PROFILE_ID NUMBER AUTOINCREMENT,
    RUN_ID VARCHAR(100),
    DATASET_ID VARCHAR(100),
    DATABASE_NAME VARCHAR(100),
    SCHEMA_NAME VARCHAR(100),
    TABLE_NAME VARCHAR(100),
    COLUMN_NAME VARCHAR(100),
    DATA_TYPE VARCHAR(50),

    TOTAL_RECORDS NUMBER,
    NULL_COUNT NUMBER,
    DISTINCT_COUNT NUMBER,

    MIN_VALUE VARCHAR,
    MAX_VALUE VARCHAR,
    AVG_VALUE NUMBER,
    STDDEV_VALUE NUMBER,

    MIN_LENGTH NUMBER,
    MAX_LENGTH NUMBER,
    AVG_LENGTH NUMBER,

    FUTURE_DATE_COUNT NUMBER,

    PROFILE_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (PROFILE_ID)
);
