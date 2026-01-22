-- ============================================================================
-- AI-DRIVEN OBSERVABILITY & INSIGHTS - PRODUCTION VERSION
-- Pi-Qualytics Data Quality Platform
-- ============================================================================
-- Purpose: Unified observability layer for AI-driven insights and monitoring
-- Prerequisites: All previous setup scripts executed (01-10)
-- Version: 1.0.0
-- ============================================================================
-- 
-- This file contains:
-- 1. DB_METRICS schema for observability
-- 2. DQ_METRICS - Unified fact table for all observable metrics
-- 3. DQ_AI_INSIGHTS - AI-generated insights with traceability
-- 4. V_SCHEMA_REGISTRY - Schema introspection for AI SQL generation
-- 5. V_UNIFIED_METRICS - Materialized view combining all metrics
-- 6. SP_INGEST_METRIC - Safe metric ingestion procedure
-- 7. SP_BACKFILL_METRICS - Populate from existing DQ data
-- 8. SP_GENERATE_AI_INSIGHTS - Generate insights from metrics
-- 
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE DATA_QUALITY_DB;
USE WAREHOUSE DQ_ANALYTICS_WH;

-- ============================================================================
-- SCHEMA CREATION
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS DB_METRICS
    COMMENT = 'AI-Driven Observability Metrics and Insights for Pi-Qualytics';

USE SCHEMA DB_METRICS;

-- ============================================================================
-- TABLE 1: UNIFIED METRICS FACT TABLE
-- ============================================================================
-- Purpose: Single source of truth for all observable metrics
-- Design: Star schema fact table with dimensional attributes
-- ============================================================================

CREATE TABLE IF NOT EXISTS DQ_METRICS (
    METRIC_ID VARCHAR(36) DEFAULT UUID_STRING() PRIMARY KEY,
    ASSET_ID VARCHAR(511) NOT NULL COMMENT 'Fully qualified: DATABASE.SCHEMA.TABLE',
    COLUMN_NAME VARCHAR(255) COMMENT 'Optional: Column name for column-level metrics',
    METRIC_NAME VARCHAR(100) NOT NULL COMMENT 'Standardized: row_count, null_rate, freshness_hours, dq_score, etc.',
    METRIC_VALUE FLOAT COMMENT 'Numeric value of the metric',
    METRIC_TEXT VARCHAR(1024) COMMENT 'Text value for categorical metrics (status, grade, etc.)',
    METRIC_TIME TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Measurement timestamp',
    SOURCE_SYSTEM VARCHAR(50) DEFAULT 'PI_QUALYTICS' COMMENT 'Origin: PI_QUALYTICS, PROFILING, CUSTOM_CHECK, etc.',
    RUN_ID VARCHAR(100) COMMENT 'Reference to DQ_RUN_CONTROL.RUN_ID for traceability',
    TAGS VARIANT COMMENT 'JSON: Additional context {rule_type, severity, business_domain, etc.}',
    CREATED_TS TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Unified fact table for all observability metrics - enables AI-driven insights';

-- Clustering for performance
ALTER TABLE DQ_METRICS CLUSTER BY (ASSET_ID, METRIC_TIME);

-- ============================================================================
-- TABLE 2: AI INSIGHTS
-- ============================================================================
-- Purpose: Store validated, AI-generated insights with full traceability
-- Design: Immutable ledger with severity levels and actionability flags
-- ============================================================================

CREATE TABLE IF NOT EXISTS DQ_AI_INSIGHTS (
    INSIGHT_ID VARCHAR(36) DEFAULT UUID_STRING() PRIMARY KEY,
    ASSET_ID VARCHAR(511) NOT NULL COMMENT 'Target asset (DATABASE.SCHEMA.TABLE)',
    INSIGHT_TYPE VARCHAR(50) NOT NULL COMMENT 'ANOMALY | TREND | SCHEMA_CHANGE | IMPACT | FRESHNESS | QUALITY | VOLUME',
    SUMMARY VARCHAR(500) NOT NULL COMMENT 'Executive summary - one-line description',
    DETAILS VARIANT COMMENT 'JSON: {bullets: [...], evidence: {...}, source_metrics: [...], recommendations: [...]}',
    SEVERITY VARCHAR(20) DEFAULT 'INFO' COMMENT 'INFO | WARNING | CRITICAL',
    IS_ACTIONABLE BOOLEAN DEFAULT FALSE COMMENT 'True if user action is recommended',
    CONFIDENCE_SCORE FLOAT COMMENT '0-100: AI confidence in this insight',
    SOURCE_METRICS VARIANT COMMENT 'JSON array of METRIC_IDs used to generate this insight',
    CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    EXPIRES_AT TIMESTAMP_TZ COMMENT 'Auto-expire old insights (default: 30 days)',
    ACKNOWLEDGED_BY VARCHAR(100) COMMENT 'User who acknowledged this insight',
    ACKNOWLEDGED_AT TIMESTAMP_TZ COMMENT 'When insight was acknowledged'
)
COMMENT = 'Immutable ledger of AI-generated, validated insights with full traceability';

-- ============================================================================
-- VIEW 1: SCHEMA REGISTRY
-- ============================================================================
-- Purpose: Antigravity AI reads this BEFORE generating SQL
-- Critical: Must include ALL relevant schemas for accurate SQL generation
-- ============================================================================

CREATE OR REPLACE VIEW V_SCHEMA_REGISTRY AS
SELECT
    TABLE_CATALOG AS DATABASE_NAME,
    TABLE_SCHEMA AS SCHEMA_NAME,
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    ORDINAL_POSITION,
    COMMENT AS COLUMN_COMMENT
FROM DATA_QUALITY_DB.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA IN ('DQ_CONFIG', 'DQ_METRICS', 'DQ_ENGINE', 'DB_METRICS', 'BRONZE', 'SILVER', 'GOLD')
  AND TABLE_CATALOG = 'DATA_QUALITY_DB'
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;

COMMENT ON VIEW V_SCHEMA_REGISTRY IS
    'Antigravity AI introspection view - READ THIS BEFORE GENERATING SQL to avoid column name errors';

-- ============================================================================
-- VIEW 2: UNIFIED METRICS (Materialized from existing DQ tables)
-- ============================================================================
-- Purpose: Combine metrics from all sources into DQ_METRICS format
-- Design: UNION ALL from DQ_CHECK_RESULTS, DQ_COLUMN_PROFILE, DQ_DAILY_SUMMARY
-- ============================================================================

CREATE OR REPLACE VIEW V_UNIFIED_METRICS AS
-- Metrics from DQ_CHECK_RESULTS (Quality Checks)
SELECT
    UUID_STRING() AS METRIC_ID,
    DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS ASSET_ID,
    COLUMN_NAME,
    'dq_pass_rate' AS METRIC_NAME,
    PASS_RATE AS METRIC_VALUE,
    CHECK_STATUS AS METRIC_TEXT,
    CHECK_TIMESTAMP AS METRIC_TIME,
    'DQ_CHECK' AS SOURCE_SYSTEM,
    RUN_ID,
    OBJECT_CONSTRUCT(
        'rule_type', RULE_TYPE,
        'rule_name', RULE_NAME,
        'threshold', THRESHOLD,
        'total_records', TOTAL_RECORDS,
        'invalid_records', INVALID_RECORDS
    ) AS TAGS,
    CREATED_TS
FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
WHERE CHECK_TIMESTAMP >= DATEADD(day, -30, CURRENT_TIMESTAMP())

UNION ALL

-- Metrics from DQ_COLUMN_PROFILE (Profiling)
SELECT
    UUID_STRING() AS METRIC_ID,
    DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS ASSET_ID,
    COLUMN_NAME,
    'null_rate' AS METRIC_NAME,
    (NULL_COUNT * 100.0 / NULLIF(TOTAL_RECORDS, 0)) AS METRIC_VALUE,
    NULL AS METRIC_TEXT,
    PROFILE_TS AS METRIC_TIME,
    'PROFILING' AS SOURCE_SYSTEM,
    RUN_ID,
    OBJECT_CONSTRUCT(
        'data_type', DATA_TYPE,
        'distinct_count', DISTINCT_COUNT,
        'total_records', TOTAL_RECORDS
    ) AS TAGS,
    PROFILE_TS AS CREATED_TS
FROM DATA_QUALITY_DB.DQ_METRICS.DQ_COLUMN_PROFILE
WHERE PROFILE_TS >= DATEADD(day, -30, CURRENT_TIMESTAMP())

UNION ALL

-- Metrics from DQ_DAILY_SUMMARY (Aggregated Scores)
SELECT
    UUID_STRING() AS METRIC_ID,
    DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS ASSET_ID,
    NULL AS COLUMN_NAME,
    'dq_score' AS METRIC_NAME,
    DQ_SCORE AS METRIC_VALUE,
    QUALITY_GRADE AS METRIC_TEXT,
    SUMMARY_DATE AS METRIC_TIME,
    'DAILY_SUMMARY' AS SOURCE_SYSTEM,
    LAST_RUN_ID AS RUN_ID,
    OBJECT_CONSTRUCT(
        'business_domain', BUSINESS_DOMAIN,
        'trust_level', TRUST_LEVEL,
        'is_sla_met', IS_SLA_MET,
        'total_checks', TOTAL_CHECKS,
        'failed_checks', FAILED_CHECKS
    ) AS TAGS,
    CREATED_TS
FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
WHERE SUMMARY_DATE >= DATEADD(day, -30, CURRENT_TIMESTAMP());

COMMENT ON VIEW V_UNIFIED_METRICS IS
    'Unified view of all metrics from DQ_CHECK_RESULTS, DQ_COLUMN_PROFILE, and DQ_DAILY_SUMMARY';

-- ============================================================================
-- PROCEDURE 1: METRIC INGESTION
-- ============================================================================
-- Purpose: Safely insert new metrics with validation
-- ============================================================================

CREATE OR REPLACE PROCEDURE SP_INGEST_METRIC(
    P_ASSET_ID VARCHAR,
    P_COLUMN_NAME VARCHAR,
    P_METRIC_NAME VARCHAR,
    P_METRIC_VALUE FLOAT,
    P_METRIC_TEXT VARCHAR,
    P_SOURCE_SYSTEM VARCHAR,
    P_RUN_ID VARCHAR,
    P_TAGS VARIANT
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Validate required fields
    IF (P_ASSET_ID IS NULL OR P_METRIC_NAME IS NULL) THEN
        RETURN 'ERROR: ASSET_ID and METRIC_NAME are required';
    END IF;
    
    INSERT INTO DATA_QUALITY_DB.DB_METRICS.DQ_METRICS (
        ASSET_ID,
        COLUMN_NAME,
        METRIC_NAME,
        METRIC_VALUE,
        METRIC_TEXT,
        SOURCE_SYSTEM,
        RUN_ID,
        TAGS
    )
    VALUES (
        UPPER(:P_ASSET_ID),
        :P_COLUMN_NAME,
        UPPER(:P_METRIC_NAME),
        :P_METRIC_VALUE,
        :P_METRIC_TEXT,
        COALESCE(:P_SOURCE_SYSTEM, 'PI_QUALYTICS'),
        :P_RUN_ID,
        :P_TAGS
    );
    
    RETURN 'SUCCESS: Metric ingested with ID ' || (SELECT MAX(METRIC_ID) FROM DATA_QUALITY_DB.DB_METRICS.DQ_METRICS);
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;

-- ============================================================================
-- PROCEDURE 2: BACKFILL METRICS
-- ============================================================================
-- Purpose: Populate DQ_METRICS from existing DQ tables (one-time or periodic)
-- ============================================================================

CREATE OR REPLACE PROCEDURE SP_BACKFILL_METRICS(
    P_DAYS_BACK INTEGER DEFAULT 30
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    LET rows_inserted INTEGER DEFAULT 0;
    
    -- Clear existing backfilled data to avoid duplicates
    DELETE FROM DATA_QUALITY_DB.DB_METRICS.DQ_METRICS
    WHERE METRIC_TIME >= DATEADD(day, -:P_DAYS_BACK, CURRENT_TIMESTAMP());
    
    -- Insert from V_UNIFIED_METRICS
    INSERT INTO DATA_QUALITY_DB.DB_METRICS.DQ_METRICS (
        ASSET_ID, COLUMN_NAME, METRIC_NAME, METRIC_VALUE, METRIC_TEXT,
        METRIC_TIME, SOURCE_SYSTEM, RUN_ID, TAGS
    )
    SELECT
        ASSET_ID, COLUMN_NAME, METRIC_NAME, METRIC_VALUE, METRIC_TEXT,
        METRIC_TIME, SOURCE_SYSTEM, RUN_ID, TAGS
    FROM DATA_QUALITY_DB.DB_METRICS.V_UNIFIED_METRICS;
    
    rows_inserted := SQLROWCOUNT;
    
    RETURN 'SUCCESS: Backfilled ' || rows_inserted || ' metrics from last ' || :P_DAYS_BACK || ' days';
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;

-- ============================================================================
-- PROCEDURE 3: GENERATE AI INSIGHTS
-- ============================================================================
-- Purpose: Analyze metrics and generate actionable insights
-- Design: Rule-based insights (can be enhanced with ML models)
-- ============================================================================

CREATE OR REPLACE PROCEDURE SP_GENERATE_AI_INSIGHTS(
    P_ASSET_ID VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import snowflake.snowpark as snowpark
from datetime import datetime, timedelta
import json

def main(session, p_asset_id):
    """
    Generate insights from metrics using rule-based analysis
    Future: Can integrate with ML models for advanced anomaly detection
    """
    
    insights_generated = 0
    
    # Build asset filter
    asset_filter = f"WHERE ASSET_ID = '{p_asset_id}'" if p_asset_id else ""
    
    try:
        # 1. ANOMALY DETECTION: Sudden drop in DQ scores
        anomaly_query = f"""
            WITH recent_scores AS (
                SELECT 
                    ASSET_ID,
                    METRIC_VALUE AS DQ_SCORE,
                    METRIC_TIME,
                    LAG(METRIC_VALUE) OVER (PARTITION BY ASSET_ID ORDER BY METRIC_TIME) AS PREV_SCORE
                FROM DATA_QUALITY_DB.DB_METRICS.DQ_METRICS
                WHERE METRIC_NAME = 'DQ_SCORE'
                  AND METRIC_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
                {asset_filter}
            )
            SELECT 
                ASSET_ID,
                DQ_SCORE,
                PREV_SCORE,
                (PREV_SCORE - DQ_SCORE) AS SCORE_DROP,
                METRIC_TIME
            FROM recent_scores
            WHERE PREV_SCORE IS NOT NULL
              AND (PREV_SCORE - DQ_SCORE) > 10
            ORDER BY SCORE_DROP DESC
            LIMIT 5
        """
        
        anomalies = session.sql(anomaly_query).collect()
        
        for row in anomalies:
            asset = row['ASSET_ID']
            drop = round(row['SCORE_DROP'], 2)
            current = round(row['DQ_SCORE'], 2)
            previous = round(row['PREV_SCORE'], 2)
            
            summary = f"DQ Score dropped {drop}% (from {previous}% to {current}%)"
            details = {
                "bullets": [
                    f"Previous score: {previous}%",
                    f"Current score: {current}%",
                    f"Drop: {drop}%",
                    "Investigate recent data changes or rule updates"
                ],
                "evidence": {
                    "metric_name": "dq_score",
                    "previous_value": previous,
                    "current_value": current,
                    "change": -drop
                },
                "recommendations": [
                    "Review failed checks in DQ_CHECK_RESULTS",
                    "Check for schema changes or data pipeline issues",
                    "Verify data source quality"
                ]
            }
            
            session.sql(f"""
                INSERT INTO DATA_QUALITY_DB.DB_METRICS.DQ_AI_INSIGHTS (
                    ASSET_ID, INSIGHT_TYPE, SUMMARY, DETAILS, SEVERITY,
                    IS_ACTIONABLE, CONFIDENCE_SCORE, EXPIRES_AT
                ) VALUES (
                    '{asset}',
                    'ANOMALY',
                    '{summary}',
                    PARSE_JSON('{json.dumps(details)}'),
                    'WARNING',
                    TRUE,
                    85.0,
                    DATEADD(day, 30, CURRENT_TIMESTAMP())
                )
            """).collect()
            
            insights_generated += 1
        
        # 2. TREND DETECTION: Consistent quality improvement/degradation
        trend_query = f"""
            WITH score_trend AS (
                SELECT 
                    ASSET_ID,
                    AVG(CASE WHEN METRIC_TIME >= DATEADD(day, -3, CURRENT_TIMESTAMP()) 
                        THEN METRIC_VALUE END) AS RECENT_AVG,
                    AVG(CASE WHEN METRIC_TIME < DATEADD(day, -3, CURRENT_TIMESTAMP()) 
                        AND METRIC_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
                        THEN METRIC_VALUE END) AS OLDER_AVG
                FROM DATA_QUALITY_DB.DB_METRICS.DQ_METRICS
                WHERE METRIC_NAME = 'DQ_SCORE'
                  AND METRIC_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
                {asset_filter}
                GROUP BY ASSET_ID
            )
            SELECT 
                ASSET_ID,
                RECENT_AVG,
                OLDER_AVG,
                (RECENT_AVG - OLDER_AVG) AS TREND
            FROM score_trend
            WHERE RECENT_AVG IS NOT NULL
              AND OLDER_AVG IS NOT NULL
              AND ABS(RECENT_AVG - OLDER_AVG) > 5
            ORDER BY ABS(TREND) DESC
            LIMIT 5
        """
        
        trends = session.sql(trend_query).collect()
        
        for row in trends:
            asset = row['ASSET_ID']
            trend = round(row['TREND'], 2)
            recent = round(row['RECENT_AVG'], 2)
            older = round(row['OLDER_AVG'], 2)
            
            if trend > 0:
                summary = f"Quality improving: +{trend}% over last week"
                severity = "INFO"
            else:
                summary = f"Quality degrading: {trend}% over last week"
                severity = "WARNING"
            
            details = {
                "bullets": [
                    f"Recent average (3 days): {recent}%",
                    f"Previous average (4-7 days ago): {older}%",
                    f"Trend: {'+' if trend > 0 else ''}{trend}%"
                ],
                "evidence": {
                    "metric_name": "dq_score",
                    "recent_avg": recent,
                    "older_avg": older,
                    "trend": trend
                }
            }
            
            session.sql(f"""
                INSERT INTO DATA_QUALITY_DB.DB_METRICS.DQ_AI_INSIGHTS (
                    ASSET_ID, INSIGHT_TYPE, SUMMARY, DETAILS, SEVERITY,
                    IS_ACTIONABLE, CONFIDENCE_SCORE, EXPIRES_AT
                ) VALUES (
                    '{asset}',
                    'TREND',
                    '{summary}',
                    PARSE_JSON('{json.dumps(details)}'),
                    '{severity}',
                    {str(trend < 0).upper()},
                    75.0,
                    DATEADD(day, 30, CURRENT_TIMESTAMP())
                )
            """).collect()
            
            insights_generated += 1
        
        # 3. FRESHNESS ALERTS: Stale data detection
        # (Add more insight types as needed)
        
        return f"SUCCESS: Generated {insights_generated} insights"
        
    except Exception as e:
        return f"ERROR: {str(e)}"
$$;

-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================

GRANT USAGE ON SCHEMA DB_METRICS TO ROLE ACCOUNTADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA DB_METRICS TO ROLE ACCOUNTADMIN;
GRANT SELECT ON ALL VIEWS IN SCHEMA DB_METRICS TO ROLE ACCOUNTADMIN;
GRANT INSERT, UPDATE, DELETE ON TABLE DQ_METRICS TO ROLE ACCOUNTADMIN;
GRANT INSERT, UPDATE ON TABLE DQ_AI_INSIGHTS TO ROLE ACCOUNTADMIN;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA DB_METRICS TO ROLE ACCOUNTADMIN;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Verify schema and objects
SHOW SCHEMAS IN DATABASE DATA_QUALITY_DB;
SHOW TABLES IN SCHEMA DATA_QUALITY_DB.DB_METRICS;
SHOW VIEWS IN SCHEMA DATA_QUALITY_DB.DB_METRICS;
SHOW PROCEDURES IN SCHEMA DATA_QUALITY_DB.DB_METRICS;

-- Test schema registry
SELECT * FROM DATA_QUALITY_DB.DB_METRICS.V_SCHEMA_REGISTRY LIMIT 10;

-- ============================================================================
-- INITIAL SETUP & TESTING
-- ============================================================================

-- 1. Backfill metrics from existing data (last 30 days)
-- CALL DATA_QUALITY_DB.DB_METRICS.SP_BACKFILL_METRICS(30);

-- 2. Generate initial insights
-- CALL DATA_QUALITY_DB.DB_METRICS.SP_GENERATE_AI_INSIGHTS(NULL);

-- 3. View generated insights
-- SELECT * FROM DATA_QUALITY_DB.DB_METRICS.DQ_AI_INSIGHTS ORDER BY CREATED_AT DESC LIMIT 10;

-- 4. View unified metrics
-- SELECT * FROM DATA_QUALITY_DB.DB_METRICS.V_UNIFIED_METRICS ORDER BY METRIC_TIME DESC LIMIT 20;

-- 5. Test metric ingestion
-- CALL DATA_QUALITY_DB.DB_METRICS.SP_INGEST_METRIC(
--     'BANKING_DW.BRONZE.STG_CUSTOMER',
--     'EMAIL',
--     'VALIDITY_RATE',
--     95.5,
--     'PASSED',
--     'CUSTOM_CHECK',
--     'DQ_RUN_20260122_101500',
--     PARSE_JSON('{"rule_type": "VALIDITY", "threshold": 95}')
-- );

-- ============================================================================
-- OBSERVABILITY SYSTEM SETUP COMPLETE
-- ============================================================================
-- Next Steps:
-- 1. Run backfill: CALL SP_BACKFILL_METRICS(30);
-- 2. Generate insights: CALL SP_GENERATE_AI_INSIGHTS(NULL);
-- 3. Integrate with Antigravity UI for visualization
-- 4. Set up periodic insight generation (daily task)
-- 5. Configure alerts for CRITICAL insights
-- ============================================================================
