USE DATABASE DATA_QUALITY_DB;


CREATE SCHEMA IF NOT EXISTS DB_METRICS
    COMMENT = 'Schema for AI-Driven Observability Metrics and Insights';

--------------------------------------------------------------------------------
-- STEP 2: Create DQ_METRICS Table
-- Purpose: Unified, standardized table of observable facts.
-- This can be a physical table OR a view materializing from existing tables.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DATA_QUALITY_DB.DB_METRICS.DQ_METRICS (
    METRIC_ID VARCHAR(36) DEFAULT UUID_STRING() PRIMARY KEY,
    ASSET_ID VARCHAR(511) NOT NULL COMMENT 'Fully qualified: DATABASE.SCHEMA.TABLE',
    COLUMN_NAME VARCHAR(255) COMMENT 'Optional: Specific column if metric is column-level',
    METRIC_NAME VARCHAR(100) NOT NULL COMMENT 'Standardized metric name: row_count, freshness_hours, null_rate, etc.',
    METRIC_VALUE FLOAT COMMENT 'Numeric value of the metric',
    METRIC_TEXT VARCHAR(1024) COMMENT 'Text value if metric is categorical (e.g., status)',
    METRIC_TIME TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Timestamp of measurement',
    SOURCE_SYSTEM VARCHAR(50) DEFAULT 'PI_QUALYTICS' COMMENT 'Origin of the metric',
    TAGS VARIANT COMMENT 'Optional JSON: Additional context tags'
)
COMMENT = 'Unified fact table for all observability metrics';

-- Create an index-like clustering for faster queries on common filters
-- ALTER TABLE DATA_QUALITY_DB.DB_METRICS.DQ_METRICS CLUSTER BY (ASSET_ID, METRIC_NAME, METRIC_TIME);
-- Note: Clustering costs compute; enable based on data volume and query patterns.

--------------------------------------------------------------------------------
-- STEP 3: Create DQ_AI_INSIGHTS Table
-- Purpose: Store validated, AI-generated insights.
-- Rule: Every insight MUST be traceable to metrics.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DATA_QUALITY_DB.DB_METRICS.DQ_AI_INSIGHTS (
    INSIGHT_ID VARCHAR(36) DEFAULT UUID_STRING() PRIMARY KEY,
    ASSET_ID VARCHAR(511) NOT NULL COMMENT 'Target asset for this insight',
    INSIGHT_TYPE VARCHAR(50) NOT NULL COMMENT 'ANOMALY | TREND | SCHEMA_CHANGE | IMPACT | FRESHNESS | QUALITY',
    SUMMARY VARCHAR(500) NOT NULL COMMENT 'One-line, executive summary',
    DETAILS VARIANT COMMENT 'JSON: { bullets: [...], evidence: {...}, source_metrics: [...] }',
    SEVERITY VARCHAR(20) DEFAULT 'INFO' COMMENT 'INFO | WARNING | CRITICAL',
    IS_ACTIONABLE BOOLEAN DEFAULT FALSE COMMENT 'True if user action is recommended',
    CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'When insight was generated',
    EXPIRES_AT TIMESTAMP_TZ COMMENT 'Optional: Auto-expire old insights'
)
COMMENT = 'Immutable ledger of AI-generated, validated insights';

--------------------------------------------------------------------------------
-- STEP 4: Create Helper View for Schema Introspection
-- Purpose: Antigravity reads this before any SQL generation.
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW DATA_QUALITY_DB.DB_METRICS.V_SCHEMA_REGISTRY AS
SELECT
    TABLE_CATALOG AS DATABASE_NAME,
    TABLE_SCHEMA AS SCHEMA_NAME,
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    ORDINAL_POSITION
FROM DATA_QUALITY_DB.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'PUBLIC') -- Exclude system schemas
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;

COMMENT ON VIEW DATA_QUALITY_DB.DB_METRICS.V_SCHEMA_REGISTRY IS
    'Antigravity introspection view. Read schema BEFORE generating SQL.';

--------------------------------------------------------------------------------
-- STEP 5: Create Stored Procedure for Metric Ingestion (Optional Enhancement)
-- Purpose: Safely insert new metrics with validation.
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE DATA_QUALITY_DB.DB_METRICS.SP_INGEST_METRIC(
    P_ASSET_ID VARCHAR,
    P_COLUMN_NAME VARCHAR,
    P_METRIC_NAME VARCHAR,
    P_METRIC_VALUE FLOAT,
    P_METRIC_TEXT VARCHAR,
    P_TAGS VARIANT
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO DATA_QUALITY_DB.DB_METRICS.DQ_METRICS (
        ASSET_ID,
        COLUMN_NAME,
        METRIC_NAME,
        METRIC_VALUE,
        METRIC_TEXT,
        TAGS
    )
    VALUES (
        UPPER(:P_ASSET_ID),
        :P_COLUMN_NAME,
        UPPER(:P_METRIC_NAME),
        :P_METRIC_VALUE,
        :P_METRIC_TEXT,
        :P_TAGS
    );
    RETURN 'SUCCESS';
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;

--------------------------------------------------------------------------------
-- STEP 6: Seed Initial Metrics from Existing DQ Tables (Backfill)
-- Purpose: Populate DQ_METRICS from existing profile/check data.
-- This is a SAMPLE; adapt to your actual column names.
--------------------------------------------------------------------------------
-- INSERT INTO DATA_QUALITY_DB.DB_METRICS.DQ_METRICS (ASSET_ID, COLUMN_NAME, METRIC_NAME, METRIC_VALUE, METRIC_TIME)
-- SELECT
--     DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS ASSET_ID,
--     COLUMN_NAME,
--     'null_rate' AS METRIC_NAME,
--     (NULL_COUNT * 100.0 / NULLIF(ROW_COUNT, 0)) AS METRIC_VALUE,
--     PROFILE_TIME AS METRIC_TIME
-- FROM DATA_QUALITY_DB.DQ_RESULTS.DQ_COLUMN_PROFILE
-- WHERE PROFILE_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP());

-- Uncomment and adapt the above to seed metrics from your existing data.

--------------------------------------------------------------------------------
-- DONE!
-- After running this script, verify:
-- 1. SHOW SCHEMAS IN DATABASE DATA_QUALITY_DB;
-- 2. SHOW TABLES IN SCHEMA DATA_QUALITY_DB.DB_METRICS;
-- 3. SELECT * FROM DATA_QUALITY_DB.DB_METRICS.V_SCHEMA_REGISTRY LIMIT 10;
--------------------------------------------------------------------------------
