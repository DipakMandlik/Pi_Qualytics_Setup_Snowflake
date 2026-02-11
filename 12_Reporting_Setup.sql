-- =====================================================
-- Pi-Qualytics Reporting Tables
-- =====================================================
-- Purpose: Store report metadata and scheduled report configurations
-- Created: 2026-01-27
-- =====================================================

USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DB_METRICS;

-- =====================================================
-- 1. Reports Table
-- =====================================================
-- Stores metadata for all generated reports
CREATE TABLE IF NOT EXISTS DQ_REPORTS (
    REPORT_ID VARCHAR(36) PRIMARY KEY,
    REPORT_TYPE VARCHAR(50) NOT NULL,  -- 'PLATFORM' | 'DATASET' | 'INCIDENT'
    SCOPE VARCHAR(100),                 -- 'PLATFORM' or dataset identifier (DB.SCHEMA.TABLE)
    REPORT_DATE DATE NOT NULL,
    GENERATED_BY VARCHAR(100),
    GENERATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FORMAT VARCHAR(10),                 -- 'PDF' | 'CSV' | 'JSON'
    FILE_PATH VARCHAR(500),             -- Path to stored file
    FILE_SIZE_BYTES NUMBER,
    DOWNLOAD_COUNT NUMBER DEFAULT 0,
    SHARE_TOKEN VARCHAR(100),           -- For shareable links
    SHARE_EXPIRES_AT TIMESTAMP_NTZ,     -- Link expiry
    METADATA VARIANT,                   -- JSON metadata (filters, parameters, etc.)
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Add comments for documentation
COMMENT ON TABLE DQ_REPORTS IS 'Stores metadata for all generated data quality reports';
COMMENT ON COLUMN DQ_REPORTS.REPORT_ID IS 'Unique identifier for the report (UUID)';
COMMENT ON COLUMN DQ_REPORTS.REPORT_TYPE IS 'Type of report: PLATFORM, DATASET, or INCIDENT';
COMMENT ON COLUMN DQ_REPORTS.SCOPE IS 'Scope of report - PLATFORM or specific dataset identifier';
COMMENT ON COLUMN DQ_REPORTS.REPORT_DATE IS 'Date for which the report was generated';
COMMENT ON COLUMN DQ_REPORTS.GENERATED_BY IS 'User who generated the report';
COMMENT ON COLUMN DQ_REPORTS.FORMAT IS 'Export format: PDF, CSV, or JSON';
COMMENT ON COLUMN DQ_REPORTS.FILE_PATH IS 'Storage path for the generated report file';
COMMENT ON COLUMN DQ_REPORTS.DOWNLOAD_COUNT IS 'Number of times report has been downloaded';
COMMENT ON COLUMN DQ_REPORTS.SHARE_TOKEN IS 'Secure token for shareable links';
COMMENT ON COLUMN DQ_REPORTS.SHARE_EXPIRES_AT IS 'Expiry timestamp for shareable links';
COMMENT ON COLUMN DQ_REPORTS.METADATA IS 'Additional metadata in JSON format';

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS IDX_REPORTS_TYPE_DATE ON DQ_REPORTS(REPORT_TYPE, REPORT_DATE DESC);
CREATE INDEX IF NOT EXISTS IDX_REPORTS_SCOPE ON DQ_REPORTS(SCOPE);
CREATE INDEX IF NOT EXISTS IDX_REPORTS_GENERATED_AT ON DQ_REPORTS(GENERATED_AT DESC);
CREATE INDEX IF NOT EXISTS IDX_REPORTS_SHARE_TOKEN ON DQ_REPORTS(SHARE_TOKEN);

-- =====================================================
-- 2. Scheduled Reports Table
-- =====================================================
-- Stores configurations for scheduled report delivery
CREATE TABLE IF NOT EXISTS DQ_SCHEDULED_REPORTS (
    SCHEDULE_ID VARCHAR(36) PRIMARY KEY,
    REPORT_TYPE VARCHAR(50) NOT NULL,   -- 'PLATFORM' | 'DATASET'
    SCOPE VARCHAR(100),                  -- 'PLATFORM' or dataset identifier
    FREQUENCY VARCHAR(20) NOT NULL,      -- 'DAILY' | 'WEEKLY' | 'MONTHLY'
    SCHEDULE_TIME TIME,                  -- Time of day to run (e.g., '09:00:00')
    DAY_OF_WEEK NUMBER,                  -- For weekly: 0=Sunday, 6=Saturday
    DAY_OF_MONTH NUMBER,                 -- For monthly: 1-31
    RECIPIENTS VARIANT NOT NULL,         -- JSON array of email addresses
    FORMAT VARCHAR(10) DEFAULT 'PDF',    -- 'PDF' | 'CSV' | 'JSON'
    ENABLED BOOLEAN DEFAULT TRUE,
    LAST_RUN_AT TIMESTAMP_NTZ,
    LAST_RUN_STATUS VARCHAR(20),         -- 'SUCCESS' | 'FAILED'
    LAST_RUN_ERROR VARCHAR(1000),
    NEXT_RUN_AT TIMESTAMP_NTZ,
    RUN_COUNT NUMBER DEFAULT 0,
    CREATED_BY VARCHAR(100),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Add comments
COMMENT ON TABLE DQ_SCHEDULED_REPORTS IS 'Configurations for scheduled report delivery';
COMMENT ON COLUMN DQ_SCHEDULED_REPORTS.SCHEDULE_ID IS 'Unique identifier for the schedule (UUID)';
COMMENT ON COLUMN DQ_SCHEDULED_REPORTS.REPORT_TYPE IS 'Type of report to generate';
COMMENT ON COLUMN DQ_SCHEDULED_REPORTS.FREQUENCY IS 'How often to run: DAILY, WEEKLY, or MONTHLY';
COMMENT ON COLUMN DQ_SCHEDULED_REPORTS.SCHEDULE_TIME IS 'Time of day to generate report';
COMMENT ON COLUMN DQ_SCHEDULED_REPORTS.RECIPIENTS IS 'JSON array of recipient email addresses';
COMMENT ON COLUMN DQ_SCHEDULED_REPORTS.ENABLED IS 'Whether the schedule is active';
COMMENT ON COLUMN DQ_SCHEDULED_REPORTS.LAST_RUN_AT IS 'Timestamp of last execution';
COMMENT ON COLUMN DQ_SCHEDULED_REPORTS.NEXT_RUN_AT IS 'Timestamp of next scheduled execution';

-- Create indexes
CREATE INDEX IF NOT EXISTS IDX_SCHEDULED_ENABLED_NEXT ON DQ_SCHEDULED_REPORTS(ENABLED, NEXT_RUN_AT);
CREATE INDEX IF NOT EXISTS IDX_SCHEDULED_TYPE ON DQ_SCHEDULED_REPORTS(REPORT_TYPE);

-- =====================================================
-- 3. Report Delivery History Table
-- =====================================================
-- Tracks each delivery attempt for scheduled reports
CREATE TABLE IF NOT EXISTS DQ_REPORT_DELIVERIES (
    DELIVERY_ID VARCHAR(36) PRIMARY KEY,
    SCHEDULE_ID VARCHAR(36) NOT NULL,
    REPORT_ID VARCHAR(36),               -- Link to generated report
    DELIVERY_METHOD VARCHAR(20),         -- 'EMAIL' | 'LINK'
    RECIPIENTS VARIANT,                  -- JSON array of recipients
    STATUS VARCHAR(20),                  -- 'SUCCESS' | 'FAILED' | 'PENDING'
    ERROR_MESSAGE VARCHAR(1000),
    DELIVERED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE DQ_REPORT_DELIVERIES IS 'Tracks delivery history for scheduled reports';
COMMENT ON COLUMN DQ_REPORT_DELIVERIES.SCHEDULE_ID IS 'Reference to scheduled report configuration';
COMMENT ON COLUMN DQ_REPORT_DELIVERIES.REPORT_ID IS 'Reference to generated report';
COMMENT ON COLUMN DQ_REPORT_DELIVERIES.STATUS IS 'Delivery status: SUCCESS, FAILED, or PENDING';

-- Create indexes
CREATE INDEX IF NOT EXISTS IDX_DELIVERIES_SCHEDULE ON DQ_REPORT_DELIVERIES(SCHEDULE_ID, DELIVERED_AT DESC);
CREATE INDEX IF NOT EXISTS IDX_DELIVERIES_REPORT ON DQ_REPORT_DELIVERIES(REPORT_ID);

-- =====================================================
-- 4. Sample Data (Optional - for testing)
-- =====================================================
-- Uncomment to insert sample scheduled report
/*
INSERT INTO DQ_SCHEDULED_REPORTS (
    SCHEDULE_ID,
    REPORT_TYPE,
    SCOPE,
    FREQUENCY,
    SCHEDULE_TIME,
    RECIPIENTS,
    FORMAT,
    ENABLED,
    NEXT_RUN_AT,
    CREATED_BY
) VALUES (
    'sample-schedule-001',
    'PLATFORM',
    'PLATFORM',
    'DAILY',
    '09:00:00',
    PARSE_JSON('["admin@company.com", "dq-team@company.com"]'),
    'PDF',
    TRUE,
    DATEADD(day, 1, CURRENT_TIMESTAMP()),
    'system'
);
*/

-- =====================================================
-- 5. Verification Queries
-- =====================================================
-- Verify tables were created
SELECT 'DQ_REPORTS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM DQ_REPORTS
UNION ALL
SELECT 'DQ_SCHEDULED_REPORTS', COUNT(*) FROM DQ_SCHEDULED_REPORTS
UNION ALL
SELECT 'DQ_REPORT_DELIVERIES', COUNT(*) FROM DQ_REPORT_DELIVERIES;

-- Show table structures
DESCRIBE TABLE DQ_REPORTS;
DESCRIBE TABLE DQ_SCHEDULED_REPORTS;
DESCRIBE TABLE DQ_REPORT_DELIVERIES;
