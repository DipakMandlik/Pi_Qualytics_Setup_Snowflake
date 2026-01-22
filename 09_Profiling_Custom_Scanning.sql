-- ============================================================================
-- PROFILING & CUSTOM SCANNING PROCEDURES - PRODUCTION VERSION
-- Pi-Qualytics Data Quality Platform
-- ============================================================================
-- Purpose: Dataset profiling and ad-hoc custom rule execution
-- Prerequisites: All previous setup scripts executed (01-08)
-- Version: 1.0.0
-- ============================================================================
-- 
-- This file contains TWO procedures:
-- 1. SP_PROFILE_DATASET - Profile a single dataset with all configured rules
-- 2. SP_RUN_CUSTOM_RULE - Run a single rule ad-hoc (with optional threshold override)
-- 
-- Both procedures reuse the same metrics tables and patterns as the main
-- execution engine (08_Execution_Engine.sql) for consistency.
-- 
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_ENGINE;
USE WAREHOUSE DQ_ANALYTICS_WH;

-- ============================================================================
-- PROCEDURE 1: DATASET PROFILING
-- ============================================================================
-- Purpose: Profile a single dataset with all configured rules
-- Use Case: Deep analysis of one dataset, scheduled profiling jobs
-- ============================================================================

CREATE OR REPLACE PROCEDURE SP_PROFILE_DATASET(
    P_DATASET_ID VARCHAR,
    P_RULE_TYPE  VARCHAR DEFAULT NULL,
    P_RUN_MODE   VARCHAR DEFAULT 'FULL'
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
from snowflake.snowpark import Session
import json
import uuid
from datetime import datetime

def main(session: snowpark.Session, p_dataset_id: str, p_rule_type: str, p_run_mode: str) -> str:
    """
    Profile a single dataset with all configured rules
    
    Parameters:
        p_dataset_id: Required - dataset to profile (e.g., 'DS_BRONZE_CUSTOMER')
        p_rule_type: Optional - filter by rule type (e.g., 'COMPLETENESS')
        p_run_mode: Optional - 'FULL' or 'CRITICAL_ONLY'
    
    Returns:
        JSON string with profiling summary
    """
    
    if not p_dataset_id:
        raise Exception("p_dataset_id is required for profiling")

    try:
        # Initialize run
        run_id = f"DQ_PROFILE_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
        start_time = datetime.now()
        triggered_by = session.sql("SELECT CURRENT_USER()").collect()[0][0]

        print("\n" + "="*80)
        print("DATA QUALITY PROFILING EXECUTION")
        print("="*80)
        print(f"Run ID       : {run_id}")
        print(f"Dataset      : {p_dataset_id}")
        print(f"Rule Type    : {p_rule_type or 'ALL'}")
        print(f"Run Mode     : {p_run_mode}")
        print(f"Triggered By : {triggered_by}")
        print("="*80 + "\n")

        # Insert run control record
        session.sql(f"""
            INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL (
                RUN_ID, 
                TRIGGERED_BY, 
                START_TS, 
                RUN_STATUS,
                TOTAL_DATASETS,
                TOTAL_CHECKS,
                PASSED_CHECKS,
                FAILED_CHECKS,
                WARNING_CHECKS,
                SKIPPED_CHECKS,
                CREATED_TS
            ) VALUES (
                '{run_id}', 
                '{triggered_by}',
                CURRENT_TIMESTAMP(),
                'RUNNING',
                0, 0, 0, 0, 0, 0,
                CURRENT_TIMESTAMP()
            )
        """).collect()

        # Fetch dataset
        datasets = fetch_datasets_for_profile(session, p_dataset_id, p_run_mode)

        if not datasets:
            raise Exception(f"No active dataset found for dataset_id = {p_dataset_id}")

        if len(datasets) > 1:
            print(f"⚠ Warning: multiple config rows found for {p_dataset_id}, using all.")

        print(f"📊 Profiling {len(datasets)} config row(s) for dataset {p_dataset_id}\n")

        # Initialize stats
        stats = {
            'total_checks': 0,
            'passed_checks': 0,
            'failed_checks': 0,
            'warning_checks': 0,
            'skipped_checks': 0,
            'total_records_processed': 0,
            'total_invalid_records': 0
        }

        # Process dataset
        for dataset in datasets:
            process_dataset_profile(session, run_id, dataset, p_rule_type, stats)

        # Finalize run
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        run_status = 'COMPLETED' if stats['failed_checks'] == 0 else 'COMPLETED_WITH_FAILURES'

        session.sql(f"""
            UPDATE DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
            SET 
                END_TS = CURRENT_TIMESTAMP(),
                DURATION_SECONDS = {duration},
                RUN_STATUS = '{run_status}',
                TOTAL_DATASETS = {len(datasets)},
                TOTAL_CHECKS = {stats['total_checks']},
                PASSED_CHECKS = {stats['passed_checks']},
                FAILED_CHECKS = {stats['failed_checks']},
                WARNING_CHECKS = {stats['warning_checks']},
                SKIPPED_CHECKS = {stats['skipped_checks']},
                ERROR_MESSAGE = NULL
            WHERE RUN_ID = '{run_id}'
        """).collect()

        # Print summary
        print("\n" + "="*80)
        print("PROFILING SUMMARY")
        print("="*80)
        print(f"Status            : {run_status}")
        print(f"Duration          : {duration:.2f} seconds")
        print(f"Total Checks      : {stats['total_checks']}")
        print(f"✓ Passed          : {stats['passed_checks']}")
        print(f"✗ Failed          : {stats['failed_checks']}")
        print(f"⚠ Warnings        : {stats['warning_checks']}")
        print(f"⊘ Skipped         : {stats['skipped_checks']}")
        print(f"Records Processed : {stats['total_records_processed']:,}")
        print(f"Invalid Records   : {stats['total_invalid_records']:,}")
        if stats['total_checks'] > 0:
            pass_rate = round((stats['passed_checks'] / stats['total_checks'] * 100), 2)
            print(f"Overall Pass Rate : {pass_rate}%")
        print("="*80 + "\n")

        # Generate daily summary
        try:
            generate_daily_summary_for_run(session, run_id)
            print("✓ Daily summary updated for profiling run\n")
        except Exception as e:
            print(f"⚠ Warning: Daily summary generation failed: {str(e)}\n")

        # Return JSON
        result = {
            'run_id': run_id,
            'status': run_status,
            'duration_seconds': duration,
            'dataset_id': p_dataset_id,
            'total_checks': stats['total_checks'],
            'passed': stats['passed_checks'],
            'failed': stats['failed_checks'],
            'warnings': stats['warning_checks'],
            'skipped': stats['skipped_checks'],
            'records_processed': stats['total_records_processed'],
            'invalid_records': stats['total_invalid_records'],
            'pass_rate': round((stats['passed_checks'] / stats['total_checks'] * 100), 2) if stats['total_checks'] > 0 else 0
        }
        return json.dumps(result, indent=2)

    except Exception as e:
        error_msg = str(e)
        print(f"\n❌ PROFILING ERROR: {error_msg}\n")

        try:
            session.sql(f"""
                UPDATE DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
                SET 
                    END_TS = CURRENT_TIMESTAMP(),
                    RUN_STATUS = 'FAILED',
                    ERROR_MESSAGE = '{error_msg.replace("'", "''")[:4000]}'
                WHERE RUN_ID = '{run_id}'
            """).collect()
        except:
            pass

        return json.dumps({'run_id': run_id, 'status': 'FAILED', 'error': error_msg})


def fetch_datasets_for_profile(session, p_dataset_id, p_run_mode):
    """Fetch dataset rows for profiling"""
    criticality_filter = "AND dc.CRITICALITY = 'CRITICAL'" if p_run_mode == 'CRITICAL_ONLY' else ""

    query = f"""
        SELECT 
            dc.DATASET_ID,
            dc.SOURCE_DATABASE,
            dc.SOURCE_SCHEMA,
            dc.SOURCE_TABLE,
            dc.BUSINESS_DOMAIN,
            dc.CRITICALITY
        FROM DATA_QUALITY_DB.DQ_CONFIG.DATASET_CONFIG dc
        WHERE dc.IS_ACTIVE = TRUE
          AND dc.DATASET_ID = '{p_dataset_id}'
        {criticality_filter}
        ORDER BY 
            CASE dc.CRITICALITY 
                WHEN 'CRITICAL' THEN 1 
                WHEN 'HIGH' THEN 2 
                WHEN 'MEDIUM' THEN 3 
                ELSE 4 
            END
    """
    return session.sql(query).collect()


def process_dataset_profile(session, run_id, dataset, p_rule_type, stats):
    """Process all rules for one dataset (profiling)"""

    dataset_id      = dataset['DATASET_ID']
    source_db       = dataset['SOURCE_DATABASE']
    source_schema   = dataset['SOURCE_SCHEMA']
    source_table    = dataset['SOURCE_TABLE']
    business_domain = dataset['BUSINESS_DOMAIN']

    print(f"📁 Profiling Dataset: {dataset_id}")
    print(f"   Table : {source_db}.{source_schema}.{source_table}")
    print(f"   Domain: {business_domain}")

    rules = fetch_rules_for_profile(session, dataset_id, business_domain, p_rule_type)

    if not rules:
        print(f"   ⚠ No active rules configured for {dataset_id}\n")
        return

    print(f"   Rules: {len(rules)}\n")

    for rule in rules:
        stats['total_checks'] += 1

        try:
            result = execute_check_profile(
                session, run_id, dataset_id, source_db,
                source_schema, source_table, rule, business_domain
            )

            stats['total_records_processed'] += result['total_records']
            stats['total_invalid_records']   += result['failed_records']

            if result['status'] == 'PASSED':
                stats['passed_checks'] += 1
                icon = '✓'
            elif result['status'] == 'FAILED':
                stats['failed_checks'] += 1
                icon = '✗'
            elif result['status'] == 'WARNING':
                stats['warning_checks'] += 1
                icon = '⚠'
            else:
                stats['skipped_checks'] += 1
                icon = '⊘'

            col_display = f"[{rule['COLUMN_NAME']}]" if rule['COLUMN_NAME'] else "[TABLE]"
            print(f"   {icon} {rule['RULE_TYPE']:15s} {col_display:25s} "
                  f"{result['status']:8s} ({result['pass_rate']:6.2f}% | "
                  f"{result['valid_records']:,}/{result['total_records']:,})")

        except Exception as e:
            stats['skipped_checks'] += 1
            print(f"   ⊘ ERROR: {rule['RULE_NAME']} - {str(e)}")
            log_error_check_profile(session, run_id, dataset_id, source_table, rule, str(e))

    print()


def fetch_rules_for_profile(session, dataset_id, business_domain, p_rule_type):
    """Fetch active rules for a dataset"""

    rule_type_filter = f"AND rm.RULE_TYPE = '{p_rule_type}'" if p_rule_type else ""

    query = f"""
        SELECT 
            drc.CONFIG_ID,
            drc.DATASET_ID,
            drc.COLUMN_NAME,
            drc.THRESHOLD_VALUE,
            rm.RULE_ID,
            rm.RULE_NAME,
            rm.RULE_TYPE,
            rm.RULE_LEVEL,
            rm.DESCRIPTION,
            rst.SQL_TEMPLATE,
            COALESCE(wm.WEIGHT, 1.0) as WEIGHT,
            COALESCE(wm.PRIORITY, 3) as PRIORITY
        FROM DATA_QUALITY_DB.DQ_CONFIG.DATASET_RULE_CONFIG drc
        INNER JOIN DATA_QUALITY_DB.DQ_CONFIG.RULE_MASTER rm
            ON drc.RULE_ID = rm.RULE_ID
        INNER JOIN DATA_QUALITY_DB.DQ_CONFIG.RULE_SQL_TEMPLATE rst
            ON rm.RULE_ID = rst.RULE_ID
            AND rst.IS_ACTIVE = TRUE
        LEFT JOIN DATA_QUALITY_DB.DQ_CONFIG.WEIGHTS_MAPPING wm
            ON rm.RULE_TYPE = wm.RULE_TYPE
            AND wm.BUSINESS_DOMAIN = '{business_domain}'
            AND wm.IS_ACTIVE = TRUE
            AND CURRENT_DATE() BETWEEN wm.EFFECTIVE_DATE 
                AND COALESCE(wm.EXPIRY_DATE, '9999-12-31')
        WHERE drc.DATASET_ID = '{dataset_id}'
          AND drc.IS_ACTIVE = TRUE
          AND rm.IS_ACTIVE = TRUE
        {rule_type_filter}
        ORDER BY 
            COALESCE(wm.PRIORITY, 3),
            CASE rm.RULE_TYPE
                WHEN 'COMPLETENESS' THEN 1
                WHEN 'UNIQUENESS'  THEN 2
                WHEN 'VALIDITY'    THEN 3
                WHEN 'CONSISTENCY' THEN 4
                WHEN 'FRESHNESS'   THEN 5
                WHEN 'VOLUME'      THEN 6
            END,
            drc.COLUMN_NAME
    """
    return session.sql(query).collect()


def execute_check_profile(session, run_id, dataset_id, source_db, source_schema, source_table, rule, business_domain):
    """Execute a single DQ check (profiling)"""

    check_start = datetime.now()

    sql = build_check_sql_profile(rule, source_db, source_schema, source_table)
    result = session.sql(sql).collect()[0]

    total_count = int(result['TOTAL_COUNT']) if result['TOTAL_COUNT'] is not None else 0
    error_count = int(result['ERROR_COUNT']) if result['ERROR_COUNT'] is not None else 0
    valid_count = total_count - error_count

    null_count = 0
    if rule['RULE_TYPE'] == 'COMPLETENESS' and rule['COLUMN_NAME']:
        null_sql = f"""
            SELECT COUNT(*) - COUNT({rule['COLUMN_NAME']}) as NULL_COUNT
            FROM {source_db}.{source_schema}.{source_table}
        """
        null_result = session.sql(null_sql).collect()[0]
        null_count = int(null_result['NULL_COUNT']) if null_result['NULL_COUNT'] is not None else 0

    duplicate_count = 0
    if rule['RULE_TYPE'] == 'UNIQUENESS' and rule['COLUMN_NAME']:
        duplicate_count = error_count

    pass_rate = round((valid_count / total_count * 100), 2) if total_count > 0 else 100.0
    threshold = float(rule['THRESHOLD_VALUE'])

    if pass_rate >= threshold:
        status = 'PASSED'
    elif pass_rate >= (threshold - 5):
        status = 'WARNING'
    else:
        status = 'FAILED'

    check_end = datetime.now()
    execution_time_ms = (check_end - check_start).total_seconds() * 1000
    execution_credits = round((execution_time_ms / 1000.0) * 0.00001, 6)

    failure_reason = None
    if status == 'FAILED':
        failure_reason = f"Pass rate {pass_rate}% below threshold {threshold}%. Found {error_count} invalid records out of {total_count}."
    elif status == 'WARNING':
        failure_reason = f"Pass rate {pass_rate}% within warning range of threshold {threshold}%."

    session.sql(f"""
        INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS (
            RUN_ID,
            CHECK_TIMESTAMP,
            DATASET_ID,
            DATABASE_NAME,
            SCHEMA_NAME,
            TABLE_NAME,
            COLUMN_NAME,
            RULE_ID,
            RULE_NAME,
            RULE_TYPE,
            RULE_LEVEL,
            TOTAL_RECORDS,
            VALID_RECORDS,
            INVALID_RECORDS,
            NULL_RECORDS,
            DUPLICATE_RECORDS,
            PASS_RATE,
            THRESHOLD,
            CHECK_STATUS,
            EXECUTION_TIME_MS,
            EXECUTION_CREDITS,
            FAILURE_REASON,
            CREATED_TS
        ) VALUES (
            '{run_id}',
            CURRENT_TIMESTAMP(),
            '{dataset_id}',
            '{source_db}',
            '{source_schema}',
            '{source_table}',
            {f"'{rule['COLUMN_NAME']}'" if rule['COLUMN_NAME'] else 'NULL'},
            {rule['RULE_ID']},
            '{rule['RULE_NAME']}',
            '{rule['RULE_TYPE']}',
            '{rule['RULE_LEVEL']}',
            {total_count},
            {valid_count},
            {error_count},
            {null_count},
            {duplicate_count},
            {pass_rate},
            {threshold},
            '{status}',
            {execution_time_ms},
            {execution_credits},
            {f"'{failure_reason}'" if failure_reason else 'NULL'},
            CURRENT_TIMESTAMP()
        )
    """).collect()

    check_id_result = session.sql("""
        SELECT MAX(CHECK_ID) as CHECK_ID 
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
    """).collect()
    check_id = check_id_result[0]['CHECK_ID']

    if status in ('FAILED', 'WARNING') and error_count > 0:
        log_failed_records_detailed_profile(
            session, check_id, run_id, dataset_id, source_db,
            source_schema, source_table, rule, error_count
        )

    return {
        'status': status,
        'pass_rate': pass_rate,
        'total_records': total_count,
        'valid_records': valid_count,
        'failed_records': error_count
    }


def log_error_check_profile(session, run_id, dataset_id, table_name, rule, error_msg):
    """Log checks that failed to execute"""
    try:
        session.sql(f"""
            INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS (
                RUN_ID,
                DATASET_ID,
                TABLE_NAME,
                COLUMN_NAME,
                RULE_ID,
                RULE_NAME,
                RULE_TYPE,
                RULE_LEVEL,
                CHECK_STATUS,
                FAILURE_REASON,
                CHECK_TIMESTAMP,
                CREATED_TS
            ) VALUES (
                '{run_id}',
                '{dataset_id}',
                '{table_name}',
                {f"'{rule['COLUMN_NAME']}'" if rule['COLUMN_NAME'] else 'NULL'},
                {rule['RULE_ID']},
                '{rule['RULE_NAME']}',
                '{rule['RULE_TYPE']}',
                '{rule['RULE_LEVEL']}',
                'ERROR',
                '{error_msg.replace("'", "''")[:500]}',
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            )
        """).collect()
    except:
        pass


def build_check_sql_profile(rule, source_db, source_schema, source_table):
    """Build dynamic SQL from template"""
    sql = rule['SQL_TEMPLATE']

    sql = sql.replace('{{DATABASE}}', source_db)
    sql = sql.replace('{{SCHEMA}}', source_schema)
    sql = sql.replace('{{TABLE}}', source_table)

    if rule['COLUMN_NAME']:
        sql = sql.replace('{{COLUMN}}', rule['COLUMN_NAME'])

    sql = sql.replace('{{THRESHOLD}}', str(int(rule['THRESHOLD_VALUE'])))

    if '{{ALLOWED_VALUES}}' in sql:
        allowed_values = get_allowed_values_profile(rule['COLUMN_NAME'])
        sql = sql.replace('{{ALLOWED_VALUES}}', allowed_values)

    if '{{PARENT_TABLE}}' in sql:
        parent_info = get_parent_table_info_profile(rule['COLUMN_NAME'])
        for key, value in parent_info.items():
            sql = sql.replace(f'{{{{{key}}}}}', value)

    return sql


def get_allowed_values_profile(column_name):
    values_map = {
        'kyc_status': "'Verified', 'Pending', 'Rejected', 'Incomplete'",
        'account_type': "'Savings', 'Checking', 'Credit', 'Investment'",
        'account_status': "'Active', 'Inactive', 'Closed', 'Frozen'",
        'transaction_type': "'Deposit', 'Withdrawal', 'Transfer', 'Payment'"
    }
    return values_map.get(column_name, "''")


def get_parent_table_info_profile(column_name):
    fk_map = {
        'customer_id': {
            'PARENT_DATABASE': 'BANKING_DW',
            'PARENT_SCHEMA': 'BRONZE',
            'PARENT_TABLE': 'STG_CUSTOMER',
            'PARENT_KEY': 'customer_id'
        },
        'account_id': {
            'PARENT_DATABASE': 'BANKING_DW',
            'PARENT_SCHEMA': 'BRONZE',
            'PARENT_TABLE': 'STG_ACCOUNT',
            'PARENT_KEY': 'account_id'
        }
    }
    return fk_map.get(column_name, {})


def log_failed_records_detailed_profile(session, check_id, run_id, dataset_id, source_db,
                                        source_schema, source_table, rule, error_count):
    """Log failed records for profiling"""
    try:
        failure_type_map = {
            'COMPLETENESS': 'NULL_VALUE',
            'UNIQUENESS': 'DUPLICATE_VALUE',
            'VALIDITY': 'INVALID_FORMAT',
            'CONSISTENCY': 'FK_VIOLATION',
            'FRESHNESS': 'STALE_DATA',
            'VOLUME': 'LOW_VOLUME'
        }
        failure_type = failure_type_map.get(rule['RULE_TYPE'], 'VALIDATION_FAILURE')

        is_critical = rule['THRESHOLD_VALUE'] >= 98.0

        remediation_map = {
            'COMPLETENESS': 'Implement default values or make field optional',
            'UNIQUENESS': 'Remove duplicates or update primary key logic',
            'VALIDITY': 'Add validation at source or implement data cleansing',
            'CONSISTENCY': 'Verify foreign key relationships and add missing parent records',
            'FRESHNESS': 'Check ETL schedule and data source availability',
            'VOLUME': 'Investigate data pipeline and source system'
        }
        remediation = remediation_map.get(rule['RULE_TYPE'], 'Review and correct data quality issues')

        session.sql(f"""
            INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_FAILED_RECORDS (
                CHECK_ID,
                RUN_ID,
                DATASET_ID,
                TABLE_NAME,
                COLUMN_NAME,
                RULE_NAME,
                RULE_TYPE,
                FAILURE_TYPE,
                FAILURE_CATEGORY,
                IS_CRITICAL,
                CAN_AUTO_REMEDIATE,
                REMEDIATION_SUGGESTION,
                DETECTED_TS,
                CREATED_TS
            ) VALUES (
                {check_id},
                '{run_id}',
                '{dataset_id}',
                '{source_table}',
                {f"'{rule['COLUMN_NAME']}'" if rule['COLUMN_NAME'] else 'NULL'},
                '{rule['RULE_NAME']}',
                '{rule['RULE_TYPE']}',
                '{failure_type}',
                '{rule['RULE_TYPE']}',
                {is_critical},
                FALSE,
                '{remediation}',
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            )
        """).collect()
    except Exception as e:
        print(f"Warning: Could not log failed records: {str(e)}")


def generate_daily_summary_for_run(session, run_id):
    """Generate/refresh daily summary for a specific run"""
    session.sql(f"""
        DELETE FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
        WHERE SUMMARY_DATE = CURRENT_DATE()
          AND LAST_RUN_ID = '{run_id}'
    """).collect()

    session.sql(f"""
        INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY (
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
            CREATED_TS
        )
        SELECT 
            CURRENT_DATE() as SUMMARY_DATE,
            DATASET_ID,
            DATABASE_NAME,
            SCHEMA_NAME,
            TABLE_NAME,
            'BANKING' as BUSINESS_DOMAIN,
            COUNT(*) as TOTAL_CHECKS,
            SUM(CASE WHEN CHECK_STATUS = 'PASSED' THEN 1 ELSE 0 END) as PASSED_CHECKS,
            SUM(CASE WHEN CHECK_STATUS = 'FAILED' THEN 1 ELSE 0 END) as FAILED_CHECKS,
            SUM(CASE WHEN CHECK_STATUS = 'WARNING' THEN 1 ELSE 0 END) as WARNING_CHECKS,
            SUM(CASE WHEN CHECK_STATUS IN ('ERROR', 'SKIPPED') THEN 1 ELSE 0 END) as SKIPPED_CHECKS,
            ROUND(AVG(PASS_RATE), 2) as DQ_SCORE,
            ROUND(AVG(CASE WHEN RULE_TYPE = 'COMPLETENESS' THEN PASS_RATE END), 2) as COMPLETENESS_SCORE,
            ROUND(AVG(CASE WHEN RULE_TYPE = 'UNIQUENESS' THEN PASS_RATE END), 2) as UNIQUENESS_SCORE,
            ROUND(AVG(CASE WHEN RULE_TYPE = 'VALIDITY' THEN PASS_RATE END), 2) as VALIDITY_SCORE,
            ROUND(AVG(CASE WHEN RULE_TYPE = 'CONSISTENCY' THEN PASS_RATE END), 2) as CONSISTENCY_SCORE,
            ROUND(AVG(CASE WHEN RULE_TYPE = 'FRESHNESS' THEN PASS_RATE END), 2) as FRESHNESS_SCORE,
            ROUND(AVG(CASE WHEN RULE_TYPE = 'VOLUME' THEN PASS_RATE END), 2) as VOLUME_SCORE,
            CASE 
                WHEN AVG(PASS_RATE) >= 95 THEN 'HIGH'
                WHEN AVG(PASS_RATE) >= 85 THEN 'MEDIUM'
                ELSE 'LOW'
            END as TRUST_LEVEL,
            CASE 
                WHEN AVG(PASS_RATE) >= 95 THEN 'A'
                WHEN AVG(PASS_RATE) >= 90 THEN 'B'
                WHEN AVG(PASS_RATE) >= 80 THEN 'C'
                WHEN AVG(PASS_RATE) >= 70 THEN 'D'
                ELSE 'F'
            END as QUALITY_GRADE,
            CASE WHEN AVG(PASS_RATE) >= 90 THEN TRUE ELSE FALSE END as IS_SLA_MET,
            MAX(TOTAL_RECORDS) as TOTAL_RECORDS,
            SUM(INVALID_RECORDS) as FAILED_RECORDS_COUNT,
            ROUND((SUM(INVALID_RECORDS)::FLOAT / NULLIF(MAX(TOTAL_RECORDS), 0)) * 100, 2) as FAILURE_RATE,
            ROUND(SUM(EXECUTION_TIME_MS) / 1000.0, 2) as TOTAL_EXECUTION_TIME_SEC,
            SUM(COALESCE(EXECUTION_CREDITS, 0)) as TOTAL_CREDITS_CONSUMED,
            '{run_id}' as LAST_RUN_ID,
            CURRENT_TIMESTAMP() as LAST_RUN_TS,
            MAX(CHECK_STATUS) as LAST_RUN_STATUS,
            CURRENT_TIMESTAMP() as CREATED_TS
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
        WHERE RUN_ID = '{run_id}'
        GROUP BY DATASET_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME
    """).collect()
$$;

-- ============================================================================
-- PROCEDURE 2: CUSTOM RULE EXECUTION
-- ============================================================================
-- Purpose: Run a single rule ad-hoc with optional threshold override
-- Use Case: Ad-hoc testing, custom scans, threshold experimentation
-- ============================================================================

CREATE OR REPLACE PROCEDURE SP_RUN_CUSTOM_RULE(
    P_DATASET_ID   VARCHAR,
    P_RULE_NAME    VARCHAR,
    P_COLUMN_NAME  VARCHAR DEFAULT NULL,
    P_THRESHOLD    FLOAT   DEFAULT NULL,
    P_RUN_MODE     VARCHAR DEFAULT 'ADHOC'
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
from snowflake.snowpark import Session
import json
import uuid
from datetime import datetime

def main(session, p_dataset_id, p_rule_name, p_column_name, p_threshold, p_run_mode):
    """
    Run a single rule ad-hoc
    
    Parameters:
        p_dataset_id: Required - dataset to check
        p_rule_name: Required - rule name (e.g., 'VALIDITY_EMAIL_FORMAT')
        p_column_name: Optional - column name (NULL for table-level rules)
        p_threshold: Optional - threshold override (if NULL, uses config)
        p_run_mode: Optional - 'ADHOC' (skip mapping lookup if threshold provided)
    
    Returns:
        JSON string with execution summary
    """
    
    if not p_dataset_id:
        raise Exception("p_dataset_id is required for custom rule execution")
    if not p_rule_name:
        raise Exception("p_rule_name is required for custom rule execution")

    try:
        run_id = f"DQ_CUSTOM_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
        start_time = datetime.now()
        triggered_by = session.sql("SELECT CURRENT_USER()").collect()[0][0]

        print("\\n" + "="*80)
        print("DATA QUALITY CUSTOM RULE EXECUTION")
        print("="*80)
        print(f"Run ID       : {run_id}")
        print(f"Dataset      : {p_dataset_id}")
        print(f"Rule Name    : {p_rule_name}")
        print(f"Column       : {p_column_name or '[TABLE LEVEL]'}")
        print(f"Threshold    : {p_threshold if p_threshold is not None else 'USE CONFIG'}")
        print(f"Run Mode     : {p_run_mode}")
        print(f"Triggered By : {triggered_by}")
        print("="*80 + "\\n")

        session.sql(f"""
            INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL (
                RUN_ID, TRIGGERED_BY, START_TS, RUN_STATUS,
                TOTAL_DATASETS, TOTAL_CHECKS, PASSED_CHECKS, FAILED_CHECKS,
                WARNING_CHECKS, SKIPPED_CHECKS, CREATED_TS
            ) VALUES (
                '{run_id}', '{triggered_by}', CURRENT_TIMESTAMP(), 'RUNNING',
                1, 0, 0, 0, 0, 0, CURRENT_TIMESTAMP()
            )
        """).collect()

        ds = fetch_dataset_for_custom(session, p_dataset_id)
        if not ds:
            raise Exception(f"No active dataset found for dataset_id = {p_dataset_id}")

        source_db = ds['SOURCE_DATABASE']
        source_schema = ds['SOURCE_SCHEMA']
        source_table = ds['SOURCE_TABLE']
        business_domain = ds['BUSINESS_DOMAIN']

        # KEY FIX: Check if ADHOC mode with explicit threshold
        if p_threshold is not None and p_run_mode == 'ADHOC':
            print(f"✓ ADHOC mode with explicit threshold ({p_threshold}%) - skipping rule mapping lookup\\n")
            row = fetch_rule_direct(session, p_rule_name, business_domain)
            if not row:
                raise Exception(f"Rule '{p_rule_name}' not found in RULE_MASTER")
            
            rule = {
                'CONFIG_ID': None,
                'DATASET_ID': p_dataset_id,
                'COLUMN_NAME': p_column_name,
                'THRESHOLD_VALUE': float(p_threshold),
                'RULE_ID': row['RULE_ID'],
                'RULE_NAME': row['RULE_NAME'],
                'RULE_TYPE': row['RULE_TYPE'],
                'RULE_LEVEL': row['RULE_LEVEL'],
                'DESCRIPTION': row['DESCRIPTION'],
                'SQL_TEMPLATE': row['SQL_TEMPLATE'],
                'WEIGHT': row['WEIGHT'],
                'PRIORITY': row['PRIORITY']
            }
        else:
            print(f"✓ Looking up rule mapping from DATASET_RULE_CONFIG\\n")
            row = fetch_single_rule_config(session, p_dataset_id, business_domain, p_rule_name, p_column_name)
            if not row:
                raise Exception(
                    f"No active rule mapping found for dataset_id={p_dataset_id}, "
                    f"rule_name={p_rule_name}, column={p_column_name}"
                )
            
            rule = {
                'CONFIG_ID': row['CONFIG_ID'],
                'DATASET_ID': row['DATASET_ID'],
                'COLUMN_NAME': row['COLUMN_NAME'],
                'THRESHOLD_VALUE': row['THRESHOLD_VALUE'],
                'RULE_ID': row['RULE_ID'],
                'RULE_NAME': row['RULE_NAME'],
                'RULE_TYPE': row['RULE_TYPE'],
                'RULE_LEVEL': row['RULE_LEVEL'],
                'DESCRIPTION': row['DESCRIPTION'],
                'SQL_TEMPLATE': row['SQL_TEMPLATE'],
                'WEIGHT': row['WEIGHT'],
                'PRIORITY': row['PRIORITY']
            }
            
            if p_threshold is not None:
                rule['THRESHOLD_VALUE'] = float(p_threshold)

        stats = {
            'total_checks': 1, 'passed_checks': 0, 'failed_checks': 0,
            'warning_checks': 0, 'skipped_checks': 0,
            'total_records_processed': 0, 'total_invalid_records': 0,
            'total_valid_records': 0
        }

        result = execute_custom_check(session, run_id, p_dataset_id, source_db, source_schema, source_table, rule, business_domain)

        stats['total_records_processed'] = result['total_records']
        stats['total_invalid_records'] = result['failed_records']
        stats['total_valid_records'] = result['valid_records']

        if result['status'] == 'PASSED':
            stats['passed_checks'] = 1
        elif result['status'] == 'FAILED':
            stats['failed_checks'] = 1
        elif result['status'] == 'WARNING':
            stats['warning_checks'] = 1
        else:
            stats['skipped_checks'] = 1

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        run_status = 'COMPLETED' if stats['failed_checks'] == 0 else 'COMPLETED_WITH_FAILURES'

        session.sql(f"""
            UPDATE DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
            SET END_TS = CURRENT_TIMESTAMP(), DURATION_SECONDS = {duration},
                RUN_STATUS = '{run_status}', TOTAL_DATASETS = 1,
                TOTAL_CHECKS = {stats['total_checks']}, PASSED_CHECKS = {stats['passed_checks']},
                FAILED_CHECKS = {stats['failed_checks']}, WARNING_CHECKS = {stats['warning_checks']},
                SKIPPED_CHECKS = {stats['skipped_checks']}, ERROR_MESSAGE = NULL
            WHERE RUN_ID = '{run_id}'
        """).collect()

        try:
            generate_daily_summary_for_run_custom(session, run_id)
            print("✓ Daily summary updated for custom rule run\\n")
        except Exception as e:
            print(f"⚠ Warning: Daily summary generation failed: {str(e)}\\n")

        result_payload = {
            'run_id': run_id,
            'status': run_status,
            'duration_seconds': duration,
            'dataset_id': p_dataset_id,
            'rule_name': p_rule_name,
            'column_name': p_column_name,
            'total_checks': stats['total_checks'],
            'passed': stats['passed_checks'],
            'failed': stats['failed_checks'],
            'warnings': stats['warning_checks'],
            'skipped': stats['skipped_checks'],
            'total_records': stats['total_records_processed'],
            'records_processed': stats['total_records_processed'],
            'valid_records': stats['total_valid_records'],
            'invalid_records': stats['total_invalid_records'],
            'pass_rate': result['pass_rate'],
            'threshold': float(rule['THRESHOLD_VALUE'])
        }
        return json.dumps(result_payload, indent=2)

    except Exception as e:
        error_msg = str(e)
        print(f"\\n❌ CUSTOM RULE ERROR: {error_msg}\\n")

        try:
            session.sql(f"""
                UPDATE DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
                SET END_TS = CURRENT_TIMESTAMP(), RUN_STATUS = 'FAILED',
                    ERROR_MESSAGE = '{error_msg.replace("'", "''")[:4000]}'
                WHERE RUN_ID = '{run_id}'
            """).collect()
        except:
            pass

        return json.dumps({'run_id': run_id, 'status': 'FAILED', 'error': error_msg})


def fetch_dataset_for_custom(session, dataset_id):
    query = f"""
        SELECT dc.DATASET_ID, dc.SOURCE_DATABASE, dc.SOURCE_SCHEMA,
               dc.SOURCE_TABLE, dc.BUSINESS_DOMAIN, dc.CRITICALITY
        FROM DATA_QUALITY_DB.DQ_CONFIG.DATASET_CONFIG dc
        WHERE dc.IS_ACTIVE = TRUE AND dc.DATASET_ID = '{dataset_id}'
        LIMIT 1
    """
    rows = session.sql(query).collect()
    return rows[0] if rows else None


def fetch_rule_direct(session, rule_name, business_domain):
    query = f"""
        SELECT rm.RULE_ID, rm.RULE_NAME, rm.RULE_TYPE, rm.RULE_LEVEL,
               rm.DESCRIPTION, rst.SQL_TEMPLATE,
               COALESCE(wm.WEIGHT, 1.0) as WEIGHT,
               COALESCE(wm.PRIORITY, 3) as PRIORITY
        FROM DATA_QUALITY_DB.DQ_CONFIG.RULE_MASTER rm
        INNER JOIN DATA_QUALITY_DB.DQ_CONFIG.RULE_SQL_TEMPLATE rst
            ON rm.RULE_ID = rst.RULE_ID AND rst.IS_ACTIVE = TRUE
        LEFT JOIN DATA_QUALITY_DB.DQ_CONFIG.WEIGHTS_MAPPING wm
            ON rm.RULE_TYPE = wm.RULE_TYPE
            AND wm.BUSINESS_DOMAIN = '{business_domain}'
            AND wm.IS_ACTIVE = TRUE
            AND CURRENT_DATE() BETWEEN wm.EFFECTIVE_DATE 
                AND COALESCE(wm.EXPIRY_DATE, '9999-12-31')
        WHERE rm.RULE_NAME = '{rule_name}' AND rm.IS_ACTIVE = TRUE
        ORDER BY rst.TEMPLATE_VERSION DESC
        LIMIT 1
    """
    rows = session.sql(query).collect()
    return rows[0] if rows else None


def fetch_single_rule_config(session, dataset_id, business_domain, rule_name, column_name):
    column_filter = f"AND NVL(drc.COLUMN_NAME, '') = '{column_name}'" if column_name else "AND drc.COLUMN_NAME IS NULL"
    query = f"""
        SELECT drc.CONFIG_ID, drc.DATASET_ID, drc.COLUMN_NAME, drc.THRESHOLD_VALUE,
               rm.RULE_ID, rm.RULE_NAME, rm.RULE_TYPE, rm.RULE_LEVEL, rm.DESCRIPTION,
               rst.SQL_TEMPLATE, COALESCE(wm.WEIGHT, 1.0) as WEIGHT,
               COALESCE(wm.PRIORITY, 3) as PRIORITY
        FROM DATA_QUALITY_DB.DQ_CONFIG.DATASET_RULE_CONFIG drc
        INNER JOIN DATA_QUALITY_DB.DQ_CONFIG.RULE_MASTER rm ON drc.RULE_ID = rm.RULE_ID
        INNER JOIN DATA_QUALITY_DB.DQ_CONFIG.RULE_SQL_TEMPLATE rst
            ON rm.RULE_ID = rst.RULE_ID AND rst.IS_ACTIVE = TRUE
        LEFT JOIN DATA_QUALITY_DB.DQ_CONFIG.WEIGHTS_MAPPING wm
            ON rm.RULE_TYPE = wm.RULE_TYPE AND wm.BUSINESS_DOMAIN = '{business_domain}'
            AND wm.IS_ACTIVE = TRUE
            AND CURRENT_DATE() BETWEEN wm.EFFECTIVE_DATE AND COALESCE(wm.EXPIRY_DATE, '9999-12-31')
        WHERE drc.DATASET_ID = '{dataset_id}' AND rm.RULE_NAME = '{rule_name}'
          AND drc.IS_ACTIVE = TRUE AND rm.IS_ACTIVE = TRUE {column_filter}
        ORDER BY rst.TEMPLATE_VERSION DESC
        LIMIT 1
    """
    rows = session.sql(query).collect()
    return rows[0] if rows else None


def execute_custom_check(session, run_id, dataset_id, source_db, source_schema, source_table, rule, business_domain):
    check_start = datetime.now()
    
    sql_template = rule['SQL_TEMPLATE']
    
    sql = sql_template.replace('{{DATABASE}}', source_db)
    sql = sql.replace('{{SCHEMA}}', source_schema)
    sql = sql.replace('{{TABLE}}', source_table)
    
    if rule['COLUMN_NAME']:
        sql = sql.replace('{{COLUMN}}', rule['COLUMN_NAME'])
    
    sql = sql.replace('{{THRESHOLD}}', str(int(rule['THRESHOLD_VALUE'])))
    
    if '{{ALLOWED_VALUES}}' in sql:
        allowed_values = get_allowed_values_custom(rule['COLUMN_NAME'])
        sql = sql.replace('{{ALLOWED_VALUES}}', allowed_values)
    
    if '{{PARENT_TABLE}}' in sql or '{{PARENT_DATABASE}}' in sql:
        parent_info = get_parent_table_info_custom(rule['COLUMN_NAME'])
        for key, value in parent_info.items():
            sql = sql.replace(f'{{{{{key}}}}}', value)
    
    print(f"\\n📝 Executing SQL:\\n{sql}\\n")
    
    try:
        result = session.sql(sql).collect()[0]
    except Exception as sql_error:
        raise Exception(f"SQL execution failed: {str(sql_error)}. SQL: {sql[:200]}")
    
    total_count = int(result['TOTAL_COUNT']) if result['TOTAL_COUNT'] is not None else 0
    error_count = int(result['ERROR_COUNT']) if result['ERROR_COUNT'] is not None else 0
    valid_count = total_count - error_count
    
    null_count = 0
    if rule['RULE_TYPE'] == 'COMPLETENESS' and rule['COLUMN_NAME']:
        null_sql = f"SELECT COUNT(*) - COUNT({rule['COLUMN_NAME']}) as NULL_COUNT FROM {source_db}.{source_schema}.{source_table}"
        null_result = session.sql(null_sql).collect()[0]
        null_count = int(null_result['NULL_COUNT']) if null_result['NULL_COUNT'] is not None else 0
    
    duplicate_count = error_count if rule['RULE_TYPE'] == 'UNIQUENESS' and rule['COLUMN_NAME'] else 0
    pass_rate = round((valid_count / total_count * 100), 2) if total_count > 0 else 100.0
    threshold = float(rule['THRESHOLD_VALUE'])
    
    if pass_rate >= threshold:
        status = 'PASSED'
    elif pass_rate >= (threshold - 5):
        status = 'WARNING'
    else:
        status = 'FAILED'
    
    check_end = datetime.now()
    execution_time_ms = (check_end - check_start).total_seconds() * 1000
    execution_credits = round((execution_time_ms / 1000.0) * 0.00001, 6)
    
    failure_reason = None
    if status == 'FAILED':
        failure_reason = f"Pass rate {pass_rate}% below threshold {threshold}%. Found {error_count} invalid records out of {total_count}."
    elif status == 'WARNING':
        failure_reason = f"Pass rate {pass_rate}% within warning range of threshold {threshold}%."
    
    col_value = f"'{rule['COLUMN_NAME']}'" if rule['COLUMN_NAME'] else 'NULL'
    fail_value = f"'{failure_reason.replace(chr(39), chr(39)+chr(39))}'" if failure_reason else 'NULL'
    
    session.sql(f"""
        INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS (
            RUN_ID, CHECK_TIMESTAMP, DATASET_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
            COLUMN_NAME, RULE_ID, RULE_NAME, RULE_TYPE, RULE_LEVEL, TOTAL_RECORDS,
            VALID_RECORDS, INVALID_RECORDS, NULL_RECORDS, DUPLICATE_RECORDS, PASS_RATE,
            THRESHOLD, CHECK_STATUS, EXECUTION_TIME_MS, EXECUTION_CREDITS, FAILURE_REASON, CREATED_TS
        ) VALUES (
            '{run_id}', CURRENT_TIMESTAMP(), '{dataset_id}', '{source_db}', '{source_schema}',
            '{source_table}', {col_value}, {rule['RULE_ID']}, '{rule['RULE_NAME']}',
            '{rule['RULE_TYPE']}', '{rule['RULE_LEVEL']}', {total_count}, {valid_count},
            {error_count}, {null_count}, {duplicate_count}, {pass_rate}, {threshold},
            '{status}', {execution_time_ms}, {execution_credits}, {fail_value}, CURRENT_TIMESTAMP()
        )
    """).collect()
    
    check_id_result = session.sql("SELECT MAX(CHECK_ID) as CHECK_ID FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS").collect()
    check_id = check_id_result[0]['CHECK_ID']
    
    if status in ('FAILED', 'WARNING') and error_count > 0:
        log_failed_records_custom(session, check_id, run_id, dataset_id, source_db, source_schema, source_table, rule, error_count)
    
    return {'status': status, 'pass_rate': pass_rate, 'total_records': total_count, 'valid_records': valid_count, 'failed_records': error_count}


def get_allowed_values_custom(column_name):
    values_map = {
        'kyc_status': "'Verified', 'Pending', 'Rejected', 'Incomplete'",
        'account_type': "'Savings', 'Checking', 'Credit', 'Investment'",
        'account_status': "'Active', 'Inactive', 'Closed', 'Frozen'",
        'transaction_type': "'Deposit', 'Withdrawal', 'Transfer', 'Payment'"
    }
    return values_map.get(column_name, "''")


def get_parent_table_info_custom(column_name):
    fk_map = {
        'customer_id': {'PARENT_DATABASE': 'BANKING_DW', 'PARENT_SCHEMA': 'BRONZE', 'PARENT_TABLE': 'STG_CUSTOMER', 'PARENT_KEY': 'customer_id'},
        'account_id': {'PARENT_DATABASE': 'BANKING_DW', 'PARENT_SCHEMA': 'BRONZE', 'PARENT_TABLE': 'STG_ACCOUNT', 'PARENT_KEY': 'account_id'}
    }
    return fk_map.get(column_name, {})


def log_failed_records_custom(session, check_id, run_id, dataset_id, source_db, source_schema, source_table, rule, error_count):
    try:
        failure_type_map = {
            'COMPLETENESS': 'NULL_VALUE', 'UNIQUENESS': 'DUPLICATE_VALUE',
            'VALIDITY': 'INVALID_FORMAT', 'CONSISTENCY': 'FK_VIOLATION',
            'FRESHNESS': 'STALE_DATA', 'VOLUME': 'LOW_VOLUME'
        }
        failure_type = failure_type_map.get(rule['RULE_TYPE'], 'VALIDATION_FAILURE')
        is_critical = rule['THRESHOLD_VALUE'] >= 98.0
        
        remediation_map = {
            'COMPLETENESS': 'Implement default values or make field optional',
            'UNIQUENESS': 'Remove duplicates or update primary key logic',
            'VALIDITY': 'Add validation at source or implement data cleansing',
            'CONSISTENCY': 'Verify foreign key relationships and add missing parent records',
            'FRESHNESS': 'Check ETL schedule and data source availability',
            'VOLUME': 'Investigate data pipeline and source system'
        }
        remediation = remediation_map.get(rule['RULE_TYPE'], 'Review and correct data quality issues')
        col_value = f"'{rule['COLUMN_NAME']}'" if rule['COLUMN_NAME'] else 'NULL'
        
        session.sql(f"""
            INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_FAILED_RECORDS (
                CHECK_ID, RUN_ID, DATASET_ID, TABLE_NAME, COLUMN_NAME, RULE_NAME,
                RULE_TYPE, FAILURE_TYPE, FAILURE_CATEGORY, IS_CRITICAL,
                CAN_AUTO_REMEDIATE, REMEDIATION_SUGGESTION, DETECTED_TS, CREATED_TS
            ) VALUES (
                {check_id}, '{run_id}', '{dataset_id}', '{source_table}', {col_value},
                '{rule['RULE_NAME']}', '{rule['RULE_TYPE']}', '{failure_type}',
                '{rule['RULE_TYPE']}', {is_critical}, FALSE, '{remediation}',
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
        """).collect()
    except Exception as e:
        print(f"Warning: Could not log failed records for custom rule: {str(e)}")


def generate_daily_summary_for_run_custom(session, run_id):
    session.sql(f"DELETE FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY WHERE SUMMARY_DATE = CURRENT_DATE() AND LAST_RUN_ID = '{run_id}'").collect()
    session.sql(f"""
        INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY (
            SUMMARY_DATE, DATASET_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, BUSINESS_DOMAIN,
            TOTAL_CHECKS, PASSED_CHECKS, FAILED_CHECKS, WARNING_CHECKS, SKIPPED_CHECKS,
            DQ_SCORE, COMPLETENESS_SCORE, UNIQUENESS_SCORE, VALIDITY_SCORE, CONSISTENCY_SCORE,
            FRESHNESS_SCORE, VOLUME_SCORE, TRUST_LEVEL, QUALITY_GRADE, IS_SLA_MET,
            TOTAL_RECORDS, FAILED_RECORDS_COUNT, FAILURE_RATE, TOTAL_EXECUTION_TIME_SEC,
            TOTAL_CREDITS_CONSUMED, LAST_RUN_ID, LAST_RUN_TS, LAST_RUN_STATUS, CREATED_TS
        )
        SELECT CURRENT_DATE(), DATASET_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, 'BANKING',
               COUNT(*), SUM(CASE WHEN CHECK_STATUS = 'PASSED' THEN 1 ELSE 0 END),
               SUM(CASE WHEN CHECK_STATUS = 'FAILED' THEN 1 ELSE 0 END),
               SUM(CASE WHEN CHECK_STATUS = 'WARNING' THEN 1 ELSE 0 END),
               SUM(CASE WHEN CHECK_STATUS IN ('ERROR', 'SKIPPED') THEN 1 ELSE 0 END),
               ROUND(AVG(PASS_RATE), 2),
               ROUND(AVG(CASE WHEN RULE_TYPE = 'COMPLETENESS' THEN PASS_RATE END), 2),
               ROUND(AVG(CASE WHEN RULE_TYPE = 'UNIQUENESS' THEN PASS_RATE END), 2),
               ROUND(AVG(CASE WHEN RULE_TYPE = 'VALIDITY' THEN PASS_RATE END), 2),
               ROUND(AVG(CASE WHEN RULE_TYPE = 'CONSISTENCY' THEN PASS_RATE END), 2),
               ROUND(AVG(CASE WHEN RULE_TYPE = 'FRESHNESS' THEN PASS_RATE END), 2),
               ROUND(AVG(CASE WHEN RULE_TYPE = 'VOLUME' THEN PASS_RATE END), 2),
               CASE WHEN AVG(PASS_RATE) >= 95 THEN 'HIGH' WHEN AVG(PASS_RATE) >= 85 THEN 'MEDIUM' ELSE 'LOW' END,
               CASE WHEN AVG(PASS_RATE) >= 95 THEN 'A' WHEN AVG(PASS_RATE) >= 90 THEN 'B' WHEN AVG(PASS_RATE) >= 80 THEN 'C' WHEN AVG(PASS_RATE) >= 70 THEN 'D' ELSE 'F' END,
               CASE WHEN AVG(PASS_RATE) >= 90 THEN TRUE ELSE FALSE END,
               MAX(TOTAL_RECORDS), SUM(INVALID_RECORDS),
               ROUND((SUM(INVALID_RECORDS)::FLOAT / NULLIF(MAX(TOTAL_RECORDS), 0)) * 100, 2),
               ROUND(SUM(EXECUTION_TIME_MS) / 1000.0, 2),
               SUM(COALESCE(EXECUTION_CREDITS, 0)),
               '{run_id}', CURRENT_TIMESTAMP(), MAX(CHECK_STATUS), CURRENT_TIMESTAMP()
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
        WHERE RUN_ID = '{run_id}'
        GROUP BY DATASET_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME
    """).collect()
$$;

-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================

GRANT USAGE ON PROCEDURE SP_PROFILE_DATASET(VARCHAR, VARCHAR, VARCHAR) TO ROLE ACCOUNTADMIN;
GRANT USAGE ON PROCEDURE SP_RUN_CUSTOM_RULE(VARCHAR, VARCHAR, VARCHAR, FLOAT, VARCHAR) TO ROLE ACCOUNTADMIN;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

SHOW PROCEDURES LIKE 'SP_%';

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================

-- PROFILING EXAMPLES:
-- Profile entire customer dataset
-- CALL DATA_QUALITY_DB.DQ_ENGINE.SP_PROFILE_DATASET('DS_BRONZE_CUSTOMER', NULL, 'FULL');

-- Profile only completeness checks
-- CALL DATA_QUALITY_DB.DQ_ENGINE.SP_PROFILE_DATASET('DS_BRONZE_CUSTOMER', 'COMPLETENESS', 'FULL');

-- CUSTOM RULE EXAMPLES:
-- Run email validation with config threshold
-- CALL DATA_QUALITY_DB.DQ_ENGINE.SP_RUN_CUSTOM_RULE('DS_BRONZE_CUSTOMER', 'VALIDITY_EMAIL_FORMAT', 'email', NULL, 'ADHOC');

-- Run email validation with custom threshold (98%)
-- CALL DATA_QUALITY_DB.DQ_ENGINE.SP_RUN_CUSTOM_RULE('DS_BRONZE_CUSTOMER', 'VALIDITY_EMAIL_FORMAT', 'email', 98.0, 'ADHOC');

-- ============================================================================
-- PROFILING & CUSTOM SCANNING SETUP COMPLETE
-- ============================================================================
-- Next Steps:
-- 1. Profile a dataset: CALL SP_PROFILE_DATASET('DS_BRONZE_CUSTOMER', NULL, 'FULL');
-- 2. Run custom check: CALL SP_RUN_CUSTOM_RULE('DS_BRONZE_CUSTOMER', 'VALIDITY_EMAIL_FORMAT', 'email', 95.0, 'ADHOC');
-- 3. Check results: SELECT * FROM DQ_CHECK_RESULTS ORDER BY CHECK_TIMESTAMP DESC LIMIT 20;
-- ============================================================================
