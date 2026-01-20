

-- =====================================================================================
--                   DATA QUALITY ENGINE - COMPLETE METRICS COLLECTION
-- =====================================================================================
-- Purpose: Config-driven DQ execution with FULL metrics population
-- All columns in all metrics tables are now properly filled
-- =====================================================================================

USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_ENGINE;
USE WAREHOUSE DQ_ANALYTICS_WH;


CREATE OR REPLACE PROCEDURE sp_execute_dq_checks(
    p_dataset_id VARCHAR DEFAULT NULL,
    p_rule_type VARCHAR DEFAULT NULL,
    p_run_mode VARCHAR DEFAULT 'FULL'
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
    Main Data Quality Check Execution Engine with COMPLETE metrics collection
    """
    
    try:
        # Initialize run
        run_id = f"DQ_RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
        start_time = datetime.now()
        triggered_by = session.sql("SELECT CURRENT_USER()").collect()[0][0]
        
        print(f"\n{'='*80}")
        print(f"DATA QUALITY EXECUTION ENGINE - FULL METRICS COLLECTION")
        print(f"{'='*80}")
        print(f"Run ID: {run_id}")
        print(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Triggered By: {triggered_by}")
        print(f"Dataset Filter: {p_dataset_id or 'ALL'}")
        print(f"Rule Type Filter: {p_rule_type or 'ALL'}")
        print(f"Run Mode: {p_run_mode}")
        print(f"{'='*80}\n")
        
        # Insert run control record with ALL fields
        session.sql(f"""
            INSERT INTO DATA_QUALITY_DB.DQ_METRICS.dq_run_control (
                run_id, 
                triggered_by, 
                start_ts, 
                run_status,
                total_datasets,
                total_checks,
                passed_checks,
                failed_checks,
                warning_checks,
                skipped_checks,
                created_ts
            ) VALUES (
                '{run_id}', 
                '{triggered_by}',
                CURRENT_TIMESTAMP(),
                'RUNNING',
                0, 0, 0, 0, 0, 0,
                CURRENT_TIMESTAMP()
            )
        """).collect()
        
        # Fetch datasets to process
        datasets = fetch_datasets(session, p_dataset_id, p_run_mode)
        
        if not datasets:
            raise Exception("No active datasets found")
        
        print(f"📊 Processing {len(datasets)} dataset(s)\n")
        
        # Initialize counters
        stats = {
            'total_checks': 0,
            'passed_checks': 0,
            'failed_checks': 0,
            'warning_checks': 0,
            'skipped_checks': 0,
            'total_records_processed': 0,
            'total_invalid_records': 0
        }
        
        # Process each dataset
        for dataset in datasets:
            process_dataset(session, run_id, dataset, p_rule_type, stats)
        
        # Finalize run with ALL metrics
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        run_status = 'COMPLETED' if stats['failed_checks'] == 0 else 'COMPLETED_WITH_FAILURES'
        
        session.sql(f"""
            UPDATE DATA_QUALITY_DB.DQ_METRICS.dq_run_control
            SET 
                end_ts = CURRENT_TIMESTAMP(),
                duration_seconds = {duration},
                run_status = '{run_status}',
                total_datasets = {len(datasets)},
                total_checks = {stats['total_checks']},
                passed_checks = {stats['passed_checks']},
                failed_checks = {stats['failed_checks']},
                warning_checks = {stats['warning_checks']},
                skipped_checks = {stats['skipped_checks']},
                error_message = NULL
            WHERE run_id = '{run_id}'
        """).collect()
        
        # Print summary
        print(f"\n{'='*80}")
        print(f"EXECUTION SUMMARY")
        print(f"{'='*80}")
        print(f"Status: {run_status}")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Datasets: {len(datasets)}")
        print(f"Total Checks: {stats['total_checks']}")
        print(f"✓ Passed: {stats['passed_checks']}")
        print(f"✗ Failed: {stats['failed_checks']}")
        print(f"⚠ Warnings: {stats['warning_checks']}")
        print(f"⊘ Skipped: {stats['skipped_checks']}")
        print(f"Records Processed: {stats['total_records_processed']:,}")
        print(f"Invalid Records: {stats['total_invalid_records']:,}")
        
        if stats['total_checks'] > 0:
            pass_rate = round((stats['passed_checks'] / stats['total_checks'] * 100), 2)
            print(f"Overall Pass Rate: {pass_rate}%")
        
        print(f"{'='*80}\n")
        
        # Generate daily summary with ALL metrics
        try:
            generate_daily_summary(session, run_id)
            print("✓ Daily summary generated with complete metrics\n")
        except Exception as e:
            print(f"⚠ Warning: Daily summary generation failed: {str(e)}\n")
        
        # Return result
        result = {
            'run_id': run_id,
            'status': run_status,
            'duration_seconds': duration,
            'datasets_processed': len(datasets),
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
        print(f"\n❌ FATAL ERROR: {error_msg}\n")
        
        try:
            session.sql(f"""
                UPDATE DATA_QUALITY_DB.DQ_METRICS.dq_run_control
                SET 
                    end_ts = CURRENT_TIMESTAMP(),
                    run_status = 'FAILED',
                    error_message = '{error_msg.replace("'", "''")[:4000]}'
                WHERE run_id = '{run_id}'
            """).collect()
        except:
            pass
        
        return json.dumps({'run_id': run_id, 'status': 'FAILED', 'error': error_msg})


def fetch_datasets(session, p_dataset_id, p_run_mode):
    """Fetch active datasets based on filters"""
    
    dataset_filter = f"AND dc.dataset_id = '{p_dataset_id}'" if p_dataset_id else ""
    criticality_filter = "AND dc.criticality = 'CRITICAL'" if p_run_mode == 'CRITICAL_ONLY' else ""
    
    query = f"""
        SELECT 
            dc.dataset_id,
            dc.source_database,
            dc.source_schema,
            dc.source_table,
            dc.business_domain,
            dc.criticality
        FROM DATA_QUALITY_DB.DQ_CONFIG.dataset_config dc
        WHERE dc.is_active = TRUE
        {dataset_filter}
        {criticality_filter}
        ORDER BY 
            CASE dc.criticality 
                WHEN 'CRITICAL' THEN 1 
                WHEN 'HIGH' THEN 2 
                WHEN 'MEDIUM' THEN 3 
                ELSE 4 
            END
    """
    
    return session.sql(query).collect()


def process_dataset(session, run_id, dataset, p_rule_type, stats):
    """Process all rules for a single dataset with FULL metrics"""
    
    dataset_id = dataset['DATASET_ID']
    source_db = dataset['SOURCE_DATABASE']
    source_schema = dataset['SOURCE_SCHEMA']
    source_table = dataset['SOURCE_TABLE']
    business_domain = dataset['BUSINESS_DOMAIN']
    
    print(f"📁 Dataset: {dataset_id}")
    print(f"   Table: {source_db}.{source_schema}.{source_table}")
    print(f"   Domain: {business_domain}")
    
    # Fetch rules for this dataset
    rules = fetch_rules(session, dataset_id, business_domain, p_rule_type)
    
    if not rules:
        print(f"   ⚠ No active rules configured\n")
        return
    
    print(f"   Rules: {len(rules)}\n")
    
    # Execute each rule
    for rule in rules:
        stats['total_checks'] += 1
        
        try:
            result = execute_check(session, run_id, dataset_id, source_db, 
                                  source_schema, source_table, rule, business_domain)
            
            # Update stats with ALL metrics
            stats['total_records_processed'] += result['total_records']
            stats['total_invalid_records'] += result['failed_records']
            
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
            
            # Print result
            col_display = f"[{rule['COLUMN_NAME']}]" if rule['COLUMN_NAME'] else "[TABLE]"
            print(f"   {icon} {rule['RULE_TYPE']:15s} {col_display:25s} "
                  f"{result['status']:8s} ({result['pass_rate']:6.2f}% | "
                  f"{result['valid_records']:,}/{result['total_records']:,})")
            
        except Exception as e:
            stats['skipped_checks'] += 1
            print(f"   ⊘ ERROR: {rule['RULE_NAME']} - {str(e)}")
            
            # Log error in check results
            log_error_check(session, run_id, dataset_id, source_table, rule, str(e))
    
    print()


def fetch_rules(session, dataset_id, business_domain, p_rule_type):
    """Fetch active rules for a dataset"""
    
    rule_type_filter = f"AND rm.rule_type = '{p_rule_type}'" if p_rule_type else ""
    
    query = f"""
        SELECT 
            drc.config_id,
            drc.dataset_id,
            drc.column_name,
            drc.threshold_value,
            rm.rule_id,
            rm.rule_name,
            rm.rule_type,
            rm.rule_level,
            rm.description,
            rst.sql_template,
            COALESCE(wm.weight, 1.0) as weight,
            COALESCE(wm.priority, 3) as priority
        FROM DATA_QUALITY_DB.DQ_CONFIG.dataset_rule_config drc
        INNER JOIN DATA_QUALITY_DB.DQ_CONFIG.rule_master rm
            ON drc.rule_id = rm.rule_id
        INNER JOIN DATA_QUALITY_DB.DQ_CONFIG.rule_sql_template rst
            ON rm.rule_id = rst.rule_id
            AND rst.is_active = TRUE
        LEFT JOIN DATA_QUALITY_DB.DQ_CONFIG.weights_mapping wm
            ON rm.rule_type = wm.rule_type
            AND wm.business_domain = '{business_domain}'
            AND wm.is_active = TRUE
            AND CURRENT_DATE() BETWEEN wm.effective_date 
                AND COALESCE(wm.expiry_date, '9999-12-31')
        WHERE drc.dataset_id = '{dataset_id}'
        AND drc.is_active = TRUE
        AND rm.is_active = TRUE
        {rule_type_filter}
        ORDER BY 
            COALESCE(wm.priority, 3),
            CASE rm.rule_type
                WHEN 'COMPLETENESS' THEN 1
                WHEN 'UNIQUENESS' THEN 2
                WHEN 'VALIDITY' THEN 3
                WHEN 'CONSISTENCY' THEN 4
                WHEN 'FRESHNESS' THEN 5
                WHEN 'VOLUME' THEN 6
            END,
            drc.column_name
    """
    
    return session.sql(query).collect()


def execute_check(session, run_id, dataset_id, source_db, source_schema, source_table, rule, business_domain):
    """Execute a single DQ check with COMPLETE metrics collection"""
    
    check_start = datetime.now()
    
    # Build dynamic SQL
    sql = build_check_sql(rule, source_db, source_schema, source_table)
    
    # Execute the check
    result = session.sql(sql).collect()[0]
    
    total_count = int(result['TOTAL_COUNT'])
    error_count = int(result['ERROR_COUNT'])
    valid_count = total_count - error_count
    
    # Calculate null count separately if it's a completeness check
    null_count = 0
    if rule['RULE_TYPE'] == 'COMPLETENESS' and rule['COLUMN_NAME']:
        null_sql = f"""
            SELECT COUNT(*) - COUNT({rule['COLUMN_NAME']}) as null_count
            FROM {source_db}.{source_schema}.{source_table}
        """
        null_result = session.sql(null_sql).collect()[0]
        null_count = int(null_result['NULL_COUNT'])
    
    # Calculate duplicate count if it's a uniqueness check
    duplicate_count = 0
    if rule['RULE_TYPE'] == 'UNIQUENESS' and rule['COLUMN_NAME']:
        duplicate_count = error_count
    
    # Calculate metrics
    pass_rate = round((valid_count / total_count * 100), 2) if total_count > 0 else 100.0
    threshold = float(rule['THRESHOLD_VALUE'])
    
    # Determine status
    if pass_rate >= threshold:
        status = 'PASSED'
    elif pass_rate >= (threshold - 5):
        status = 'WARNING'
    else:
        status = 'FAILED'
    
    check_end = datetime.now()
    execution_time_ms = (check_end - check_start).total_seconds() * 1000
    
    # Estimate credits (rough approximation based on execution time)
    execution_credits = round((execution_time_ms / 1000.0) * 0.00001, 6)
    
    # Prepare failure reason if check failed
    failure_reason = None
    if status == 'FAILED':
        failure_reason = f"Pass rate {pass_rate}% below threshold {threshold}%. Found {error_count} invalid records out of {total_count}."
    elif status == 'WARNING':
        failure_reason = f"Pass rate {pass_rate}% within warning range of threshold {threshold}%."
    
    # Log results with ALL columns populated
    session.sql(f"""
        INSERT INTO DATA_QUALITY_DB.DQ_METRICS.dq_check_results (
            run_id,
            check_timestamp,
            dataset_id,
            database_name,
            schema_name,
            table_name,
            column_name,
            rule_id,
            rule_name,
            rule_type,
            rule_level,
            total_records,
            valid_records,
            invalid_records,
            null_records,
            duplicate_records,
            pass_rate,
            threshold,
            check_status,
            execution_time_ms,
            execution_credits,
            failure_reason,
            created_ts
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
    
    # Get the check_id that was just inserted
    check_id_result = session.sql("""
        SELECT MAX(check_id) as check_id 
        FROM DATA_QUALITY_DB.DQ_METRICS.dq_check_results
    """).collect()
    check_id = check_id_result[0]['CHECK_ID']
    
    # Log failed records with sample data if check failed
    if status in ('FAILED', 'WARNING') and error_count > 0:
        log_failed_records_detailed(session, check_id, run_id, dataset_id, source_db,
                                   source_schema, source_table, rule, error_count)
    
    return {
        'status': status,
        'pass_rate': pass_rate,
        'total_records': total_count,
        'valid_records': valid_count,
        'failed_records': error_count
    }


def log_error_check(session, run_id, dataset_id, table_name, rule, error_msg):
    """Log checks that failed to execute"""
    
    try:
        session.sql(f"""
            INSERT INTO DATA_QUALITY_DB.DQ_METRICS.dq_check_results (
                run_id,
                dataset_id,
                table_name,
                column_name,
                rule_id,
                rule_name,
                rule_type,
                rule_level,
                check_status,
                failure_reason,
                check_timestamp,
                created_ts
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


def build_check_sql(rule, source_db, source_schema, source_table):
    """Build dynamic SQL from template"""
    
    sql = rule['SQL_TEMPLATE']
    
    # Replace standard placeholders
    sql = sql.replace('{{DATABASE}}', source_db)
    sql = sql.replace('{{SCHEMA}}', source_schema)
    sql = sql.replace('{{TABLE}}', source_table)
    
    if rule['COLUMN_NAME']:
        sql = sql.replace('{{COLUMN}}', rule['COLUMN_NAME'])
    
    sql = sql.replace('{{THRESHOLD}}', str(int(rule['THRESHOLD_VALUE'])))
    
    # Handle special placeholders
    if '{{ALLOWED_VALUES}}' in sql:
        allowed_values = get_allowed_values(rule['COLUMN_NAME'])
        sql = sql.replace('{{ALLOWED_VALUES}}', allowed_values)
    
    if '{{PARENT_TABLE}}' in sql:
        parent_info = get_parent_table_info(rule['COLUMN_NAME'])
        for key, value in parent_info.items():
            sql = sql.replace(f'{{{{{key}}}}}', value)
    
    return sql


def get_allowed_values(column_name):
    """Get allowed values for validation"""
    
    values_map = {
        'kyc_status': "'Verified', 'Pending', 'Rejected', 'Incomplete'",
        'account_type': "'Savings', 'Checking', 'Credit', 'Investment'",
        'account_status': "'Active', 'Inactive', 'Closed', 'Frozen'",
        'transaction_type': "'Deposit', 'Withdrawal', 'Transfer', 'Payment'"
    }
    
    return values_map.get(column_name, "''")


def get_parent_table_info(column_name):
    """Get parent table info for FK checks"""
    
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


def log_failed_records_detailed(session, check_id, run_id, dataset_id, source_db,
                                source_schema, source_table, rule, error_count):
    """Log detailed sample of failed records with ALL fields populated"""
    
    try:
        # Determine failure type based on rule type
        failure_type_map = {
            'COMPLETENESS': 'NULL_VALUE',
            'UNIQUENESS': 'DUPLICATE_VALUE',
            'VALIDITY': 'INVALID_FORMAT',
            'CONSISTENCY': 'FK_VIOLATION',
            'FRESHNESS': 'STALE_DATA',
            'VOLUME': 'LOW_VOLUME'
        }
        
        failure_type = failure_type_map.get(rule['RULE_TYPE'], 'VALIDATION_FAILURE')
        
        # Determine if critical based on threshold
        is_critical = rule['THRESHOLD_VALUE'] >= 98.0
        
        # Suggest remediation
        remediation_map = {
            'COMPLETENESS': 'Implement default values or make field optional',
            'UNIQUENESS': 'Remove duplicates or update primary key logic',
            'VALIDITY': 'Add validation at source or implement data cleansing',
            'CONSISTENCY': 'Verify foreign key relationships and add missing parent records',
            'FRESHNESS': 'Check ETL schedule and data source availability',
            'VOLUME': 'Investigate data pipeline and source system'
        }
        
        remediation = remediation_map.get(rule['RULE_TYPE'], 'Review and correct data quality issues')
        
        # Insert with ALL fields populated
        session.sql(f"""
            INSERT INTO DATA_QUALITY_DB.DQ_METRICS.dq_failed_records (
                check_id,
                run_id,
                dataset_id,
                table_name,
                column_name,
                rule_name,
                rule_type,
                failure_type,
                failure_category,
                is_critical,
                can_auto_remediate,
                remediation_suggestion,
                detected_ts,
                created_ts
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


def generate_daily_summary(session, run_id):
    """Generate comprehensive daily summary with ALL metrics populated"""
    
    # Delete existing summary for today
    session.sql(f"""
        DELETE FROM DATA_QUALITY_DB.DQ_METRICS.dq_daily_summary
        WHERE summary_date = CURRENT_DATE()
    """).collect()
    
    # Insert new comprehensive summary
    session.sql(f"""
        INSERT INTO DATA_QUALITY_DB.DQ_METRICS.dq_daily_summary (
            summary_date,
            dataset_id,
            database_name,
            schema_name,
            table_name,
            business_domain,
            total_checks,
            passed_checks,
            failed_checks,
            warning_checks,
            skipped_checks,
            dq_score,
            completeness_score,
            uniqueness_score,
            validity_score,
            consistency_score,
            freshness_score,
            volume_score,
            trust_level,
            quality_grade,
            is_sla_met,
            total_records,
            failed_records_count,
            failure_rate,
            total_execution_time_sec,
            total_credits_consumed,
            last_run_id,
            last_run_ts,
            last_run_status,
            created_ts
        )
        SELECT 
            CURRENT_DATE() as summary_date,
            dataset_id,
            database_name,
            schema_name,
            table_name,
            'BANKING' as business_domain,
            COUNT(*) as total_checks,
            SUM(CASE WHEN check_status = 'PASSED' THEN 1 ELSE 0 END) as passed_checks,
            SUM(CASE WHEN check_status = 'FAILED' THEN 1 ELSE 0 END) as failed_checks,
            SUM(CASE WHEN check_status = 'WARNING' THEN 1 ELSE 0 END) as warning_checks,
            SUM(CASE WHEN check_status IN ('ERROR', 'SKIPPED') THEN 1 ELSE 0 END) as skipped_checks,
            ROUND(AVG(pass_rate), 2) as dq_score,
            ROUND(AVG(CASE WHEN rule_type = 'COMPLETENESS' THEN pass_rate END), 2) as completeness_score,
            ROUND(AVG(CASE WHEN rule_type = 'UNIQUENESS' THEN pass_rate END), 2) as uniqueness_score,
            ROUND(AVG(CASE WHEN rule_type = 'VALIDITY' THEN pass_rate END), 2) as validity_score,
            ROUND(AVG(CASE WHEN rule_type = 'CONSISTENCY' THEN pass_rate END), 2) as consistency_score,
            ROUND(AVG(CASE WHEN rule_type = 'FRESHNESS' THEN pass_rate END), 2) as freshness_score,
            ROUND(AVG(CASE WHEN rule_type = 'VOLUME' THEN pass_rate END), 2) as volume_score,
            CASE 
                WHEN AVG(pass_rate) >= 95 THEN 'HIGH'
                WHEN AVG(pass_rate) >= 85 THEN 'MEDIUM'
                ELSE 'LOW'
            END as trust_level,
            CASE 
                WHEN AVG(pass_rate) >= 95 THEN 'A'
                WHEN AVG(pass_rate) >= 90 THEN 'B'
                WHEN AVG(pass_rate) >= 80 THEN 'C'
                WHEN AVG(pass_rate) >= 70 THEN 'D'
                ELSE 'F'
            END as quality_grade,
            CASE WHEN AVG(pass_rate) >= 90 THEN TRUE ELSE FALSE END as is_sla_met,
            MAX(total_records) as total_records,
            SUM(invalid_records) as failed_records_count,
            ROUND((SUM(invalid_records)::FLOAT / NULLIF(MAX(total_records), 0)) * 100, 2) as failure_rate,
            ROUND(SUM(execution_time_ms) / 1000.0, 2) as total_execution_time_sec,
            SUM(COALESCE(execution_credits, 0)) as total_credits_consumed,
            '{run_id}' as last_run_id,
            CURRENT_TIMESTAMP() as last_run_ts,
            MAX(check_status) as last_run_status,
            CURRENT_TIMESTAMP() as created_ts
        FROM DATA_QUALITY_DB.DQ_METRICS.dq_check_results
        WHERE run_id = '{run_id}'
        GROUP BY dataset_id, database_name, schema_name, table_name
    """).collect()

$$;

-- =====================================================================================
-- USAGE
-- =====================================================================================
CALL DATA_QUALITY_DB.DQ_ENGINE.sp_execute_dq_checks(NULL, NULL, 'FULL');





-- EXAMPLE USAGE
-- =====================================================================================
-- Run all checks for all datasets:
CALL DATA_QUALITY_DB.DQ_ENGINE.sp_execute_dq_checks(NULL, NULL, 'FULL');

-- Run checks for specific dataset:
-- CALL DATA_QUALITY_DB.DQ_ENGINE.sp_execute_dq_checks('DS_CUSTOMER', NULL, 'FULL');

-- Run only completeness checks:
-- CALL DATA_QUALITY_DB.DQ_ENGINE.sp_execute_dq_checks(NULL, 'COMPLETENESS', 'FULL');

-- Run only critical datasets:
-- CALL DATA_QUALITY_DB.DQ_ENGINE.sp_execute_dq_checks(NULL, NULL, 'CRITICAL_ONLY');