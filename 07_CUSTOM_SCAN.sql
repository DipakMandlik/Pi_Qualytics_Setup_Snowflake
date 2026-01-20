USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_ENGINE;
USE WAREHOUSE DQ_ANALYTICS_WH;
CREATE OR REPLACE PROCEDURE sp_run_custom_rule(
    p_dataset_id   VARCHAR,
    p_rule_name    VARCHAR,
    p_column_name  VARCHAR DEFAULT NULL,
    p_threshold    FLOAT   DEFAULT NULL,
    p_run_mode     VARCHAR DEFAULT 'ADHOC'
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
            INSERT INTO DATA_QUALITY_DB.DQ_METRICS.dq_run_control (
                run_id, triggered_by, start_ts, run_status,
                total_datasets, total_checks, passed_checks, failed_checks,
                warning_checks, skipped_checks, created_ts
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
            'total_valid_records': 0  # ADDED
        }
        result = execute_custom_check(session, run_id, p_dataset_id, source_db, source_schema, source_table, rule, business_domain)
        stats['total_records_processed'] = result['total_records']
        stats['total_invalid_records'] = result['failed_records']
        stats['total_valid_records'] = result['valid_records']  # ADDED
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
            UPDATE DATA_QUALITY_DB.DQ_METRICS.dq_run_control
            SET end_ts = CURRENT_TIMESTAMP(), duration_seconds = {duration},
                run_status = '{run_status}', total_datasets = 1,
                total_checks = {stats['total_checks']}, passed_checks = {stats['passed_checks']},
                failed_checks = {stats['failed_checks']}, warning_checks = {stats['warning_checks']},
                skipped_checks = {stats['skipped_checks']}, error_message = NULL
            WHERE run_id = '{run_id}'
        """).collect()
        try:
            generate_daily_summary_for_run(session, run_id)
            print("✓ Daily summary updated for custom rule run\\n")
        except Exception as e:
            print(f"⚠ Warning: Daily summary generation failed: {str(e)}\\n")
        # FIXED: Include valid_records in response
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
            'valid_records': stats['total_valid_records'],  # ADDED
            'invalid_records': stats['total_invalid_records'],
            'pass_rate': result['pass_rate'],
            'threshold': float(rule['THRESHOLD_VALUE'])  # ADDED
        }
        return json.dumps(result_payload, indent=2)
    except Exception as e:
        error_msg = str(e)
        print(f"\\n❌ CUSTOM RULE ERROR: {error_msg}\\n")
        try:
            session.sql(f"""
                UPDATE DATA_QUALITY_DB.DQ_METRICS.dq_run_control
                SET end_ts = CURRENT_TIMESTAMP(), run_status = 'FAILED',
                    error_message = '{error_msg.replace("'", "''")[:4000]}'
                WHERE run_id = '{run_id}'
            """).collect()
        except:
            pass
        return json.dumps({'run_id': run_id, 'status': 'FAILED', 'error': error_msg})
def fetch_dataset_for_custom(session, dataset_id):
    query = f"""
        SELECT dc.dataset_id, dc.source_database, dc.source_schema,
               dc.source_table, dc.business_domain, dc.criticality
        FROM DATA_QUALITY_DB.DQ_CONFIG.dataset_config dc
        WHERE dc.is_active = TRUE AND dc.dataset_id = '{dataset_id}'
        LIMIT 1
    """
    rows = session.sql(query).collect()
    return rows[0] if rows else None
def fetch_rule_direct(session, rule_name, business_domain):
    query = f"""
        SELECT rm.rule_id, rm.rule_name, rm.rule_type, rm.rule_level,
               rm.description, rst.sql_template,
               COALESCE(wm.weight, 1.0) as weight,
               COALESCE(wm.priority, 3) as priority
        FROM DATA_QUALITY_DB.DQ_CONFIG.rule_master rm
        INNER JOIN DATA_QUALITY_DB.DQ_CONFIG.rule_sql_template rst
            ON rm.rule_id = rst.rule_id AND rst.is_active = TRUE
        LEFT JOIN DATA_QUALITY_DB.DQ_CONFIG.weights_mapping wm
            ON rm.rule_type = wm.rule_type
            AND wm.business_domain = '{business_domain}'
            AND wm.is_active = TRUE
            AND CURRENT_DATE() BETWEEN wm.effective_date 
                AND COALESCE(wm.expiry_date, '9999-12-31')
        WHERE rm.rule_name = '{rule_name}' AND rm.is_active = TRUE
        ORDER BY rst.template_version DESC
        LIMIT 1
    """
    rows = session.sql(query).collect()
    return rows[0] if rows else None
def fetch_single_rule_config(session, dataset_id, business_domain, rule_name, column_name):
    column_filter = f"AND NVL(drc.column_name, '') = '{column_name}'" if column_name else "AND drc.column_name IS NULL"
    query = f"""
        SELECT drc.config_id, drc.dataset_id, drc.column_name, drc.threshold_value,
               rm.rule_id, rm.rule_name, rm.rule_type, rm.rule_level, rm.description,
               rst.sql_template, COALESCE(wm.weight, 1.0) as weight,
               COALESCE(wm.priority, 3) as priority
        FROM DATA_QUALITY_DB.DQ_CONFIG.dataset_rule_config drc
        INNER JOIN DATA_QUALITY_DB.DQ_CONFIG.rule_master rm ON drc.rule_id = rm.rule_id
        INNER JOIN DATA_QUALITY_DB.DQ_CONFIG.rule_sql_template rst
            ON rm.rule_id = rst.rule_id AND rst.is_active = TRUE
        LEFT JOIN DATA_QUALITY_DB.DQ_CONFIG.weights_mapping wm
            ON rm.rule_type = wm.rule_type AND wm.business_domain = '{business_domain}'
            AND wm.is_active = TRUE
            AND CURRENT_DATE() BETWEEN wm.effective_date AND COALESCE(wm.expiry_date, '9999-12-31')
        WHERE drc.dataset_id = '{dataset_id}' AND rm.rule_name = '{rule_name}'
          AND drc.is_active = TRUE AND rm.is_active = TRUE {column_filter}
        ORDER BY rst.template_version DESC
        LIMIT 1
    """
    rows = session.sql(query).collect()
    return rows[0] if rows else None
def execute_custom_check(session, run_id, dataset_id, source_db, source_schema, source_table, rule, business_domain):
    check_start = datetime.now()
    
    # Build the SQL with proper placeholder replacement
    sql_template = rule['SQL_TEMPLATE']
    
    # Replace all placeholders - CRITICAL FIX
    sql = sql_template.replace('{{DATABASE}}', source_db)
    sql = sql.replace('{{SCHEMA}}', source_schema)
    sql = sql.replace('{{TABLE}}', source_table)
    
    # Handle column placeholder
    if rule['COLUMN_NAME']:
        sql = sql.replace('{{COLUMN}}', rule['COLUMN_NAME'])
    
    # Replace threshold
    sql = sql.replace('{{THRESHOLD}}', str(int(rule['THRESHOLD_VALUE'])))
    
    # Handle special placeholders if present
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
        null_sql = f"SELECT COUNT(*) - COUNT({rule['COLUMN_NAME']}) as null_count FROM {source_db}.{source_schema}.{source_table}"
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
        INSERT INTO DATA_QUALITY_DB.DQ_METRICS.dq_check_results (
            run_id, check_timestamp, dataset_id, database_name, schema_name, table_name,
            column_name, rule_id, rule_name, rule_type, rule_level, total_records,
            valid_records, invalid_records, null_records, duplicate_records, pass_rate,
            threshold, check_status, execution_time_ms, execution_credits, failure_reason, created_ts
        ) VALUES (
            '{run_id}', CURRENT_TIMESTAMP(), '{dataset_id}', '{source_db}', '{source_schema}',
            '{source_table}', {col_value}, {rule['RULE_ID']}, '{rule['RULE_NAME']}',
            '{rule['RULE_TYPE']}', '{rule['RULE_LEVEL']}', {total_count}, {valid_count},
            {error_count}, {null_count}, {duplicate_count}, {pass_rate}, {threshold},
            '{status}', {execution_time_ms}, {execution_credits}, {fail_value}, CURRENT_TIMESTAMP()
        )
    """).collect()
    
    check_id_result = session.sql("SELECT MAX(check_id) as check_id FROM DATA_QUALITY_DB.DQ_METRICS.dq_check_results").collect()
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
            INSERT INTO DATA_QUALITY_DB.DQ_METRICS.dq_failed_records (
                check_id, run_id, dataset_id, table_name, column_name, rule_name,
                rule_type, failure_type, failure_category, is_critical,
                can_auto_remediate, remediation_suggestion, detected_ts, created_ts
            ) VALUES (
                {check_id}, '{run_id}', '{dataset_id}', '{source_table}', {col_value},
                '{rule['RULE_NAME']}', '{rule['RULE_TYPE']}', '{failure_type}',
                '{rule['RULE_TYPE']}', {is_critical}, FALSE, '{remediation}',
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
        """).collect()
    except Exception as e:
        print(f"Warning: Could not log failed records for custom rule: {str(e)}")
def generate_daily_summary_for_run(session, run_id):
    session.sql(f"DELETE FROM DATA_QUALITY_DB.DQ_METRICS.dq_daily_summary WHERE summary_date = CURRENT_DATE() AND last_run_id = '{run_id}'").collect()
    session.sql(f"""
        INSERT INTO DATA_QUALITY_DB.DQ_METRICS.dq_daily_summary (
            summary_date, dataset_id, database_name, schema_name, table_name, business_domain,
            total_checks, passed_checks, failed_checks, warning_checks, skipped_checks,
            dq_score, completeness_score, uniqueness_score, validity_score, consistency_score,
            freshness_score, volume_score, trust_level, quality_grade, is_sla_met,
            total_records, failed_records_count, failure_rate, total_execution_time_sec,
            total_credits_consumed, last_run_id, last_run_ts, last_run_status, created_ts
        )
        SELECT CURRENT_DATE(), dataset_id, database_name, schema_name, table_name, 'BANKING',
               COUNT(*), SUM(CASE WHEN check_status = 'PASSED' THEN 1 ELSE 0 END),
               SUM(CASE WHEN check_status = 'FAILED' THEN 1 ELSE 0 END),
               SUM(CASE WHEN check_status = 'WARNING' THEN 1 ELSE 0 END),
               SUM(CASE WHEN check_status IN ('ERROR', 'SKIPPED') THEN 1 ELSE 0 END),
               ROUND(AVG(pass_rate), 2),
               ROUND(AVG(CASE WHEN rule_type = 'COMPLETENESS' THEN pass_rate END), 2),
               ROUND(AVG(CASE WHEN rule_type = 'UNIQUENESS' THEN pass_rate END), 2),
               ROUND(AVG(CASE WHEN rule_type = 'VALIDITY' THEN pass_rate END), 2),
               ROUND(AVG(CASE WHEN rule_type = 'CONSISTENCY' THEN pass_rate END), 2),
               ROUND(AVG(CASE WHEN rule_type = 'FRESHNESS' THEN pass_rate END), 2),
               ROUND(AVG(CASE WHEN rule_type = 'VOLUME' THEN pass_rate END), 2),
               CASE WHEN AVG(pass_rate) >= 95 THEN 'HIGH' WHEN AVG(pass_rate) >= 85 THEN 'MEDIUM' ELSE 'LOW' END,
               CASE WHEN AVG(pass_rate) >= 95 THEN 'A' WHEN AVG(pass_rate) >= 90 THEN 'B' WHEN AVG(pass_rate) >= 80 THEN 'C' WHEN AVG(pass_rate) >= 70 THEN 'D' ELSE 'F' END,
               CASE WHEN AVG(pass_rate) >= 90 THEN TRUE ELSE FALSE END,
               MAX(total_records), SUM(invalid_records),
               ROUND((SUM(invalid_records)::FLOAT / NULLIF(MAX(total_records), 0)) * 100, 2),
               ROUND(SUM(execution_time_ms) / 1000.0, 2),
               SUM(COALESCE(execution_credits, 0)),
               '{run_id}', CURRENT_TIMESTAMP(), MAX(check_status), CURRENT_TIMESTAMP()
        FROM DATA_QUALITY_DB.DQ_METRICS.dq_check_results
        WHERE run_id = '{run_id}'
        GROUP BY dataset_id, database_name, schema_name, table_name
    """).collect()
$$;
GRANT USAGE ON PROCEDURE sp_run_custom_rule(VARCHAR, VARCHAR, VARCHAR, FLOAT, VARCHAR) TO ROLE ACCOUNTADMIN;