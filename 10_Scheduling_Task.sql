-- ============================================================================
-- AUTOMATED SCHEDULING & TASK SYSTEM - PRODUCTION VERSION
-- Pi-Qualytics Data Quality Platform
-- ============================================================================
-- Purpose: Automated scheduling for profiling and custom checks
-- Prerequisites: All previous setup scripts executed (01-09)
-- Version: 1.0.0
-- ============================================================================
-- 
-- This file contains:
-- 1. SP_RUN_DATA_PROFILING - Profile a single dataset (called by scheduler)
-- 2. SP_RUN_CUSTOM_CHECKS_BATCH - Run custom checks for a dataset (selective)
-- 3. SP_PROCESS_DUE_SCHEDULES - Main scheduler processor (checks SCAN_SCHEDULES)
-- 4. DQ_SCHEDULE_PROCESSOR_TASK - Snowflake Task (runs every minute)
-- 
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_METRICS;
USE WAREHOUSE DQ_ANALYTICS_WH;

-- ============================================================================
-- PROCEDURE 1: DATA PROFILING
-- ============================================================================
-- Purpose: Profile a single dataset (generates column profiles + synthetic checks)
-- Called by: Scheduler, API endpoints
-- ============================================================================

CREATE OR REPLACE PROCEDURE SP_RUN_DATA_PROFILING(
    P_DATASET_ID VARCHAR,
    P_DATABASE   VARCHAR,
    P_SCHEMA     VARCHAR,
    P_TABLE      VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import snowflake.snowpark as snowpark
from datetime import datetime
import uuid
import json

def main(session, p_dataset_id, p_database, p_schema, p_table):
    try:
        # 1. Setup Context
        start_time = datetime.now()
        run_id = f"DQ_PROFILE_{datetime.now().strftime('%Y%m%d%H%M%S')}_{str(uuid.uuid4())[:8].upper()}"
        triggered_by = 'SCHEDULED_TASK'
        
        # 2. Insert Run Control (RUNNING)
        session.sql(f"""
            INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL (
                RUN_ID, TRIGGERED_BY, START_TS, RUN_STATUS, TOTAL_CHECKS, CREATED_TS
            ) VALUES (
                '{run_id}', '{triggered_by}', CURRENT_TIMESTAMP(), 'RUNNING', 0, CURRENT_TIMESTAMP()
            )
        """).collect()
        
        # 3. Fetch Columns
        cols_query = f"""
            SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
            FROM {p_database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_CATALOG = '{p_database.upper()}'
              AND TABLE_SCHEMA = '{p_schema.upper()}'
              AND TABLE_NAME = '{p_table.upper()}'
            ORDER BY ORDINAL_POSITION
        """
        columns = session.sql(cols_query).collect()
        
        if not columns:
             session.sql(f"""
                UPDATE DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
                SET RUN_STATUS = 'FAILED', END_TS = CURRENT_TIMESTAMP(), 
                    ERROR_MESSAGE = 'No columns found'
                WHERE RUN_ID = '{run_id}'
            """).collect()
             return {"status": "FAILED", "error": "No columns found"}
        
        processed_columns = 0
        profile_results = []
        
        # 4. Profile Each Column
        for col in columns:
            col_name = col['COLUMN_NAME']
            data_type = col['DATA_TYPE'].upper()
            
            # Determine type category
            is_numeric = any(x in data_type for x in ['NUMBER', 'INT', 'FLOAT', 'DECIMAL', 'DOUBLE', 'REAL'])
            is_string = any(x in data_type for x in ['VARCHAR', 'CHAR', 'TEXT', 'STRING'])
            is_date = any(x in data_type for x in ['DATE', 'TIME', 'TIMESTAMP'])
            
            # Build Profile Query
            if is_numeric:
                sql = f"""
                    SELECT COUNT(*) as TOTAL, COUNT(*) - COUNT("{col_name}") as NULLS,
                           COUNT(DISTINCT "{col_name}") as DISTINCTS,
                           MIN("{col_name}")::VARCHAR as MIN_VAL, MAX("{col_name}")::VARCHAR as MAX_VAL,
                           AVG("{col_name}") as AVG_VAL, STDDEV("{col_name}") as STDDEV_VAL,
                           NULL as MIN_LEN, NULL as MAX_LEN, NULL as AVG_LEN, NULL as FUTURES
                    FROM {p_database}.{p_schema}."{p_table}"
                """
            elif is_string:
                sql = f"""
                    SELECT COUNT(*) as TOTAL, COUNT(*) - COUNT("{col_name}") as NULLS,
                           COUNT(DISTINCT "{col_name}") as DISTINCTS,
                           MIN("{col_name}")::VARCHAR as MIN_VAL, MAX("{col_name}")::VARCHAR as MAX_VAL,
                           NULL as AVG_VAL, NULL as STDDEV_VAL,
                           MIN(LENGTH("{col_name}")) as MIN_LEN, MAX(LENGTH("{col_name}")) as MAX_LEN,
                           AVG(LENGTH("{col_name}")) as AVG_LEN, NULL as FUTURES
                    FROM {p_database}.{p_schema}."{p_table}"
                """
            elif is_date:
                sql = f"""
                    SELECT COUNT(*) as TOTAL, COUNT(*) - COUNT("{col_name}") as NULLS,
                           COUNT(DISTINCT "{col_name}") as DISTINCTS,
                           MIN("{col_name}")::VARCHAR as MIN_VAL, MAX("{col_name}")::VARCHAR as MAX_VAL,
                           NULL as AVG_VAL, NULL as STDDEV_VAL, NULL as MIN_LEN, NULL as MAX_LEN, NULL as AVG_LEN,
                           SUM(CASE WHEN "{col_name}" > CURRENT_TIMESTAMP() THEN 1 ELSE 0 END) as FUTURES
                    FROM {p_database}.{p_schema}."{p_table}"
                """
            else:
                 sql = f"""
                    SELECT COUNT(*) as TOTAL, COUNT(*) - COUNT("{col_name}") as NULLS,
                           COUNT(DISTINCT "{col_name}") as DISTINCTS,
                           NULL as MIN_VAL, NULL as MAX_VAL, NULL as AVG_VAL, NULL as STDDEV_VAL,
                           NULL as MIN_LEN, NULL as MAX_LEN, NULL as AVG_LEN, NULL as FUTURES
                    FROM {p_database}.{p_schema}."{p_table}"
                """
            
            try:
                row = session.sql(sql).collect()[0]
                
                # Insert Profile
                insert_sql = f"""
                    INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_COLUMN_PROFILE (
                        RUN_ID, DATASET_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
                        COLUMN_NAME, DATA_TYPE, TOTAL_RECORDS, NULL_COUNT, DISTINCT_COUNT,
                        MIN_VALUE, MAX_VALUE, AVG_VALUE, STDDEV_VALUE,
                        MIN_LENGTH, MAX_LENGTH, AVG_LENGTH, FUTURE_DATE_COUNT, PROFILE_TS
                    ) VALUES (
                        '{run_id}', '{p_dataset_id}', '{p_database.upper()}', '{p_schema.upper()}', '{p_table.upper()}',
                        '{col_name}', '{data_type}', {row['TOTAL'] or 0}, {row['NULLS'] or 0}, {row['DISTINCTS'] or 0},
                        {f"'{str(row['MIN_VAL'])[:255]}'" if row['MIN_VAL'] else 'NULL'}, 
                        {f"'{str(row['MAX_VAL'])[:255]}'" if row['MAX_VAL'] else 'NULL'},
                        {row['AVG_VAL'] or 'NULL'}, {row['STDDEV_VAL'] or 'NULL'},
                        {row['MIN_LEN'] or 'NULL'}, {row['MAX_LEN'] or 'NULL'}, {row['AVG_LEN'] or 'NULL'},
                        {row['FUTURES'] or 'NULL'}, CURRENT_TIMESTAMP()
                    )
                """
                session.sql(insert_sql).collect()
                
                profile_results.append({
                    "col": col_name, "total": row['TOTAL'] or 0, 
                    "nulls": row['NULLS'] or 0, "distincts": row['DISTINCTS'] or 0,
                    "futures": row['FUTURES'] or 0
                })
                processed_columns += 1
                
            except Exception as e:
                print(f"Error profiling {col_name}: {str(e)}")
        
        # 5. Generate Synthetic Checks
        passed, failed, warnings = 0, 0, 0
        
        for res in profile_results:
            total = res['total']
            if total == 0: continue
            
            null_pct = (res['nulls'] / total) * 100
            dist_pct = (res['distincts'] / total) * 100
            
            checks = []
            
            # Null Check
            if null_pct > 20:
                checks.append(("PROFILE_HIGH_NULLS", "COMPLETENESS", "FAILED", 100-null_pct, 80, res['nulls'], f"{null_pct:.1f}% nulls"))
                failed += 1
            elif null_pct > 5:
                checks.append(("PROFILE_NULL_CHECK", "COMPLETENESS", "WARNING", 100-null_pct, 95, res['nulls'], f"{null_pct:.1f}% nulls"))
                warnings += 1
            else:
                checks.append(("PROFILE_NULL_CHECK", "COMPLETENESS", "PASSED", 100-null_pct, 95, res['nulls'], None))
                passed += 1
                
            # Cardinality Check
            if dist_pct < 10 and res['distincts'] > 1 and total > 10:
                checks.append(("PROFILE_LOW_CARDINALITY", "UNIQUENESS", "WARNING", dist_pct, 10, total-res['distincts'], f"Only {res['distincts']} distinct values"))
                warnings += 1
                
            # Future Dates
            if res['futures'] > 0:
                checks.append(("PROFILE_FUTURE_DATES", "VALIDITY", "FAILED", ((total-res['futures'])/total)*100, 100, res['futures'], f"{res['futures']} future dates"))
                failed += 1
                
            for c in checks:
                rule_name, rule_type, status, pass_rate, threshold, invalid_recs, reason = c
                fail_reason = f"'{reason}'" if reason else "NULL"
                
                session.sql(f"""
                    INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS (
                        RUN_ID, DATASET_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
                        COLUMN_NAME, RULE_NAME, RULE_TYPE, CHECK_STATUS,
                        PASS_RATE, THRESHOLD, TOTAL_RECORDS, INVALID_RECORDS,
                        FAILURE_REASON, CREATED_TS
                    ) VALUES (
                        '{run_id}', '{p_dataset_id}', '{p_database.upper()}', '{p_schema.upper()}', '{p_table.upper()}',
                        '{res['col']}', '{rule_name}', '{rule_type}', '{status}',
                        {pass_rate}, {threshold}, {total}, {invalid_recs},
                        {fail_reason}, CURRENT_TIMESTAMP()
                    )
                """).collect()
        
        # 6. Completion
        final_status = 'COMPLETED_WITH_FAILURES' if failed > 0 else 'COMPLETED'
        total_checks = passed + failed + warnings
        duration = (datetime.now() - start_time).total_seconds()
        
        session.sql(f"""
            UPDATE DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
            SET RUN_STATUS = '{final_status}', END_TS = CURRENT_TIMESTAMP(),
                DURATION_SECONDS = {duration}, TOTAL_CHECKS = {total_checks},
                PASSED_CHECKS = {passed}, FAILED_CHECKS = {failed}, WARNING_CHECKS = {warnings}
            WHERE RUN_ID = '{run_id}'
        """).collect()
        
        return {"status": final_status, "run_id": run_id, "checks": total_checks}
        
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}
$$;

-- ============================================================================
-- PROCEDURE 2: CUSTOM CHECKS BATCH
-- ============================================================================
-- Purpose: Run custom checks for a dataset (with selective rule filtering)
-- Called by: Scheduler (with CUSTOM_CONFIG from SCAN_SCHEDULES)
-- ============================================================================

CREATE OR REPLACE PROCEDURE SP_RUN_CUSTOM_CHECKS_BATCH(
    P_DATASET_ID VARCHAR,
    P_DATABASE   VARCHAR,
    P_SCHEMA     VARCHAR,
    P_TABLE      VARCHAR,
    P_CUSTOM_CONFIG VARCHAR
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
import json
import uuid
from datetime import datetime

def main(session, p_dataset_id, p_database, p_schema, p_table, p_custom_config):
    if not p_dataset_id:
        return "Error: Missing dataset_id"
    
    # Parse Config
    selected_rule_ids = []
    try:
        if p_custom_config and p_custom_config != 'NULL':
            config_json = json.loads(p_custom_config)
            if 'customRules' in config_json and isinstance(config_json['customRules'], list):
                selected_rule_ids = config_json['customRules']
    except Exception as e:
        return f"Error parsing config: {str(e)}"
    
    # 1. Base Query
    base_query = f"""
        SELECT 
            rm.RULE_NAME,
            rm.RULE_ID,
            drc.COLUMN_NAME,
            drc.THRESHOLD_VALUE
        FROM DATA_QUALITY_DB.DQ_CONFIG.DATASET_RULE_CONFIG drc
        JOIN DATA_QUALITY_DB.DQ_CONFIG.RULE_MASTER rm 
          ON drc.RULE_ID = rm.RULE_ID
        WHERE drc.DATASET_ID = '{p_dataset_id}'
          AND drc.IS_ACTIVE = TRUE
          AND rm.IS_ACTIVE = TRUE
    """
    
    # 2. Apply Filtering if specific rules are selected
    if selected_rule_ids:
        ids_str = "', '".join(selected_rule_ids)
        base_query += f" AND rm.RULE_ID IN ('{ids_str}')"
        
    rules = session.sql(base_query).collect()
    
    # 3. Handle No Rules (Create Warning)
    if not rules:
        run_id = f"DQ_CUSTOM_BATCH_{datetime.now().strftime('%Y%m%d%H%M%S')}_{str(uuid.uuid4())[:8]}"
        
        msg = "No active custom rules configured"
        reason = "No active custom rules found. Please configure rules."
        if selected_rule_ids:
             msg = "Selected rules not found or inactive"
             reason = "The specific rules selected for this schedule are no longer active."
        
        session.sql(f"""
            INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL 
            (RUN_ID, TRIGGERED_BY, START_TS, END_TS, RUN_STATUS, TOTAL_CHECKS, ERROR_MESSAGE, CREATED_TS)
            VALUES 
            ('{run_id}', 'SCHEDULED', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'COMPLETED_WITH_FAILURES', 1, 
            '{msg}', CURRENT_TIMESTAMP())
        """).collect()
        
        session.sql(f"""
            INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS (
                RUN_ID, DATASET_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
                RULE_NAME, RULE_TYPE, CHECK_STATUS, TOTAL_RECORDS, PASS_RATE, THRESHOLD,
                FAILURE_REASON, CREATED_TS
            ) VALUES (
                '{run_id}', '{p_dataset_id}', '{p_database.upper()}', '{p_schema.upper()}', '{p_table.upper()}',
                'SCHEDULER_CONFIG_CHECK', 'CONFIGURATION', 'WARNING', 0, 0, 0,
                '{reason}', CURRENT_TIMESTAMP()
            )
        """).collect()
        
        return f"Warning logged: {msg}"
        
    results = []
    
    # 4. Execute Each Rule
    for row in rules:
        rule_name = row['RULE_NAME']
        col_name = row['COLUMN_NAME']
        threshold = row['THRESHOLD_VALUE']
        
        col_arg = f"'{col_name}'" if col_name else "NULL"
        thresh_arg = str(threshold) if threshold is not None else "NULL"
        
        try:
            call_sql = f"""
                CALL DATA_QUALITY_DB.DQ_ENGINE.SP_RUN_CUSTOM_RULE(
                    '{p_dataset_id}', 
                    '{rule_name}', 
                    {col_arg}, 
                    {thresh_arg}, 
                    'SCHEDULED'
                )
            """
            session.sql(call_sql).collect()
            results.append(f"✓ Ran {rule_name}")
        except Exception as e:
            results.append(f"✗ Failed {rule_name}: {str(e)}")
            
    return "\\n".join(results)
$$;

-- ============================================================================
-- PROCEDURE 3: SCHEDULE PROCESSOR
-- ============================================================================
-- Purpose: Main scheduler - checks SCAN_SCHEDULES and executes due scans
-- Called by: DQ_SCHEDULE_PROCESSOR_TASK (every minute)
-- ============================================================================

CREATE OR REPLACE PROCEDURE SP_PROCESS_DUE_SCHEDULES()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import snowflake.snowpark as snowpark

def main(session):
    # 1. Fetch Due Active Schedules
    due_schedules = session.sql("""
        SELECT 
            SCHEDULE_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
            SCAN_TYPE, SCHEDULE_TYPE, IS_RECURRING, SCHEDULE_DAYS,
            CUSTOM_CONFIG
        FROM DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES
        WHERE STATUS = 'active'
          AND NEXT_RUN_AT <= CURRENT_TIMESTAMP()
        ORDER BY NEXT_RUN_AT ASC
        LIMIT 10
    """).collect()
    
    if not due_schedules:
        return "No schedules due."
        
    results = []
    
    for row in due_schedules:
        s_id = row['SCHEDULE_ID']
        db = row['DATABASE_NAME']
        sch = row['SCHEMA_NAME']
        tbl = row['TABLE_NAME']
        scan_type = row['SCAN_TYPE'].lower()
        is_rec = row['IS_RECURRING']
        sched_type = row['SCHEDULE_TYPE']
        
        # Pass the config string, defaulting to 'NULL' if empty
        custom_config = row['CUSTOM_CONFIG']
        custom_config_arg = f"'{custom_config}'" if custom_config else "'NULL'"
        
        is_recurring = True if is_rec else False
        
        try:
            # 2. RESOLVE DATASET_ID
            ds_row = session.sql(f"""
                SELECT DATASET_ID, IS_ACTIVE 
                FROM DATA_QUALITY_DB.DQ_CONFIG.DATASET_CONFIG
                WHERE SOURCE_DATABASE = '{db.upper()}'
                  AND SOURCE_SCHEMA = '{sch.upper()}'
                  AND SOURCE_TABLE = '{tbl.upper()}'
                LIMIT 1
            """).collect()
            
            if not ds_row:
                raise Exception(f"Dataset not found for {db}.{sch}.{tbl}")
                
            if not ds_row[0]['IS_ACTIVE']:
                raise Exception(f"Dataset {db}.{sch}.{tbl} is inactive")
                
            dataset_id = ds_row[0]['DATASET_ID']
            executed_actions = []
            
            # 3. EXECUTE SCANS
            if scan_type in ('profiling', 'full', 'anomalies'):
                session.sql(f"CALL DATA_QUALITY_DB.DQ_METRICS.SP_RUN_DATA_PROFILING('{dataset_id}', '{db}', '{sch}', '{tbl}')").collect()
                executed_actions.append("Profiling")
            
            if scan_type in ('checks', 'custom', 'full'):
                session.sql(f"CALL DATA_QUALITY_DB.DQ_METRICS.SP_RUN_CUSTOM_CHECKS_BATCH('{dataset_id}', '{db}', '{sch}', '{tbl}', {custom_config_arg})").collect()
                executed_actions.append("Custom Checks")
            
            results.append(f"Success {s_id}: Ran {', '.join(executed_actions)}")
            
            # 4. RESCHEDULE
            next_run_expr = "NULL"
            if is_recurring:
                if sched_type == 'hourly':
                    next_run_expr = "DATEADD(hour, 1, CURRENT_TIMESTAMP())"
                elif sched_type == 'daily':
                    next_run_expr = "DATEADD(day, 1, CURRENT_TIMESTAMP())"
                elif sched_type == 'weekly':
                    next_run_expr = "DATEADD(week, 1, CURRENT_TIMESTAMP())"
                else: 
                     next_run_expr = "DATEADD(day, 1, CURRENT_TIMESTAMP())" 
            
            update_sql = f"""
                UPDATE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES
                SET LAST_RUN_AT = CURRENT_TIMESTAMP(),
                    NEXT_RUN_AT = {next_run_expr},
                    UPDATED_AT = CURRENT_TIMESTAMP(),
                    FAILURE_COUNT = 0
                WHERE SCHEDULE_ID = '{s_id}'
            """
            session.sql(update_sql).collect()
            
        except Exception as e:
            err_msg = str(e).replace("'", "")[:255]
            results.append(f"Failed {s_id}: {err_msg}")
            
            session.sql(f"""
                UPDATE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES
                SET FAILURE_COUNT = IFNULL(FAILURE_COUNT, 0) + 1,
                    UPDATED_AT = CURRENT_TIMESTAMP(),
                    NEXT_RUN_AT = DATEADD(hour, 1, CURRENT_TIMESTAMP()) 
                WHERE SCHEDULE_ID = '{s_id}'
            """).collect()
    
    return "\\n".join(results)
$$;

-- ============================================================================
-- SNOWFLAKE TASK: SCHEDULE PROCESSOR
-- ============================================================================
-- Purpose: Runs every minute to check for due schedules
-- ============================================================================

CREATE OR REPLACE TASK DQ_SCHEDULE_PROCESSOR_TASK
    WAREHOUSE = DQ_ANALYTICS_WH
    SCHEDULE = '1 MINUTE'
    COMMENT = 'Runs every minute to process due schedules from SCAN_SCHEDULES table'
AS
    CALL DATA_QUALITY_DB.DQ_METRICS.SP_PROCESS_DUE_SCHEDULES();

-- ============================================================================
-- ENABLE TASK
-- ============================================================================

ALTER TASK DQ_SCHEDULE_PROCESSOR_TASK RESUME;

-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================

GRANT USAGE ON PROCEDURE SP_RUN_DATA_PROFILING(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE ACCOUNTADMIN;
GRANT USAGE ON PROCEDURE SP_RUN_CUSTOM_CHECKS_BATCH(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE ACCOUNTADMIN;
GRANT USAGE ON PROCEDURE SP_PROCESS_DUE_SCHEDULES() TO ROLE ACCOUNTADMIN;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Verify procedures created
SHOW PROCEDURES LIKE 'SP_%';

-- Verify task created and running
SHOW TASKS LIKE 'DQ_SCHEDULE_PROCESSOR_TASK';

-- Check task status
-- SELECT 
--     NAME,
--     DATABASE_NAME,
--     SCHEMA_NAME,
--     STATE,
--     SCHEDULE,
--     WAREHOUSE,
--     CREATED_ON,
--     NEXT_SCHEDULED_TIME
-- FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
-- WHERE NAME = 'DQ_SCHEDULE_PROCESSOR_TASK'
-- ORDER BY SCHEDULED_TIME DESC
-- LIMIT 5;

-- ============================================================================
-- MANUAL TESTING
-- ============================================================================

-- Test profiling procedure
-- CALL DATA_QUALITY_DB.DQ_METRICS.SP_RUN_DATA_PROFILING('DS_BRONZE_CUSTOMER', 'BANKING_DW', 'BRONZE', 'STG_CUSTOMER');

-- Test custom checks batch
-- CALL DATA_QUALITY_DB.DQ_METRICS.SP_RUN_CUSTOM_CHECKS_BATCH('DS_BRONZE_CUSTOMER', 'BANKING_DW', 'BRONZE', 'STG_CUSTOMER', 'NULL');

-- Test schedule processor
-- CALL DATA_QUALITY_DB.DQ_METRICS.SP_PROCESS_DUE_SCHEDULES();

-- Manually trigger task once
-- EXECUTE TASK DQ_SCHEDULE_PROCESSOR_TASK;

-- ============================================================================
-- SCHEDULING SYSTEM SETUP COMPLETE
-- ============================================================================
-- Next Steps:
-- 1. Create schedules via API or directly in SCAN_SCHEDULES table
-- 2. Task will automatically process due schedules every minute
-- 3. Monitor execution: SELECT * FROM DQ_RUN_CONTROL ORDER BY START_TS DESC LIMIT 20;
-- 4. Check task history: SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) WHERE NAME = 'DQ_SCHEDULE_PROCESSOR_TASK';
-- ============================================================================
