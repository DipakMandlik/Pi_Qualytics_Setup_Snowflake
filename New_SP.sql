CREATE OR REPLACE PROCEDURE DATA_QUALITY_DB.DQ_ENGINE.sp_execute_custom_dq_scan(
    p_dataset_id VARCHAR,
    p_database_name VARCHAR,
    p_schema_name VARCHAR,
    p_table_name VARCHAR,
    p_rule_ids ARRAY
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
EXECUTE AS CALLER
AS
$$
import uuid
from datetime import datetime
import json

def main(session,
         p_dataset_id,
         p_database_name,
         p_schema_name,
         p_table_name,
         p_rule_ids):

    run_id = f"DQ_CUSTOM_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    start_time = datetime.now()
    user = session.sql("SELECT CURRENT_USER()").collect()[0][0]

    # ------------------------------------------------------------
    # 1. Create Run Control Entry
    # ------------------------------------------------------------
    session.sql(f"""
        INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL (
            run_id,
            triggered_by,
            start_ts,
            run_status,
            total_checks,
            passed_checks,
            failed_checks,
            warning_checks,
            skipped_checks,
            created_ts
        )
        VALUES (
            '{run_id}',
            '{user}',
            CURRENT_TIMESTAMP(),
            'RUNNING',
            0, 0, 0, 0, 0,
            CURRENT_TIMESTAMP()
        )
    """).collect()

    stats = {
        "total": 0,
        "passed": 0,
        "failed": 0,
        "warning": 0,
        "skipped": 0
    }

    # ------------------------------------------------------------
    # 2. Fetch Rule Definitions
    # ------------------------------------------------------------
    rule_ids_str = ",".join([str(r) for r in p_rule_ids])

    rules = session.sql(f"""
        SELECT
            rm.rule_id,
            rm.rule_name,
            rm.rule_type,
            rm.rule_level,
            drc.column_name,
            drc.threshold_value,
            rst.sql_template
        FROM DATA_QUALITY_DB.DQ_CONFIG.rule_master rm
        JOIN DATA_QUALITY_DB.DQ_CONFIG.dataset_rule_config drc
            ON rm.rule_id = drc.rule_id
        JOIN DATA_QUALITY_DB.DQ_CONFIG.rule_sql_template rst
            ON rm.rule_id = rst.rule_id
        WHERE rm.rule_id IN ({rule_ids_str})
          AND drc.dataset_id = '{p_dataset_id}'
          AND rm.is_active = TRUE
          AND drc.is_active = TRUE
          AND rst.is_active = TRUE
    """).collect()

    if not rules:
        raise Exception("No valid rules found for execution")

    # ------------------------------------------------------------
    # 3. Execute Each Rule
    # ------------------------------------------------------------
    for rule in rules:
        stats["total"] += 1
        check_start = datetime.now()

        column = rule["COLUMN_NAME"]
        threshold = float(rule["THRESHOLD_VALUE"])
        sql = rule["SQL_TEMPLATE"]

        sql = (
            sql.replace("{{DATABASE}}", p_database_name)
               .replace("{{SCHEMA}}", p_schema_name)
               .replace("{{TABLE}}", p_table_name)
        )

        if column:
            sql = sql.replace("{{COLUMN}}", column)

        result = session.sql(sql).collect()[0]

        total_records = int(result["TOTAL_COUNT"])
        invalid_records = int(result["ERROR_COUNT"])
        valid_records = total_records - invalid_records

        pass_rate = round(
            (valid_records / total_records) * 100, 2
        ) if total_records > 0 else 100.0

        if pass_rate >= threshold:
            status = "PASSED"
            stats["passed"] += 1
        elif pass_rate >= threshold - 5:
            status = "WARNING"
            stats["warning"] += 1
        else:
            status = "FAILED"
            stats["failed"] += 1

        exec_time_ms = (datetime.now() - check_start).total_seconds() * 1000

        # --------------------------------------------------------
        # 4. Insert Check Result
        # --------------------------------------------------------
        session.sql(f"""
            INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS (
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
                pass_rate,
                threshold,
                check_status,
                execution_time_ms,
                created_ts
            )
            VALUES (
                '{run_id}',
                CURRENT_TIMESTAMP(),
                '{p_dataset_id}',
                '{p_database_name}',
                '{p_schema_name}',
                '{p_table_name}',
                {f"'{column}'" if column else "NULL"},
                {rule["RULE_ID"]},
                '{rule["RULE_NAME"]}',
                '{rule["RULE_TYPE"]}',
                '{rule["RULE_LEVEL"]}',
                {total_records},
                {valid_records},
                {invalid_records},
                {pass_rate},
                {threshold},
                '{status}',
                {exec_time_ms},
                CURRENT_TIMESTAMP()
            )
        """).collect()

    # ------------------------------------------------------------
    # 5. Finalize Run
    # ------------------------------------------------------------
    session.sql(f"""
        UPDATE DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
        SET
            end_ts = CURRENT_TIMESTAMP(),
            run_status = 'COMPLETED',
            total_checks = {stats["total"]},
            passed_checks = {stats["passed"]},
            failed_checks = {stats["failed"]},
            warning_checks = {stats["warning"]},
            skipped_checks = {stats["skipped"]}
        WHERE run_id = '{run_id}'
    """).collect()

    return {
        "run_id": run_id,
        "status": "COMPLETED",
        "executed_rules": len(rules),
        "stats": stats
    }
$$;


CALL DATA_QUALITY_DB.DQ_ENGINE.sp_execute_custom_dq_scan(
    'STG_ACCOUNT',
    'BANKING_DW',
    'BRONZE',
    'STG_ACCOUNT',
    ARRAY_CONSTRUCT(1, 2, 3)
);
