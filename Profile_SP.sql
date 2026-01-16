
USE DATABASE DATA_QUALITY_DB;
USE SCHEMA DQ_ENGINE;
USE WAREHOUSE DQ_ANALYTICS_WH;

CREATE OR REPLACE PROCEDURE sp_run_table_profiling(
    p_dataset_id     VARCHAR,
    p_database_name  VARCHAR,
    p_schema_name    VARCHAR,
    p_table_name     VARCHAR,
    p_profile_level  VARCHAR DEFAULT 'BASIC'  -- BASIC | EXTENDED
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
from snowflake.snowpark import Session
from datetime import datetime
import uuid
import json

def main(session: Session,
         p_dataset_id: str,
         p_database_name: str,
         p_schema_name: str,
         p_table_name: str,
         p_profile_level: str):

    # ------------------------------------------------------------------
    # Validate inputs
    # ------------------------------------------------------------------
    if not p_dataset_id or not p_database_name or not p_schema_name or not p_table_name:
        raise Exception("dataset_id, database_name, schema_name and table_name are required")

    run_id = f"DQ_PROFILE_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    start_time = datetime.now()
    triggered_by = session.sql("SELECT CURRENT_USER()").collect()[0][0]

    # ------------------------------------------------------------------
    # 1. Insert RUN CONTROL
    # ------------------------------------------------------------------
    session.sql(f"""
        INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL (
            run_id,
            triggered_by,
            start_ts,
            run_status,
            total_checks,
            created_ts
        )
        VALUES (
            '{run_id}',
            '{triggered_by}',
            CURRENT_TIMESTAMP(),
            'RUNNING',
            0,
            CURRENT_TIMESTAMP()
        )
    """).collect()

    # ------------------------------------------------------------------
    # 2. Fetch columns
    # ------------------------------------------------------------------
    columns = session.sql(f"""
        SELECT
            COLUMN_NAME,
            DATA_TYPE
        FROM {p_database_name}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{p_schema_name}'
          AND TABLE_NAME   = '{p_table_name}'
        ORDER BY ORDINAL_POSITION
    """).collect()

    if not columns:
        raise Exception("No columns found for table")

    total_columns = 0

    # ------------------------------------------------------------------
    # 3. Profile each column
    # ------------------------------------------------------------------
    for col in columns:
        column_name = col['COLUMN_NAME']
        data_type = col['DATA_TYPE'].upper()
        total_columns += 1

        # ---------------- Numeric ----------------
        if any(t in data_type for t in ['NUMBER', 'INT', 'DECIMAL', 'FLOAT', 'DOUBLE']):
            sql = f"""
                SELECT
                    COUNT(*) AS total_records,
                    COUNT(*) - COUNT({column_name}) AS null_count,
                    COUNT(DISTINCT {column_name}) AS distinct_count,
                    MIN({column_name}) AS min_value,
                    MAX({column_name}) AS max_value,
                    AVG({column_name}) AS avg_value,
                    STDDEV({column_name}) AS stddev_value
                FROM {p_database_name}.{p_schema_name}.{p_table_name}
            """

            r = session.sql(sql).collect()[0]

            session.sql(f"""
                INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_COLUMN_PROFILE (
                    RUN_ID, DATASET_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
                    COLUMN_NAME, DATA_TYPE,
                    TOTAL_RECORDS, NULL_COUNT, DISTINCT_COUNT,
                    MIN_VALUE, MAX_VALUE, AVG_VALUE, STDDEV_VALUE
                )
                VALUES (
                    '{run_id}', '{p_dataset_id}', '{p_database_name}', '{p_schema_name}', '{p_table_name}',
                    '{column_name}', '{data_type}',
                    {r['TOTAL_RECORDS']}, {r['NULL_COUNT']}, {r['DISTINCT_COUNT']},
                    '{r['MIN_VALUE']}', '{r['MAX_VALUE']}',
                    {r['AVG_VALUE'] if r['AVG_VALUE'] is not None else 'NULL'},
                    {r['STDDEV_VALUE'] if r['STDDEV_VALUE'] is not None else 'NULL'}
                )
            """).collect()

        # ---------------- String ----------------
        elif 'CHAR' in data_type or 'TEXT' in data_type or 'STRING' in data_type:
            sql = f"""
                SELECT
                    COUNT(*) AS total_records,
                    COUNT(*) - COUNT({column_name}) AS null_count,
                    COUNT(DISTINCT {column_name}) AS distinct_count,
                    MIN(LENGTH({column_name})) AS min_length,
                    MAX(LENGTH({column_name})) AS max_length,
                    AVG(LENGTH({column_name})) AS avg_length
                FROM {p_database_name}.{p_schema_name}.{p_table_name}
            """

            r = session.sql(sql).collect()[0]

            session.sql(f"""
                INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_COLUMN_PROFILE (
                    RUN_ID, DATASET_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
                    COLUMN_NAME, DATA_TYPE,
                    TOTAL_RECORDS, NULL_COUNT, DISTINCT_COUNT,
                    MIN_LENGTH, MAX_LENGTH, AVG_LENGTH
                )
                VALUES (
                    '{run_id}', '{p_dataset_id}', '{p_database_name}', '{p_schema_name}', '{p_table_name}',
                    '{column_name}', '{data_type}',
                    {r['TOTAL_RECORDS']}, {r['NULL_COUNT']}, {r['DISTINCT_COUNT']},
                    {r['MIN_LENGTH'] if r['MIN_LENGTH'] is not None else 'NULL'},
                    {r['MAX_LENGTH'] if r['MAX_LENGTH'] is not None else 'NULL'},
                    {r['AVG_LENGTH'] if r['AVG_LENGTH'] is not None else 'NULL'}
                )
            """).collect()

        # ---------------- Date / Timestamp ----------------
        elif 'DATE' in data_type or 'TIME' in data_type:
            sql = f"""
                SELECT
                    COUNT(*) AS total_records,
                    COUNT(*) - COUNT({column_name}) AS null_count,
                    MIN({column_name}) AS min_value,
                    MAX({column_name}) AS max_value,
                    SUM(CASE WHEN {column_name} > CURRENT_TIMESTAMP() THEN 1 ELSE 0 END)
                        AS future_date_count
                FROM {p_database_name}.{p_schema_name}.{p_table_name}
            """

            r = session.sql(sql).collect()[0]

            session.sql(f"""
                INSERT INTO DATA_QUALITY_DB.DQ_METRICS.DQ_COLUMN_PROFILE (
                    RUN_ID, DATASET_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
                    COLUMN_NAME, DATA_TYPE,
                    TOTAL_RECORDS, NULL_COUNT,
                    MIN_VALUE, MAX_VALUE, FUTURE_DATE_COUNT
                )
                VALUES (
                    '{run_id}', '{p_dataset_id}', '{p_database_name}', '{p_schema_name}', '{p_table_name}',
                    '{column_name}', '{data_type}',
                    {r['TOTAL_RECORDS']}, {r['NULL_COUNT']},
                    '{r['MIN_VALUE']}', '{r['MAX_VALUE']}',
                    {r['FUTURE_DATE_COUNT']}
                )
            """).collect()

    # ------------------------------------------------------------------
    # 4. Finalize run
    # ------------------------------------------------------------------
    duration = (datetime.now() - start_time).total_seconds()

    session.sql(f"""
        UPDATE DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
        SET
            end_ts = CURRENT_TIMESTAMP(),
            duration_seconds = {duration},
            run_status = 'COMPLETED',
            total_checks = {total_columns}
        WHERE run_id = '{run_id}'
    """).collect()

    return {
        "run_id": run_id,
        "status": "COMPLETED",
        "profiled_columns": total_columns,
        "duration_seconds": round(duration, 2)
    }
$$;



CALL DATA_QUALITY_DB.DQ_ENGINE.sp_run_table_profiling(
  'DS_CUSTOMER',
  'BANKING_DW',
  'BRONZE',
  'STG_CUSTOMER',
  'BASIC'
);



SELECT * FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL WHERE RUN_ID='DQ_PROFILE_20260112_011522_465655';

SELECT * FROM DATA_QUALITY_DB.DQ_METRICS.DQ_COLUMN_PROFILE;