import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

// GET /api/dq/activity?database=BANKING_DW&schema=BRONZE&table=STG_CUSTOMER
// Returns recent DQ runs (custom scans, full scans) for the given table
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const database = (searchParams.get("database") || "").toUpperCase();
    const schema = (searchParams.get("schema") || "").toUpperCase();
    const table = (searchParams.get("table") || "").toUpperCase();
    const dqDatabase = (searchParams.get("dqDatabase") || "DATA_QUALITY_DB").toUpperCase();
    const dqMetricsSchema = (searchParams.get("dqMetricsSchema") || "DQ_METRICS").toUpperCase();
    const limit = parseInt(searchParams.get("limit") || "20", 10);

    if (!database || !schema || !table) {
      return NextResponse.json(
        { success: false, error: "Missing required parameters: database, schema, table" },
        { status: 400 }
      );
    }

    let conn: any;
    try {
      const config = getServerConfig();
      conn = await snowflakePool.getConnection(config || undefined);
    } catch (e: any) {
      return NextResponse.json(
        { success: false, error: `Unable to establish Snowflake connection: ${e?.message || e}` },
        { status: 401 }
      );
    }

    const sql = `
      WITH scan_runs AS (
        SELECT
          rc.run_id,
          CASE
            WHEN rc.run_id LIKE 'DQ_PROFILE%' THEN 'PROFILING'
            WHEN rc.run_id LIKE 'DQ_CUSTOM%' THEN 'CUSTOM_SCAN'
            ELSE 'FULL_SCAN'
          END AS run_type,
          rc.triggered_by,
          rc.start_ts,
          rc.end_ts,
          rc.duration_seconds,
          rc.run_status,
          rc.total_checks,
          rc.failed_checks,
          rc.warning_checks,
          rc.skipped_checks,
          MIN(cr.database_name) AS database_name,
          MIN(cr.schema_name)   AS schema_name,
          MIN(cr.table_name)    AS table_name
        FROM ${dqDatabase}.${dqMetricsSchema}.DQ_RUN_CONTROL rc
        JOIN ${dqDatabase}.${dqMetricsSchema}.DQ_CHECK_RESULTS cr
          ON rc.run_id = cr.run_id
        WHERE UPPER(cr.database_name) = ?
          AND UPPER(cr.schema_name)   = ?
          AND UPPER(cr.table_name)    = ?
        GROUP BY
          rc.run_id,
          rc.triggered_by,
          rc.start_ts,
          rc.end_ts,
          rc.duration_seconds,
          rc.run_status,
          rc.total_checks,
          rc.failed_checks,
          rc.warning_checks,
          rc.skipped_checks
      ),
      profile_runs AS (
        SELECT
          rc.run_id,
          'PROFILING' AS run_type,
          rc.triggered_by,
          rc.start_ts,
          rc.end_ts,
          rc.duration_seconds,
          rc.run_status,
          rc.total_checks,
          rc.failed_checks,
          rc.warning_checks,
          NULL AS skipped_checks,
          MIN(cp.database_name) AS database_name,
          MIN(cp.schema_name)   AS schema_name,
          MIN(cp.table_name)    AS table_name
        FROM ${dqDatabase}.${dqMetricsSchema}.DQ_RUN_CONTROL rc
        JOIN ${dqDatabase}.${dqMetricsSchema}.DQ_COLUMN_PROFILE cp
          ON rc.run_id = cp.run_id
        WHERE rc.run_id LIKE 'DQ_PROFILE%'
          AND rc.run_id NOT IN (SELECT run_id FROM scan_runs)
          AND UPPER(cp.database_name) = ?
          AND UPPER(cp.schema_name)   = ?
          AND UPPER(cp.table_name)    = ?
        GROUP BY
          rc.run_id,
          rc.triggered_by,
          rc.start_ts,
          rc.end_ts,
          rc.duration_seconds,
          rc.run_status,
          rc.total_checks,
          rc.failed_checks,
          rc.warning_checks
      )
      SELECT * FROM scan_runs
      UNION ALL
      SELECT * FROM profile_runs
      ORDER BY start_ts DESC
      LIMIT ?
    `;

    const result = await new Promise<any[]>((resolve, reject) => {
      conn.execute({
        sqlText: sql,
        binds: [database, schema, table, database, schema, table, limit],
        complete: (err: any, _stmt: any, rows: any) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      });
    });

    // Transform the data to camelCase for frontend consistency
    const data = result.map((row: any) => ({
      run_id: row.RUN_ID,
      run_type: row.RUN_TYPE,
      triggered_by: row.TRIGGERED_BY,
      start_ts: row.START_TS,
      end_ts: row.END_TS,
      duration_seconds: row.DURATION_SECONDS,
      run_status: row.RUN_STATUS,
      total_checks: row.TOTAL_CHECKS,
      failed_checks: row.FAILED_CHECKS,
      warning_checks: row.WARNING_CHECKS,
      skipped_checks: row.SKIPPED_CHECKS,
      database_name: row.DATABASE_NAME,
      schema_name: row.SCHEMA_NAME,
      table_name: row.TABLE_NAME,
    }));

    return NextResponse.json({ success: true, data });
  } catch (error: any) {
    console.error("GET /api/dq/activity error:", error);
    return NextResponse.json(
      { success: false, error: error.message || "Failed to fetch activity" },
      { status: 500 }
    );
  }
}
