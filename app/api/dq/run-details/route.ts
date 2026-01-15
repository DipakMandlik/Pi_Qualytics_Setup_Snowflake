import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

// GET /api/dq/run-details?run_id=DQ_CUSTOM_20260108_...
// Returns run summary and check-level results for a specific run
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const runId = searchParams.get("run_id");
    const dqDatabase = (searchParams.get("dqDatabase") || "DATA_QUALITY_DB").toUpperCase();
    const dqMetricsSchema = (searchParams.get("dqMetricsSchema") || "DQ_METRICS").toUpperCase();

    if (!runId) {
      return NextResponse.json(
        { success: false, error: "Missing required parameter: run_id" },
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

    // A) Run Summary
    const summarySQL = `
      SELECT
        run_id,
        run_status,
        start_ts,
        end_ts,
        duration_seconds,
        total_checks,
        passed_checks,
        failed_checks,
        warning_checks,
        skipped_checks,
        triggered_by
      FROM ${dqDatabase}.${dqMetricsSchema}.DQ_RUN_CONTROL
      WHERE run_id = ?
    `;

    const summaryResult = await new Promise<any[]>((resolve, reject) => {
      conn.execute({
        sqlText: summarySQL,
        binds: [runId],
        complete: (err: any, _stmt: any, rows: any) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      });
    });

    if (summaryResult.length === 0) {
      return NextResponse.json(
        { success: false, error: `No run found with run_id = ${runId}` },
        { status: 404 }
      );
    }

    // B) Check-Level Results
    const checksSQL = `
      SELECT
        rule_name,
        rule_type,
        column_name,
        check_status,
        pass_rate,
        threshold,
        total_records,
        invalid_records,
        failure_reason,
        database_name,
        schema_name,
        table_name,
        dataset_id
      FROM ${dqDatabase}.${dqMetricsSchema}.DQ_CHECK_RESULTS
      WHERE run_id = ?
      ORDER BY check_status DESC, rule_type
    `;

    const checksResult = await new Promise<any[]>((resolve, reject) => {
      conn.execute({
        sqlText: checksSQL,
        binds: [runId],
        complete: (err: any, _stmt: any, rows: any) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      });
    });

    // Transform to camelCase
    const summary = summaryResult[0];
    const runSummary = {
      run_id: summary.RUN_ID,
      run_status: summary.RUN_STATUS,
      start_ts: summary.START_TS,
      end_ts: summary.END_TS,
      duration_seconds: summary.DURATION_SECONDS,
      total_checks: summary.TOTAL_CHECKS,
      passed_checks: summary.PASSED_CHECKS,
      failed_checks: summary.FAILED_CHECKS,
      warning_checks: summary.WARNING_CHECKS,
      skipped_checks: summary.SKIPPED_CHECKS,
      triggered_by: summary.TRIGGERED_BY,
      run_type: runId.startsWith("DQ_CUSTOM") ? "CUSTOM_SCAN" : "FULL_SCAN",
    };

    const checks = checksResult.map((row: any) => ({
      rule_name: row.RULE_NAME,
      rule_type: row.RULE_TYPE,
      column_name: row.COLUMN_NAME,
      check_status: row.CHECK_STATUS,
      pass_rate: row.PASS_RATE,
      threshold: row.THRESHOLD,
      total_records: row.TOTAL_RECORDS,
      invalid_records: row.INVALID_RECORDS,
      failure_reason: row.FAILURE_REASON,
      database_name: row.DATABASE_NAME,
      schema_name: row.SCHEMA_NAME,
      table_name: row.TABLE_NAME,
      dataset_id: row.DATASET_ID,
    }));

    return NextResponse.json({
      success: true,
      data: {
        summary: runSummary,
        checks,
      },
    });
  } catch (error: any) {
    console.error("GET /api/dq/run-details error:", error);
    return NextResponse.json(
      { success: false, error: error.message || "Failed to fetch run details" },
      { status: 500 }
    );
  }
}
