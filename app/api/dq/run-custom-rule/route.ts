import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

// POST /api/dq/run-custom-rule
// Body: { dataset_id: string, rule_name: string, column_name?: string | null, threshold?: number | null, run_mode?: string }
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const searchParams = request.nextUrl.searchParams;
    const dqDatabase = (searchParams.get("dqDatabase") || "DATA_QUALITY_DB").toUpperCase();
    const dqEngineSchema = (searchParams.get("dqEngineSchema") || "DQ_ENGINE").toUpperCase();
    const { dataset_id, rule_name, column_name = null, threshold = null, run_mode = 'ADHOC' } = body || {};

    if (!dataset_id || !rule_name) {
      return NextResponse.json(
        { success: false, error: "Missing required payload: dataset_id, rule_name" },
        { status: 400 }
      );
    }

    // Try server-stored config first, then fall back to environment variables
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

    // Call stored procedure with fully-qualified path: <DQ_DB>.<DQ_ENGINE_SCHEMA>.sp_run_custom_rule(...)
    const sql = `CALL ${dqDatabase}.${dqEngineSchema}.sp_run_custom_rule(?, ?, ?, ?, ?)`;
    const binds = [dataset_id, rule_name, column_name, threshold, run_mode];

    const result = await new Promise<any>((resolve, reject) => {
      conn.execute({
        sqlText: sql,
        binds,
        complete: (err: any, _stmt: any, rows: any) => {
          if (err) reject(err);
          else resolve(rows);
        },
      });
    });

    console.log("SP result:", JSON.stringify(result, null, 2));

    return NextResponse.json({ success: true, data: result });
  } catch (error: any) {
    console.error("POST /api/dq/run-custom-rule error:", error);
    return NextResponse.json(
      { success: false, error: error.message || "Failed to run custom rule" },
      { status: 500 }
    );
  }
}
