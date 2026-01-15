import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

// GET /api/dq/rules?dqDatabase=...&dqSchema=...
// Returns all available active rules: [{ rule_id, rule_name, rule_type, rule_level, description }]
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const dqDatabase = searchParams.get("dqDatabase") || "DATA_QUALITY_DB";
    const dqSchema = searchParams.get("dqSchema") || "DQ_CONFIG";

    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        { success: false, error: "No Snowflake connection found. Please connect first." },
        { status: 401 }
      );
    }

    const conn = await snowflakePool.getConnection(config);

    // Query fetches all available rules from RULE_MASTER (dynamic database/schema)
    const sql = `
      SELECT
        r.RULE_ID,
        r.RULE_NAME,
        r.RULE_TYPE,
        r.RULE_LEVEL,
        r.DESCRIPTION
      FROM ${dqDatabase.toUpperCase()}.${dqSchema.toUpperCase()}.RULE_MASTER r
      WHERE r.IS_ACTIVE = TRUE
      ORDER BY
        r.RULE_LEVEL,
        r.RULE_TYPE,
        r.RULE_NAME
    `;

    const rows = await new Promise<any[]>((resolve, reject) => {
      conn.execute({
        sqlText: sql,
        complete: (err: any, _stmt: any, rows: any[]) => {
          if (err) reject(err);
          else resolve(rows);
        },
      });
    });

    const data = rows.map((r) => ({
      rule_id: r["RULE_ID"],
      rule_name: r["RULE_NAME"],
      rule_type: r["RULE_TYPE"],
      rule_level: r["RULE_LEVEL"],
      description: r["DESCRIPTION"],
    }));

    return NextResponse.json({ success: true, data });
  } catch (error: any) {
    console.error("GET /api/dq/rules error:", error);
    return NextResponse.json(
      { success: false, error: error.message || "Failed to fetch rules" },
      { status: 500 }
    );
  }
}
