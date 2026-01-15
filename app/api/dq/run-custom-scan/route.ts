import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

// POST /api/dq/run-custom-scan
// Body: { dataset_id, database, schema, table, rule_names[], column }
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { dataset_id, database, schema, table, rule_names, column } = body || {};

    if (!dataset_id || !Array.isArray(rule_names) || rule_names.length === 0) {
      return NextResponse.json(
        { success: false, error: "Missing or invalid payload. Expect dataset_id and rule_names[]" },
        { status: 400 }
      );
    }

    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        { success: false, error: "No Snowflake connection found. Please connect first." },
        { status: 401 }
      );
    }

    const conn = await snowflakePool.getConnection(config);
    const results = [];
    const errors = [];

    // Resolve true DATASET_ID from config if database/schema/table are provided
    // The frontend sends fully qualified name, but config might use synthetic IDs (e.g. DS_CUSTOMER)
    let resolvedDatasetId = dataset_id;
    if (database && schema && table) {
      try {
        const idRows = await new Promise<any[]>((resolve, reject) => {
          conn.execute({
            sqlText: `
              SELECT DATASET_ID 
              FROM DATA_QUALITY_DB.DQ_CONFIG.DATASET_CONFIG 
              WHERE UPPER(SOURCE_DATABASE) = UPPER(?) 
                AND UPPER(SOURCE_SCHEMA) = UPPER(?) 
                AND UPPER(SOURCE_TABLE) = UPPER(?)
              LIMIT 1
            `,
            binds: [database, schema, table],
            complete: (err: any, _stmt: any, rows: any) => {
              if (err) resolve([]);
              else resolve(rows);
            },
          });
        });

        if (idRows && idRows.length > 0) {
          resolvedDatasetId = idRows[0].DATASET_ID;
          console.log(`Resolved dataset_id from '${dataset_id}' to '${resolvedDatasetId}'`);
        } else {
          console.warn(`No config found for ${database}.${schema}.${table}, using default ID: ${dataset_id}`);
        }
      } catch (lookupErr) {
        console.error("Error looking up dataset_id:", lookupErr);
      }
    }

    // Iterate through rules and call sp_run_custom_rule for each
    for (const rule_name of rule_names) {
      try {
        console.log(`Executing rule: ${rule_name} on column: ${column} for dataset: ${resolvedDatasetId}`);

        // Params: dataset_id, rule_name, column_name, threshold, run_mode
        // Note: binds array needs to match the ? count. 
        // Logic check: sp_run_custom_rule(?, ?, ?, ?, ?) has 5 params.

        // Use default threshold 100 to force adhoc execution without needing active rule mapping in DB
        // 100 usually implies "Require 100% pass", which is a safe default for ad-hoc checks.
        const ruleBinds = [resolvedDatasetId, rule_name, column, 100, 'ADHOC'];
        const ruleSql = `CALL DATA_QUALITY_DB.DQ_ENGINE.sp_run_custom_rule(?, ?, ?, ?, ?)`;

        const row = await new Promise<any>((resolve, reject) => {
          conn.execute({
            sqlText: ruleSql,
            binds: ruleBinds,
            complete: (err: any, _stmt: any, rows: any) => {
              if (err) reject(err);
              else resolve(rows);
            },
          });
        });

        // Attempt to fetch full details from DB if run_id is present
        let dbRows = null;
        try {
          const spResult = row?.[0];
          let runId = null;

          // Extract run_id from SP output (usually a JSON string in the first column)
          if (spResult) {
            const values = Object.values(spResult);
            for (const val of values) {
              if (typeof val === 'string' && val.includes('run_id')) {
                try {
                  const parsed = JSON.parse(val);
                  if (parsed.run_id) {
                    runId = parsed.run_id;
                    break;
                  }
                } catch (e) { /* ignore */ }
              }
            }
          }

          if (runId) {
            console.log(`Fetching details for run_id: ${runId}`);
            dbRows = await new Promise<any>((resolve, reject) => {
              conn.execute({
                sqlText: `SELECT * FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS WHERE RUN_ID = ?`,
                binds: [runId],
                complete: (err: any, _stmt: any, rows: any) => {
                  if (err) resolve(null); // Resolve null on error to fallback
                  else resolve(rows);
                },
              });
            });
          }
        } catch (dbErr) {
          console.error("Error fetching run details from DB:", dbErr);
        }

        // Use DB rows if found, otherwise fallback to SP output
        results.push({ rule: rule_name, success: true, daa: dbRows && dbRows.length > 0 ? dbRows : row });
      } catch (err: any) {
        console.error(`Error running rule ${rule_name}:`, err);
        errors.push({ rule: rule_name, error: err.message });
      }
    }

    return NextResponse.json({
      success: errors.length === 0,
      data: { results, errors },
      message: `Executed ${results.length} rules, ${errors.length} failed.`
    });

  } catch (error: any) {
    console.error("POST /api/dq/run-custom-scan error:", error);
    return NextResponse.json(
      { success: false, error: error.message || "Failed to start custom scan" },
      { status: 500 }
    );
  }
}
