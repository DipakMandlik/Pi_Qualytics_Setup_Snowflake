import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool, executeQuery, ensureConnectionContext } from "@/lib/snowflake";

/**
 * GET /api/dq/table-checks
 * Returns check results and summary for a specific table
 * 
 * Query Parameters:
 * - database: Database name (required)
 * - schema: Schema name (required)
 * - table: Table name (required)
 * - days: Number of days to look back (default: 30)
 */
export async function GET(request: NextRequest) {
    try {
        const searchParams = request.nextUrl.searchParams;
        const database = searchParams.get("database");
        const schema = searchParams.get("schema");
        const table = searchParams.get("table");
        const days = parseInt(searchParams.get("days") || "30");

        if (!database || !schema || !table) {
            return NextResponse.json(
                {
                    success: false,
                    error: "Missing required parameters: database, schema, and table are required",
                },
                { status: 400 }
            );
        }

        const config = getServerConfig();
        if (!config) {
            return NextResponse.json(
                {
                    success: false,
                    error: "No Snowflake connection found. Please connect first.",
                },
                { status: 401 }
            );
        }

        const connection = await snowflakePool.getConnection(config);
        await ensureConnectionContext(connection, config);

        // Query to get checks for this specific table
        const checksQuery = `
            SELECT 
                CHECK_ID,
                RUN_ID,
                CHECK_TIMESTAMP,
                DATASET_ID,
                DATABASE_NAME,
                SCHEMA_NAME,
                TABLE_NAME,
                COLUMN_NAME,
                RULE_ID,
                RULE_NAME,
                RULE_TYPE,
                RULE_LEVEL,
                TOTAL_RECORDS,
                VALID_RECORDS,
                INVALID_RECORDS,
                NULL_RECORDS,
                PASS_RATE,
                THRESHOLD,
                CHECK_STATUS,
                EXECUTION_TIME_MS,
                FAILURE_REASON,
                SAMPLE_INVALID_VALUES,
                CREATED_TS
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
            WHERE DATABASE_NAME = '${database.toUpperCase()}'
                AND SCHEMA_NAME = '${schema.toUpperCase()}'
                AND TABLE_NAME = '${table.toUpperCase()}'
                AND CHECK_TIMESTAMP >= DATEADD(day, -${days}, CURRENT_TIMESTAMP())
            ORDER BY CHECK_TIMESTAMP DESC
            LIMIT 100
        `;

        // Query to get summary stats
        const summaryQuery = `
            SELECT 
                COUNT(*) AS TOTAL_CHECKS,
                SUM(CASE WHEN CHECK_STATUS = 'PASS' THEN 1 ELSE 0 END) AS PASSED_CHECKS,
                SUM(CASE WHEN CHECK_STATUS = 'FAIL' THEN 1 ELSE 0 END) AS FAILED_CHECKS,
                SUM(CASE WHEN CHECK_STATUS = 'WARNING' THEN 1 ELSE 0 END) AS WARNING_CHECKS,
                MAX(CHECK_TIMESTAMP) AS LAST_RUN_TIME
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
            WHERE DATABASE_NAME = '${database.toUpperCase()}'
                AND SCHEMA_NAME = '${schema.toUpperCase()}'
                AND TABLE_NAME = '${table.toUpperCase()}'
                AND CHECK_TIMESTAMP >= DATEADD(day, -${days}, CURRENT_TIMESTAMP())
        `;

        let checks: any[] = [];
        let summary = {
            totalChecks: 0,
            passedChecks: 0,
            failedChecks: 0,
            warningChecks: 0,
            lastRunTime: null as string | null,
            lastRunTimeFormatted: null as string | null,
        };

        // Get checks
        try {
            const checksResult = await executeQuery(connection, checksQuery);
            checks = checksResult.rows.map((row) => {
                const obj: any = {};
                checksResult.columns.forEach((col, idx) => {
                    obj[col] = row[idx];
                });
                return {
                    checkId: obj.CHECK_ID,
                    runId: obj.RUN_ID,
                    checkTimestamp: obj.CHECK_TIMESTAMP,
                    datasetId: obj.DATASET_ID,
                    databaseName: obj.DATABASE_NAME,
                    schemaName: obj.SCHEMA_NAME,
                    tableName: obj.TABLE_NAME,
                    columnName: obj.COLUMN_NAME,
                    ruleId: obj.RULE_ID,
                    ruleName: obj.RULE_NAME,
                    ruleType: obj.RULE_TYPE,
                    ruleLevel: obj.RULE_LEVEL || "Medium",
                    totalRecords: obj.TOTAL_RECORDS,
                    validRecords: obj.VALID_RECORDS,
                    invalidRecords: obj.INVALID_RECORDS,
                    nullRecords: obj.NULL_RECORDS,
                    passRate: obj.PASS_RATE,
                    threshold: obj.THRESHOLD,
                    checkStatus: obj.CHECK_STATUS,
                    executionTimeMs: obj.EXECUTION_TIME_MS,
                    failureReason: obj.FAILURE_REASON,
                    sampleInvalidValues: obj.SAMPLE_INVALID_VALUES,
                    createdTs: obj.CREATED_TS,
                    // Derive scope from column name
                    scope: obj.COLUMN_NAME ? "Column" : "Table",
                    target: obj.COLUMN_NAME || obj.TABLE_NAME,
                };
            });
        } catch (e: any) {
            console.log("Checks query error:", e.message);
        }

        // Get summary
        try {
            const summaryResult = await executeQuery(connection, summaryQuery);
            if (summaryResult.rows.length > 0) {
                const row = summaryResult.rows[0];
                const cols = summaryResult.columns;
                const getVal = (name: string) => row[cols.indexOf(name)];

                summary.totalChecks = getVal("TOTAL_CHECKS") || 0;
                summary.passedChecks = getVal("PASSED_CHECKS") || 0;
                summary.failedChecks = getVal("FAILED_CHECKS") || 0;
                summary.warningChecks = getVal("WARNING_CHECKS") || 0;

                const lastRun = getVal("LAST_RUN_TIME");
                if (lastRun) {
                    summary.lastRunTime = new Date(lastRun).toISOString();
                    summary.lastRunTimeFormatted = new Date(lastRun).toLocaleString("en-US", {
                        dateStyle: "medium",
                        timeStyle: "short",
                    });
                }
            }
        } catch (e: any) {
            console.log("Summary query error:", e.message);
        }

        // Group checks by latest run per rule (deduplicate)
        const latestByRule = new Map<string, any>();
        for (const check of checks) {
            const key = `${check.ruleName}_${check.columnName || "TABLE"}`;
            if (!latestByRule.has(key) || new Date(check.checkTimestamp) > new Date(latestByRule.get(key).checkTimestamp)) {
                latestByRule.set(key, check);
            }
        }
        const uniqueChecks = Array.from(latestByRule.values());

        return NextResponse.json({
            success: true,
            data: {
                summary,
                checks: uniqueChecks,
                allChecks: checks,
            },
        });
    } catch (error: any) {
        console.error("Error fetching table checks:", error);
        return NextResponse.json(
            {
                success: false,
                error: error.message || "Failed to fetch table checks",
            },
            { status: 500 }
        );
    }
}
