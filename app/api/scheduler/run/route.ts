import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool, executeQuery, ensureConnectionContext } from "@/lib/snowflake";

/**
 * GET /api/scheduler/run
 * Check for due schedules and execute them by calling actual scan APIs
 */
export async function GET(request: NextRequest) {
    try {
        const config = getServerConfig();
        if (!config) {
            return NextResponse.json(
                { success: false, error: "No Snowflake connection" },
                { status: 401 }
            );
        }

        const connection = await snowflakePool.getConnection(config);
        await ensureConnectionContext(connection, config);

        // Get all active schedules that are due (NEXT_RUN_AT <= NOW)
        const dueSchedulesQuery = `
            SELECT 
                SCHEDULE_ID,
                DATABASE_NAME,
                SCHEMA_NAME,
                TABLE_NAME,
                SCAN_TYPE,
                IS_RECURRING,
                SCHEDULE_TYPE,
                SCHEDULE_TIME,
                TIMEZONE
            FROM DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES
            WHERE STATUS = 'active'
                AND NEXT_RUN_AT <= CURRENT_TIMESTAMP()
            ORDER BY NEXT_RUN_AT ASC
            LIMIT 5
        `;

        let schedules: any[] = [];
        try {
            const result = await executeQuery(connection, dueSchedulesQuery);
            schedules = result.rows.map((row) => {
                const obj: any = {};
                result.columns.forEach((col, idx) => {
                    obj[col] = row[idx];
                });
                return obj;
            });
        } catch (e: any) {
            if (e.message?.includes("does not exist")) {
                return NextResponse.json({
                    success: true,
                    data: { executed: 0, message: "No schedules table yet" },
                });
            }
            throw e;
        }

        if (schedules.length === 0) {
            return NextResponse.json({
                success: true,
                data: { executed: 0, message: "No schedules due" },
            });
        }

        const executedRuns: any[] = [];

        // Get the host from the request to call internal APIs
        const host = request.headers.get("host") || "localhost:3000";
        const protocol = host.includes("localhost") ? "http" : "https";
        const baseUrl = `${protocol}://${host}`;

        for (const schedule of schedules) {
            try {
                console.log(`Executing scheduled ${schedule.SCAN_TYPE} scan for ${schedule.DATABASE_NAME}.${schedule.SCHEMA_NAME}.${schedule.TABLE_NAME}`);

                let apiResult: any = null;

                // Call the actual API endpoints based on scan type
                if (schedule.SCAN_TYPE === "profiling" || schedule.SCAN_TYPE === "full") {
                    // Call the real profiling API
                    const profilingResponse = await fetch(`${baseUrl}/api/dq/run-profiling`, {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({
                            database: schedule.DATABASE_NAME,
                            schema: schedule.SCHEMA_NAME,
                            table: schedule.TABLE_NAME,
                            profile_level: "BASIC",
                            triggered_by: "scheduled",
                        }),
                    });
                    apiResult = await profilingResponse.json();
                    console.log(`Profiling result for ${schedule.TABLE_NAME}:`, apiResult.success ? "Success" : apiResult.error);
                }

                if (schedule.SCAN_TYPE === "checks" || schedule.SCAN_TYPE === "full") {
                    // Call the checks/custom scan API
                    const checksResponse = await fetch(`${baseUrl}/api/dq/run-custom`, {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({
                            database: schedule.DATABASE_NAME,
                            schema: schedule.SCHEMA_NAME,
                            table: schedule.TABLE_NAME,
                            triggered_by: "scheduled",
                        }),
                    });
                    apiResult = await checksResponse.json();
                    console.log(`Checks result for ${schedule.TABLE_NAME}:`, apiResult.success ? "Success" : apiResult.error);
                }

                if (schedule.SCAN_TYPE === "anomalies") {
                    // For anomalies, we can also call profiling with anomaly detection
                    const anomalyResponse = await fetch(`${baseUrl}/api/dq/run-profiling`, {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({
                            database: schedule.DATABASE_NAME,
                            schema: schedule.SCHEMA_NAME,
                            table: schedule.TABLE_NAME,
                            profile_level: "BASIC",
                            triggered_by: "scheduled",
                        }),
                    });
                    apiResult = await anomalyResponse.json();
                    console.log(`Anomaly scan result for ${schedule.TABLE_NAME}:`, apiResult.success ? "Success" : apiResult.error);
                }

                // Calculate next run time based on schedule type
                let nextRunExpression = "NULL";
                if (schedule.IS_RECURRING) {
                    if (schedule.SCHEDULE_TYPE === "hourly") {
                        nextRunExpression = "DATEADD(hour, 1, CURRENT_TIMESTAMP())";
                    } else if (schedule.SCHEDULE_TYPE === "daily") {
                        nextRunExpression = "DATEADD(day, 1, CURRENT_TIMESTAMP())";
                    } else if (schedule.SCHEDULE_TYPE === "weekly") {
                        nextRunExpression = "DATEADD(week, 1, CURRENT_TIMESTAMP())";
                    }
                }

                // Update the schedule with LAST_RUN_AT and NEXT_RUN_AT
                const updateQuery = `
                    UPDATE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES
                    SET LAST_RUN_AT = CURRENT_TIMESTAMP(),
                        NEXT_RUN_AT = ${nextRunExpression},
                        FAILURE_COUNT = 0,
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE SCHEDULE_ID = '${schedule.SCHEDULE_ID}'
                `;
                await executeQuery(connection, updateQuery);

                executedRuns.push({
                    scheduleId: schedule.SCHEDULE_ID,
                    runId: apiResult?.data?.runId || `SCHEDULED_${Date.now()}`,
                    scanType: schedule.SCAN_TYPE,
                    table: `${schedule.DATABASE_NAME}.${schedule.SCHEMA_NAME}.${schedule.TABLE_NAME}`,
                    status: apiResult?.success ? "completed" : "failed",
                });

            } catch (scanError: any) {
                console.error(`Error executing schedule ${schedule.SCHEDULE_ID}:`, scanError.message);

                // Update failure count
                const failureUpdateQuery = `
                    UPDATE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES
                    SET FAILURE_COUNT = COALESCE(FAILURE_COUNT, 0) + 1,
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE SCHEDULE_ID = '${schedule.SCHEDULE_ID}'
                `;
                try {
                    await executeQuery(connection, failureUpdateQuery);
                } catch (e) { }

                executedRuns.push({
                    scheduleId: schedule.SCHEDULE_ID,
                    scanType: schedule.SCAN_TYPE,
                    table: `${schedule.DATABASE_NAME}.${schedule.SCHEMA_NAME}.${schedule.TABLE_NAME}`,
                    status: "failed",
                    error: scanError.message,
                });
            }
        }

        return NextResponse.json({
            success: true,
            data: {
                executed: executedRuns.length,
                runs: executedRuns,
            },
        });

    } catch (error: any) {
        console.error("Scheduler error:", error);
        return NextResponse.json(
            { success: false, error: error.message },
            { status: 500 }
        );
    }
}

/**
 * POST /api/scheduler/run
 * Force execute a specific schedule immediately
 */
export async function POST(request: NextRequest) {
    try {
        const body = await request.json();
        const { scheduleId } = body;

        if (!scheduleId) {
            return NextResponse.json(
                { success: false, error: "Missing scheduleId" },
                { status: 400 }
            );
        }

        const config = getServerConfig();
        if (!config) {
            return NextResponse.json(
                { success: false, error: "No Snowflake connection" },
                { status: 401 }
            );
        }

        const connection = await snowflakePool.getConnection(config);
        await ensureConnectionContext(connection, config);

        // Force update the schedule to be due now
        const updateQuery = `
            UPDATE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES
            SET NEXT_RUN_AT = CURRENT_TIMESTAMP()
            WHERE SCHEDULE_ID = '${scheduleId}'
        `;
        await executeQuery(connection, updateQuery);

        return NextResponse.json({
            success: true,
            data: { message: "Schedule marked for immediate execution" },
        });

    } catch (error: any) {
        console.error("Force execute error:", error);
        return NextResponse.json(
            { success: false, error: error.message },
            { status: 500 }
        );
    }
}
