import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool, executeQuery, ensureConnectionContext } from "@/lib/snowflake";

/**
 * GET /api/schedules
 * List schedules for a table
 */
export async function GET(request: NextRequest) {
    try {
        const searchParams = request.nextUrl.searchParams;
        const database = searchParams.get("database");
        const schema = searchParams.get("schema");
        const table = searchParams.get("table");

        if (!database || !schema || !table) {
            return NextResponse.json(
                { success: false, error: "Missing required parameters" },
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

        const query = `
            SELECT 
                SCHEDULE_ID,
                DATABASE_NAME,
                SCHEMA_NAME,
                TABLE_NAME,
                SCAN_TYPE,
                IS_RECURRING,
                SCHEDULE_TYPE,
                SCHEDULE_TIME,
                SCHEDULE_DAYS,
                TIMEZONE,
                START_DATE,
                END_DATE,
                SKIP_IF_RUNNING,
                ON_FAILURE_ACTION,
                MAX_FAILURES,
                FAILURE_COUNT,
                NOTIFY_ON_FAILURE,
                NOTIFY_ON_SUCCESS,
                STATUS,
                NEXT_RUN_AT,
                LAST_RUN_AT,
                CREATED_BY,
                CREATED_AT
            FROM DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES
            WHERE DATABASE_NAME = '${database.toUpperCase()}'
                AND SCHEMA_NAME = '${schema.toUpperCase()}'
                AND TABLE_NAME = '${table.toUpperCase()}'
                AND STATUS != 'deleted'
            ORDER BY CREATED_AT DESC
        `;

        try {
            const result = await executeQuery(connection, query);
            const schedules = result.rows.map((row) => {
                const obj: any = {};
                result.columns.forEach((col, idx) => {
                    obj[col] = row[idx];
                });
                return {
                    scheduleId: obj.SCHEDULE_ID,
                    databaseName: obj.DATABASE_NAME,
                    schemaName: obj.SCHEMA_NAME,
                    tableName: obj.TABLE_NAME,
                    scanType: obj.SCAN_TYPE,
                    isRecurring: obj.IS_RECURRING,
                    scheduleType: obj.SCHEDULE_TYPE,
                    scheduleTime: obj.SCHEDULE_TIME,
                    scheduleDays: obj.SCHEDULE_DAYS ? JSON.parse(obj.SCHEDULE_DAYS) : [],
                    timezone: obj.TIMEZONE,
                    startDate: obj.START_DATE,
                    endDate: obj.END_DATE,
                    skipIfRunning: obj.SKIP_IF_RUNNING,
                    onFailureAction: obj.ON_FAILURE_ACTION,
                    maxFailures: obj.MAX_FAILURES,
                    failureCount: obj.FAILURE_COUNT,
                    notifyOnFailure: obj.NOTIFY_ON_FAILURE,
                    notifyOnSuccess: obj.NOTIFY_ON_SUCCESS,
                    status: obj.STATUS,
                    nextRunAt: obj.NEXT_RUN_AT,
                    lastRunAt: obj.LAST_RUN_AT,
                    createdBy: obj.CREATED_BY,
                    createdAt: obj.CREATED_AT,
                    nextRunFormatted: obj.NEXT_RUN_AT
                        ? new Date(obj.NEXT_RUN_AT).toLocaleString("en-US", { dateStyle: "medium", timeStyle: "short" })
                        : null,
                };
            });

            return NextResponse.json({
                success: true,
                data: { schedules },
            });
        } catch (e: any) {
            if (e.message?.includes("does not exist")) {
                return NextResponse.json({
                    success: true,
                    data: { schedules: [], tableNotExists: true },
                });
            }
            throw e;
        }
    } catch (error: any) {
        console.error("Error fetching schedules:", error);
        return NextResponse.json(
            { success: false, error: error.message },
            { status: 500 }
        );
    }
}

/**
 * POST /api/schedules
 * Create a new schedule
 */
export async function POST(request: NextRequest) {
    try {
        const body = await request.json();
        const {
            database,
            schema,
            table,
            scanType,
            isRecurring,
            scheduleType,
            scheduleTime,
            scheduleDays,
            timezone,
            startDate,
            endDate,
            skipIfRunning,
            onFailureAction,
            maxFailures,
            notifyOnFailure,
            notifyOnSuccess,
        } = body;

        if (!database || !schema || !table || !scanType) {
            return NextResponse.json(
                { success: false, error: "Missing required fields" },
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

        const scheduleId = crypto.randomUUID();

        // Create table with enhanced schema (replace if exists with old schema)
        const createTableQuery = `
            CREATE TABLE IF NOT EXISTS DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES (
                SCHEDULE_ID VARCHAR(36) PRIMARY KEY,
                DATABASE_NAME VARCHAR(255) NOT NULL,
                SCHEMA_NAME VARCHAR(255) NOT NULL,
                TABLE_NAME VARCHAR(255) NOT NULL,
                SCAN_TYPE VARCHAR(50) NOT NULL,
                IS_RECURRING BOOLEAN DEFAULT FALSE,
                SCHEDULE_TYPE VARCHAR(20),
                SCHEDULE_TIME VARCHAR(10),
                SCHEDULE_DAYS VARCHAR(100),
                TIMEZONE VARCHAR(50) DEFAULT 'UTC',
                START_DATE DATE,
                END_DATE DATE,
                SKIP_IF_RUNNING BOOLEAN DEFAULT FALSE,
                ON_FAILURE_ACTION VARCHAR(20) DEFAULT 'continue',
                MAX_FAILURES INTEGER DEFAULT 3,
                FAILURE_COUNT INTEGER DEFAULT 0,
                NOTIFY_ON_FAILURE BOOLEAN DEFAULT FALSE,
                NOTIFY_ON_SUCCESS BOOLEAN DEFAULT FALSE,
                STATUS VARCHAR(20) DEFAULT 'active',
                NEXT_RUN_AT TIMESTAMP_TZ,
                LAST_RUN_AT TIMESTAMP_TZ,
                CREATED_BY VARCHAR(255),
                CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
            )
        `;

        try {
            await executeQuery(connection, createTableQuery);
        } catch (e: any) {
            console.log("Table creation note:", e.message);
        }

        // Calculate next run time based on schedule time and timezone
        // Calculate the date in TypeScript for reliability
        let nextRunAt = "CURRENT_TIMESTAMP()";
        if (scheduleTime) {
            // Parse schedule time
            const [hours, minutes] = scheduleTime.split(":").map(Number);
            const now = new Date();

            // Create a date for today at the scheduled time
            const scheduledToday = new Date();
            scheduledToday.setHours(hours, minutes, 0, 0);

            // If the scheduled time has passed today, schedule for tomorrow
            let targetDate = scheduledToday;
            if (scheduledToday <= now) {
                targetDate = new Date(scheduledToday.getTime() + 24 * 60 * 60 * 1000); // tomorrow
            }

            // Format as ISO string for Snowflake
            const isoDate = targetDate.toISOString().slice(0, 16).replace("T", " ");
            nextRunAt = `TO_TIMESTAMP_TZ('${isoDate}', 'YYYY-MM-DD HH24:MI')`;
        }

        const insertQuery = `
            INSERT INTO DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES (
                SCHEDULE_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
                SCAN_TYPE, IS_RECURRING, SCHEDULE_TYPE, SCHEDULE_TIME,
                SCHEDULE_DAYS, TIMEZONE, START_DATE, END_DATE,
                SKIP_IF_RUNNING, ON_FAILURE_ACTION, MAX_FAILURES,
                NOTIFY_ON_FAILURE, NOTIFY_ON_SUCCESS, STATUS, NEXT_RUN_AT
            ) VALUES (
                '${scheduleId}',
                '${database.toUpperCase()}',
                '${schema.toUpperCase()}',
                '${table.toUpperCase()}',
                '${scanType}',
                ${isRecurring ?? false},
                ${scheduleType ? `'${scheduleType}'` : "NULL"},
                ${scheduleTime ? `'${scheduleTime}'` : "NULL"},
                ${scheduleDays?.length ? `'${JSON.stringify(scheduleDays)}'` : "NULL"},
                '${timezone || "UTC"}',
                ${startDate ? `'${startDate}'` : "NULL"},
                ${endDate ? `'${endDate}'` : "NULL"},
                ${skipIfRunning ?? false},
                '${onFailureAction || "continue"}',
                ${maxFailures ?? 3},
                ${notifyOnFailure ?? false},
                ${notifyOnSuccess ?? false},
                'active',
                ${nextRunAt}
            )
        `;

        // Try insert, if schema error (invalid identifier), drop and recreate table
        try {
            await executeQuery(connection, insertQuery);
        } catch (insertError: any) {
            if (insertError.message?.includes("invalid identifier")) {
                console.log("Schema mismatch detected, recreating table...");
                // Drop old table and recreate
                await executeQuery(connection, "DROP TABLE IF EXISTS DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES");
                await executeQuery(connection, createTableQuery);
                // Retry insert
                await executeQuery(connection, insertQuery);
            } else {
                throw insertError;
            }
        }

        // Generate human-readable summary
        let summary = "";
        if (isRecurring) {
            if (scheduleType === "daily") {
                summary = `Daily at ${scheduleTime} ${timezone}`;
            } else if (scheduleType === "weekly") {
                const days = scheduleDays?.join(", ") || "";
                summary = `Weekly on ${days} at ${scheduleTime} ${timezone}`;
            } else if (scheduleType === "hourly") {
                summary = `Every hour`;
            }
        } else {
            summary = `One-time on ${startDate} at ${scheduleTime} ${timezone}`;
        }

        return NextResponse.json({
            success: true,
            data: {
                scheduleId,
                summary,
                message: "Schedule created successfully",
            },
        });
    } catch (error: any) {
        console.error("Error creating schedule:", error);
        return NextResponse.json(
            { success: false, error: error.message },
            { status: 500 }
        );
    }
}

/**
 * PUT /api/schedules
 * Update a schedule (pause/resume/delete/update)
 */
export async function PUT(request: NextRequest) {
    try {
        const body = await request.json();
        const { scheduleId, status, forceRunNow, ...updates } = body;

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

        let setClause = "UPDATED_AT = CURRENT_TIMESTAMP()";
        if (status) {
            setClause += `, STATUS = '${status}'`;
        }
        if (forceRunNow) {
            setClause += ", NEXT_RUN_AT = CURRENT_TIMESTAMP()";
        }

        const updateQuery = `
            UPDATE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES
            SET ${setClause}
            WHERE SCHEDULE_ID = '${scheduleId}'
        `;

        await executeQuery(connection, updateQuery);

        return NextResponse.json({
            success: true,
            data: { message: forceRunNow ? "Schedule marked for immediate execution" : `Schedule ${status === "deleted" ? "deleted" : status || "updated"}` },
        });
    } catch (error: any) {
        console.error("Error updating schedule:", error);
        return NextResponse.json(
            { success: false, error: error.message },
            { status: 500 }
        );
    }
}

/**
 * DELETE /api/schedules
 * Soft delete a schedule
 */
export async function DELETE(request: NextRequest) {
    try {
        const searchParams = request.nextUrl.searchParams;
        const scheduleId = searchParams.get("scheduleId");

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

        const deleteQuery = `
            UPDATE DATA_QUALITY_DB.DQ_CONFIG.SCAN_SCHEDULES
            SET STATUS = 'deleted',
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE SCHEDULE_ID = '${scheduleId}'
        `;

        await executeQuery(connection, deleteQuery);

        return NextResponse.json({
            success: true,
            data: { message: "Schedule deleted" },
        });
    } catch (error: any) {
        console.error("Error deleting schedule:", error);
        return NextResponse.json(
            { success: false, error: error.message },
            { status: 500 }
        );
    }
}
