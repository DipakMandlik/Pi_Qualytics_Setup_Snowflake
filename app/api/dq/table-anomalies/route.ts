import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool, executeQuery, ensureConnectionContext } from "@/lib/snowflake";

/**
 * GET /api/dq/table-anomalies
 * Detects statistical anomalies by comparing current metrics to historical baselines
 * 
 * Query Parameters:
 * - database: Database name (required)
 * - schema: Schema name (required)
 * - table: Table name (required)
 * - days: Number of days for historical baseline (default: 14)
 */
export async function GET(request: NextRequest) {
    try {
        const searchParams = request.nextUrl.searchParams;
        const database = searchParams.get("database");
        const schema = searchParams.get("schema");
        const table = searchParams.get("table");
        const days = parseInt(searchParams.get("days") || "14");

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

        const anomalies: any[] = [];
        let lastScanTime: string | null = null;

        // Helper function to calculate severity based on deviation percentage
        const getSeverity = (deviationPct: number): string => {
            const absDeviation = Math.abs(deviationPct);
            if (absDeviation > 50) return "Critical";
            if (absDeviation > 25) return "High";
            if (absDeviation > 10) return "Medium";
            return "Low";
        };

        // 1. Volume Anomaly Detection - Compare current row count to historical average
        const volumeQuery = `
            WITH current_stats AS (
                SELECT 
                    ROW_COUNT,
                    LAST_ALTERED
                FROM ${database.toUpperCase()}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '${schema.toUpperCase()}'
                    AND TABLE_NAME = '${table.toUpperCase()}'
            ),
            historical_stats AS (
                SELECT 
                    AVG(TOTAL_RECORDS) AS AVG_ROW_COUNT,
                    STDDEV(TOTAL_RECORDS) AS STDDEV_ROW_COUNT,
                    COUNT(*) AS SAMPLE_COUNT
                FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
                WHERE DATABASE_NAME = '${database.toUpperCase()}'
                    AND SCHEMA_NAME = '${schema.toUpperCase()}'
                    AND TABLE_NAME = '${table.toUpperCase()}'
                    AND CHECK_TIMESTAMP >= DATEADD(day, -${days}, CURRENT_TIMESTAMP())
                    AND TOTAL_RECORDS > 0
            )
            SELECT 
                c.ROW_COUNT AS CURRENT_VALUE,
                c.LAST_ALTERED,
                h.AVG_ROW_COUNT AS BASELINE_VALUE,
                h.STDDEV_ROW_COUNT,
                h.SAMPLE_COUNT,
                CASE 
                    WHEN h.AVG_ROW_COUNT > 0 
                    THEN ((c.ROW_COUNT - h.AVG_ROW_COUNT) / h.AVG_ROW_COUNT) * 100 
                    ELSE 0 
                END AS DEVIATION_PCT
            FROM current_stats c, historical_stats h
        `;

        try {
            const volumeResult = await executeQuery(connection, volumeQuery);
            if (volumeResult.rows.length > 0) {
                const row = volumeResult.rows[0];
                const cols = volumeResult.columns;
                const getVal = (name: string) => row[cols.indexOf(name)];

                const currentValue = getVal("CURRENT_VALUE");
                const baselineValue = getVal("BASELINE_VALUE");
                const deviationPct = getVal("DEVIATION_PCT") || 0;
                const lastAltered = getVal("LAST_ALTERED");
                lastScanTime = lastAltered ? new Date(lastAltered).toISOString() : null;

                // Only flag if we have historical data and significant deviation
                if (baselineValue && Math.abs(deviationPct) > 5) {
                    anomalies.push({
                        anomalyId: `VOL_${table.toUpperCase()}_${Date.now()}`,
                        metric: "Row Count",
                        scope: "Table",
                        target: table.toUpperCase(),
                        severity: getSeverity(deviationPct),
                        baseline: Math.round(baselineValue),
                        current: currentValue,
                        deviationPct: Math.round(deviationPct * 10) / 10,
                        detectedAt: lastScanTime,
                        detectedAtFormatted: lastScanTime ? new Date(lastScanTime).toLocaleString("en-US", { dateStyle: "medium", timeStyle: "short" }) : null,
                        status: Math.abs(deviationPct) > 10 ? "Active" : "Resolved",
                        description: deviationPct > 0
                            ? `Row count is ${Math.abs(Math.round(deviationPct))}% higher than the ${days}-day average`
                            : `Row count is ${Math.abs(Math.round(deviationPct))}% lower than the ${days}-day average`,
                    });
                }
            }
        } catch (e: any) {
            console.log("Volume anomaly detection error:", e.message);
        }

        // 2. Null Percentage Anomalies - From profiling data
        const nullQuery = `
            WITH current_profile AS (
                SELECT 
                    COLUMN_NAME,
                    NULL_PERCENTAGE AS CURRENT_NULL_PCT,
                    PROFILE_TIMESTAMP
                FROM DATA_QUALITY_DB.DQ_METRICS.DQ_COLUMN_PROFILE
                WHERE 1=1
                    AND lower(TABLE_CATALOG) = lower('${database}')
                    AND lower(TABLE_SCHEMA) = lower('${schema}')
                    AND lower(TABLE_NAME) = lower('${table}')
                GROUP BY 1, 2, 3
            ),
            LATEST_RUN AS (
                SELECT 
                    TABLE_CATALOG,
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    MAX(PROFILE_TIMESTAMP) as MAX_TS
                FROM DATA_QUALITY_DB.DQ_METRICS.DQ_COLUMN_PROFILE
                WHERE 1=1
                    AND lower(TABLE_CATALOG) = lower('${database}')
                    AND lower(TABLE_SCHEMA) = lower('${schema}')
                    AND lower(TABLE_NAME) = lower('${table}')
                GROUP BY 1, 2, 3
            ),
            NULL_ANOMALIES AS (
                SELECT 
                    COLUMN_NAME,
                    NULL_COUNT,
                    TOTAL_RECORDS,
                    PROFILE_TIMESTAMP AS PROFILE_TS
                FROM DATA_QUALITY_DB.DQ_METRICS.DQ_COLUMN_PROFILE
                WHERE lower(TABLE_CATALOG) = lower('${database}')
                    AND lower(TABLE_SCHEMA) = lower('${schema}')
                    AND lower(TABLE_NAME) = lower('${table}')
            ),
            historical_avg AS (
                SELECT 
                    COLUMN_NAME,
                    AVG(NULL_PERCENTAGE) AS AVG_NULL_PCT
                FROM DATA_QUALITY_DB.DQ_METRICS.DQ_COLUMN_PROFILE
                WHERE lower(TABLE_CATALOG) = lower('${database}')
                    AND lower(TABLE_SCHEMA) = lower('${schema}')
                    AND lower(TABLE_NAME) = lower('${table}')
                    AND PROFILE_TIMESTAMP >= DATEADD(day, -${days}, CURRENT_TIMESTAMP())
                    AND PROFILE_TIMESTAMP < (
                        SELECT MAX(PROFILE_TIMESTAMP) 
                        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_COLUMN_PROFILE
                        WHERE lower(TABLE_CATALOG) = lower('${database}')
                            AND lower(TABLE_SCHEMA) = lower('${schema}')
                            AND lower(TABLE_NAME) = lower('${table}')
                    )
                GROUP BY COLUMN_NAME
            )
            SELECT 
                c.COLUMN_NAME,
                c.CURRENT_NULL_PCT,
                c.PROFILE_TIMESTAMP,
                h.AVG_NULL_PCT AS BASELINE_NULL_PCT,
                c.CURRENT_NULL_PCT - h.AVG_NULL_PCT AS DEVIATION
            FROM current_profile c
            JOIN historical_avg h ON c.COLUMN_NAME = h.COLUMN_NAME
            WHERE ABS(c.CURRENT_NULL_PCT - h.AVG_NULL_PCT) > 5
            ORDER BY ABS(c.CURRENT_NULL_PCT - h.AVG_NULL_PCT) DESC
            LIMIT 5
        `;

        try {
            const nullResult = await executeQuery(connection, nullQuery);
            for (const row of nullResult.rows) {
                const cols = nullResult.columns;
                const getVal = (name: string) => row[cols.indexOf(name)];

                const columnName = getVal("COLUMN_NAME");
                const currentNullPct = getVal("CURRENT_NULL_PCT") || 0;
                const baselineNullPct = getVal("BASELINE_NULL_PCT") || 0;
                const deviation = getVal("DEVIATION") || 0;
                const profileTimestamp = getVal("PROFILE_TIMESTAMP");

                if (!lastScanTime && profileTimestamp) {
                    lastScanTime = new Date(profileTimestamp).toISOString();
                }

                anomalies.push({
                    anomalyId: `NULL_${columnName}_${Date.now()}`,
                    metric: "Null Percentage",
                    scope: "Column",
                    target: columnName,
                    severity: getSeverity(deviation),
                    baseline: Math.round(baselineNullPct * 10) / 10,
                    current: Math.round(currentNullPct * 10) / 10,
                    deviationPct: Math.round(deviation * 10) / 10,
                    detectedAt: profileTimestamp ? new Date(profileTimestamp).toISOString() : null,
                    detectedAtFormatted: profileTimestamp ? new Date(profileTimestamp).toLocaleString("en-US", { dateStyle: "medium", timeStyle: "short" }) : null,
                    status: Math.abs(deviation) > 10 ? "Active" : "Resolved",
                    description: deviation > 0
                        ? `Null percentage increased by ${Math.abs(Math.round(deviation))} points`
                        : `Null percentage decreased by ${Math.abs(Math.round(deviation))} points`,
                });
            }
        } catch (e: any) {
            console.log("Null anomaly detection error:", e.message);
        }

        // 3. Duplicate Rate Anomalies - From profiling data
        const dupQuery = `
            WITH current_profile AS (
                SELECT 
                    COLUMN_NAME,
                    DUPLICATE_PERCENTAGE AS CURRENT_DUP_PCT,
                    PROFILE_TIMESTAMP
                FROM DATA_QUALITY_DB.DQ_METRICS.DQ_PROFILE_RESULTS
                WHERE DATABASE_NAME = '${database.toUpperCase()}'
                    AND SCHEMA_NAME = '${schema.toUpperCase()}'
                    AND TABLE_NAME = '${table.toUpperCase()}'
                    AND PROFILE_TIMESTAMP = (
                        SELECT MAX(PROFILE_TIMESTAMP) 
                        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_PROFILE_RESULTS
                        WHERE DATABASE_NAME = '${database.toUpperCase()}'
                            AND SCHEMA_NAME = '${schema.toUpperCase()}'
                            AND TABLE_NAME = '${table.toUpperCase()}'
                    )
            ),
            historical_avg AS (
                SELECT 
                    COLUMN_NAME,
                    AVG(DUPLICATE_PERCENTAGE) AS AVG_DUP_PCT
                FROM DATA_QUALITY_DB.DQ_METRICS.DQ_PROFILE_RESULTS
                WHERE DATABASE_NAME = '${database.toUpperCase()}'
                    AND SCHEMA_NAME = '${schema.toUpperCase()}'
                    AND TABLE_NAME = '${table.toUpperCase()}'
                    AND PROFILE_TIMESTAMP >= DATEADD(day, -${days}, CURRENT_TIMESTAMP())
                    AND PROFILE_TIMESTAMP < (
                        SELECT MAX(PROFILE_TIMESTAMP) 
                        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_PROFILE_RESULTS
                        WHERE DATABASE_NAME = '${database.toUpperCase()}'
                            AND SCHEMA_NAME = '${schema.toUpperCase()}'
                            AND TABLE_NAME = '${table.toUpperCase()}'
                    )
                GROUP BY COLUMN_NAME
            )
            SELECT 
                c.COLUMN_NAME,
                c.CURRENT_DUP_PCT,
                h.AVG_DUP_PCT AS BASELINE_DUP_PCT,
                c.CURRENT_DUP_PCT - h.AVG_DUP_PCT AS DEVIATION
            FROM current_profile c
            JOIN historical_avg h ON c.COLUMN_NAME = h.COLUMN_NAME
            WHERE ABS(c.CURRENT_DUP_PCT - h.AVG_DUP_PCT) > 5
            ORDER BY ABS(c.CURRENT_DUP_PCT - h.AVG_DUP_PCT) DESC
            LIMIT 5
        `;

        try {
            const dupResult = await executeQuery(connection, dupQuery);
            for (const row of dupResult.rows) {
                const cols = dupResult.columns;
                const getVal = (name: string) => row[cols.indexOf(name)];

                const columnName = getVal("COLUMN_NAME");
                const currentDupPct = getVal("CURRENT_DUP_PCT") || 0;
                const baselineDupPct = getVal("BASELINE_DUP_PCT") || 0;
                const deviation = getVal("DEVIATION") || 0;

                anomalies.push({
                    anomalyId: `DUP_${columnName}_${Date.now()}`,
                    metric: "Duplicate Rate",
                    scope: "Column",
                    target: columnName,
                    severity: getSeverity(deviation),
                    baseline: Math.round(baselineDupPct * 10) / 10,
                    current: Math.round(currentDupPct * 10) / 10,
                    deviationPct: Math.round(deviation * 10) / 10,
                    detectedAt: lastScanTime,
                    detectedAtFormatted: lastScanTime ? new Date(lastScanTime).toLocaleString("en-US", { dateStyle: "medium", timeStyle: "short" }) : null,
                    status: Math.abs(deviation) > 10 ? "Active" : "Resolved",
                    description: deviation > 0
                        ? `Duplicate rate increased by ${Math.abs(Math.round(deviation))} points`
                        : `Duplicate rate decreased by ${Math.abs(Math.round(deviation))} points`,
                });
            }
        } catch (e: any) {
            console.log("Duplicate anomaly detection error:", e.message);
        }

        // Sort anomalies by severity
        const severityOrder: Record<string, number> = { "Critical": 0, "High": 1, "Medium": 2, "Low": 3 };
        anomalies.sort((a, b) => (severityOrder[a.severity] ?? 99) - (severityOrder[b.severity] ?? 99));

        // Calculate summary
        const summary = {
            activeAnomalies: anomalies.filter(a => a.status === "Active").length,
            criticalAnomalies: anomalies.filter(a => a.severity === "Critical").length,
            resolvedAnomalies: anomalies.filter(a => a.status === "Resolved").length,
            totalAnomalies: anomalies.length,
            lastScanTime,
            lastScanTimeFormatted: lastScanTime ? new Date(lastScanTime).toLocaleString("en-US", { dateStyle: "medium", timeStyle: "short" }) : null,
        };

        return NextResponse.json({
            success: true,
            data: {
                summary,
                anomalies,
            },
        });
    } catch (error: any) {
        console.error("Error detecting anomalies:", error);
        return NextResponse.json(
            {
                success: false,
                error: error.message || "Failed to detect anomalies",
            },
            { status: 500 }
        );
    }
}
