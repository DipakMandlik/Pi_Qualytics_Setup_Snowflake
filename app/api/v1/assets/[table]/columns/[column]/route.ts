import { NextRequest, NextResponse } from 'next/server';
import { getServerConfig } from '@/lib/server-config';
import { snowflakePool } from '@/lib/snowflake';

export async function GET(
    request: NextRequest,
    { params }: { params: Promise<{ table: string; column: string }> }
) {
    try {
        const { table, column } = await params;
        console.log(`[API Details] Fetching details for ${table}.${column}`);
        const searchParams = request.nextUrl.searchParams;
        const database = searchParams.get('database');
        const schema = searchParams.get('schema');

        console.log(`[API Details] Params: DB=${database}, Schema=${schema}`);

        if (!database || !schema) {
            return NextResponse.json(
                { success: false, error: 'Missing required parameters: database, schema' },
                { status: 400 }
            );
        }

        const dqDatabase = 'DATA_QUALITY_DB';
        const observabilitySchema = 'DQ_METRICS';

        const config = getServerConfig();
        const conn = await snowflakePool.getConnection(config || undefined);

        // 1. Get Metadata from DQ_COLUMN_PROFILE (Latest)
        // Note: We use DQ_RUN_CONTROL to get latest completed run first
        // 1. Get Metadata from DQ_COLUMN_PROFILE (Latest)
        // Find latest run specific to this table
        const metadataSql = `
        WITH LatestExecution AS (
            SELECT MAX(RUN_ID) as RUN_ID
            FROM ${dqDatabase}.${observabilitySchema}.DQ_COLUMN_PROFILE
            WHERE UPPER(TABLE_NAME) = ?
              AND UPPER(SCHEMA_NAME) = ?
              AND UPPER(DATABASE_NAME) = ?
         )
         SELECT 
            COLUMN_NAME, 
            DATA_TYPE, 
            TOTAL_RECORDS as ROW_COUNT, 
            NULL_COUNT, 
            DISTINCT_COUNT,
            MIN_VALUE,
            MAX_VALUE,
            AVG_VALUE,
            STDDEV_VALUE,
            PROFILE_TS as LAST_UPDATED
         FROM ${dqDatabase}.${observabilitySchema}.DQ_COLUMN_PROFILE
         WHERE RUN_ID = (SELECT RUN_ID FROM LatestExecution)
           AND UPPER(TABLE_NAME) = ?
           AND UPPER(SCHEMA_NAME) = ?
           AND UPPER(DATABASE_NAME) = ?
           AND UPPER(COLUMN_NAME) = ?
        `;

        // 2. Get Stats History (Last 30 days) from DQ_COLUMN_PROFILE
        const historySql = `
         SELECT 
            PROFILE_TS as EXECUTION_TIMESTAMP,
            NULL_COUNT,
            DISTINCT_COUNT,
            TOTAL_RECORDS as ROW_COUNT
         FROM ${dqDatabase}.${observabilitySchema}.DQ_COLUMN_PROFILE
         WHERE UPPER(TABLE_NAME) = ?
           AND UPPER(SCHEMA_NAME) = ?
           AND UPPER(DATABASE_NAME) = ?
           AND UPPER(COLUMN_NAME) = ?
           AND PROFILE_TS >= DATEADD(day, -30, CURRENT_TIMESTAMP())
         ORDER BY PROFILE_TS ASC
        `;

        const [metadataRows, historyRows] = await Promise.all([
            new Promise<any[]>((resolve, reject) => {
                conn.execute({
                    sqlText: metadataSql,
                    binds: [
                        table.toUpperCase(), schema.toUpperCase(), database.toUpperCase(), // LatestExecution
                        table.toUpperCase(), schema.toUpperCase(), database.toUpperCase(), column.toUpperCase() // Main Query
                    ],
                    complete: (err: any, _stmt: any, rows: any) => {
                        if (err) reject(err); else resolve(rows || []);
                    }
                })
            }),
            new Promise<any[]>((resolve, reject) => {
                conn.execute({
                    sqlText: historySql,
                    binds: [table.toUpperCase(), schema.toUpperCase(), database.toUpperCase(), column.toUpperCase()],
                    complete: (err: any, _stmt: any, rows: any) => {
                        if (err) reject(err); else resolve(rows || []);
                    }
                })
            })
        ]);

        if (metadataRows.length === 0) {
            console.log(`[API Details] Column not found in DQ_COLUMN_PROFILE, trying INFORMATION_SCHEMA fallback`);

            // Fallback: Fetch from INFORMATION_SCHEMA
            try {
                const fallbackSql = `
                    SELECT 
                        COLUMN_NAME,
                        DATA_TYPE,
                        IS_NULLABLE
                    FROM ${database}.INFORMATION_SCHEMA.COLUMNS
                    WHERE UPPER(TABLE_NAME) = ?
                      AND UPPER(TABLE_SCHEMA) = ?
                      AND UPPER(COLUMN_NAME) = ?
                `;

                const fallbackRows = await new Promise<any[]>((resolve, reject) => {
                    conn.execute({
                        sqlText: fallbackSql,
                        binds: [table.toUpperCase(), schema.toUpperCase(), column.toUpperCase()],
                        complete: (err: any, _stmt: any, rows: any) => {
                            if (err) reject(err); else resolve(rows || []);
                        }
                    });
                });

                if (fallbackRows.length === 0) {
                    return NextResponse.json({
                        success: false,
                        error: 'Column not found in database. Please verify the column name.'
                    }, { status: 404 });
                }

                const fallbackMeta = fallbackRows[0];
                console.log(`[API Details] Found column in INFORMATION_SCHEMA:`, fallbackMeta);

                // Return minimal metadata without profiling stats
                return NextResponse.json({
                    success: true,
                    data: {
                        metadata: {
                            columnName: fallbackMeta.COLUMN_NAME,
                            dataType: fallbackMeta.DATA_TYPE,
                            isNullable: fallbackMeta.IS_NULLABLE === 'YES',
                            rowCount: null,
                            lastUpdated: null
                        },
                        currentStats: {
                            distinctCount: null,
                            nullCount: null,
                            rowCount: null,
                            min: null,
                            max: null,
                            avg: null,
                            stdDev: null
                        },
                        history: [],
                        needsProfiling: true // Flag to indicate profiling is needed
                    }
                });
            } catch (fallbackError: any) {
                console.error('[API Details] Fallback query failed:', fallbackError);
                return NextResponse.json({
                    success: false,
                    error: 'Column not found. Please run profiling first.'
                }, { status: 404 });
            }
        }

        const meta = metadataRows[0];
        console.log(`[API Details] Found metadata for ${column}:`, meta);

        // Transform stats history
        const statsHistory = historyRows.map(row => ({
            timestamp: row.EXECUTION_TIMESTAMP,
            nullCount: row.NULL_COUNT,
            distinctCount: row.DISTINCT_COUNT,
            rowCount: row.ROW_COUNT,
            nullPct: row.ROW_COUNT > 0 ? (row.NULL_COUNT / row.ROW_COUNT) * 100 : 0
        }));

        return NextResponse.json({
            success: true,
            data: {
                metadata: {
                    columnName: meta.COLUMN_NAME, // Frontend expects columnName
                    dataType: meta.DATA_TYPE,      // Frontend expects dataType
                    isNullable: false,             // Note: isNullable not in DQ_COLUMN_PROFILE, defaulting to false or query info schema if needed
                    rowCount: meta.ROW_COUNT,
                    lastUpdated: meta.LAST_UPDATED
                },
                currentStats: { // Renamed from statistics
                    distinctCount: meta.DISTINCT_COUNT,
                    nullCount: meta.NULL_COUNT,
                    rowCount: meta.ROW_COUNT,
                    min: meta.MIN_VALUE,
                    max: meta.MAX_VALUE,
                    avg: meta.AVG_VALUE,
                    stdDev: meta.STDDEV_VALUE
                },
                history: statsHistory // Renamed from statsHistory
            }
        });

    } catch (error: any) {
        console.error('API Error:', error);
        return NextResponse.json(
            { success: false, error: error.message || 'Internal Server Error' },
            { status: 500 }
        );
    }
}
