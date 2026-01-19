import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';

/**
 * GET /api/dq/quality-score-history
 * Fetches quality score history for a specific table from DQ_DAILY_SUMMARY
 * Query params: database, schema, table, days (optional, default 30)
 */
export async function GET(request: NextRequest) {
    try {
        const { searchParams } = new URL(request.url);
        const database = searchParams.get('database');
        const schema = searchParams.get('schema');
        const table = searchParams.get('table');
        const days = parseInt(searchParams.get('days') || '30', 10);

        if (!database || !schema || !table) {
            return NextResponse.json(
                {
                    success: false,
                    error: 'Missing required parameters: database, schema, table',
                },
                { status: 400 }
            );
        }

        // Get config from server-side storage
        const config = getServerConfig();
        if (!config) {
            return NextResponse.json(
                {
                    success: false,
                    error: 'Not connected to Snowflake. Please connect first.',
                },
                { status: 401 }
            );
        }

        const connection = await snowflakePool.getConnection(config);
        await ensureConnectionContext(connection, config);

        // Query quality score history from DQ_DAILY_SUMMARY
        const query = `
      SELECT
        SUMMARY_DATE,
        ROUND(DQ_SCORE, 2) AS dq_score
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      WHERE DATABASE_NAME = ?
        AND SCHEMA_NAME = ?
        AND TABLE_NAME = ?
      ORDER BY SUMMARY_DATE DESC
      LIMIT ?
    `;

        const result = await executeQuery(connection, query, [
            database.toUpperCase(),
            schema.toUpperCase(),
            table.toUpperCase(),
            days,
        ]);

        // Format the data for the chart
        const history = result.rows.map((row: any) => ({
            date: row[0], // SUMMARY_DATE
            dq_score: row[1], // DQ_SCORE
        })).reverse(); // Reverse to show oldest to newest for chart

        return NextResponse.json({
            success: true,
            data: {
                history,
                count: history.length,
                database,
                schema,
                table,
            },
        });
    } catch (error: any) {
        console.error('Error fetching quality score history:', error);
        return NextResponse.json(
            {
                success: false,
                error: error.message || 'Failed to fetch quality score history',
            },
            { status: 500 }
        );
    }
}
