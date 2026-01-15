import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';

/**
 * GET /api/dq/failed-checks
 * Fetches total failed checks for CURRENT_DATE
 * Falls back to most recent date if no data for today
 */
export async function GET(request: NextRequest) {
  try {
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

    // Try to get failed checks for current date first
    const todayFailedChecksQuery = `
      SELECT
        SUM(FAILED_CHECKS) AS total_failed_checks
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      WHERE SUMMARY_DATE = CURRENT_DATE
    `;

    const todayResult = await executeQuery(connection, todayFailedChecksQuery);
    let totalFailedChecks = null;
    let failedChecksDiff = undefined;

    if (todayResult.rows.length > 0 && todayResult.rows[0][0] !== null) {
      totalFailedChecks = todayResult.rows[0][0];
      
      // Static value for yesterday's failed checks (for now)
      const yesterdayFailedChecks = 10;
      
      if (yesterdayFailedChecks > 0) {
        failedChecksDiff = Math.round(((totalFailedChecks - yesterdayFailedChecks) / yesterdayFailedChecks) * 100 * 10) / 10;
      }
    } else {
      // Fallback to most recent date
      const fallbackQuery = `
        SELECT
          SUM(FAILED_CHECKS) AS total_failed_checks
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
        WHERE SUMMARY_DATE = (
          SELECT MAX(SUMMARY_DATE)
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
        )
      `;

      const fallbackResult = await executeQuery(connection, fallbackQuery);
      if (fallbackResult.rows.length > 0 && fallbackResult.rows[0][0] !== null) {
        totalFailedChecks = fallbackResult.rows[0][0];
        
        // Static value for yesterday's failed checks (for now)
        const yesterdayFailedChecks = 10;
        
        if (yesterdayFailedChecks > 0) {
          failedChecksDiff = Math.round(((totalFailedChecks - yesterdayFailedChecks) / yesterdayFailedChecks) * 100 * 10) / 10;
        }
      }
    }

    return NextResponse.json({
      success: true,
      data: {
        totalFailedChecks: totalFailedChecks || 0,
        failedChecksDifference: failedChecksDiff,
      },
    });
  } catch (error: any) {
    console.error('Error fetching failed checks:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to fetch failed checks',
      },
      { status: 500 }
    );
  }
}