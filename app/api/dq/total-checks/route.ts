import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';

/**
 * GET /api/dq/total-checks
 * Fetches total checks executed for CURRENT_DATE
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

    // Try to get total checks for current date first
    const todayChecksQuery = `
      SELECT
        START_TS::DATE AS check_date,
        SUM(TOTAL_CHECKS) AS total_checks_executed
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
      WHERE START_TS::DATE = CURRENT_DATE
      GROUP BY START_TS::DATE
    `;

    const todayResult = await executeQuery(connection, todayChecksQuery);
    let totalChecks = null;
    let checkDate = null;

    if (todayResult.rows.length > 0 && todayResult.rows[0][1] !== null) {
      checkDate = todayResult.rows[0][0];
      totalChecks = todayResult.rows[0][1];
    } else {
      // Fallback to most recent date
      const fallbackQuery = `
        SELECT
          START_TS::DATE AS check_date,
          SUM(TOTAL_CHECKS) AS total_checks_executed
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
        WHERE START_TS::DATE = (
          SELECT MAX(START_TS::DATE)
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
        )
        GROUP BY START_TS::DATE
      `;
      
      const fallbackResult = await executeQuery(connection, fallbackQuery);
      if (fallbackResult.rows.length > 0 && fallbackResult.rows[0][1] !== null) {
        checkDate = fallbackResult.rows[0][0];
        totalChecks = fallbackResult.rows[0][1];
      }
    }

    return NextResponse.json({
      success: true,
      data: {
        totalChecks: totalChecks || 0,
        checkDate: checkDate,
      },
    });
  } catch (error: any) {
    console.error('Error fetching total checks:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to fetch total checks',
      },
      { status: 500 }
    );
  }
}

