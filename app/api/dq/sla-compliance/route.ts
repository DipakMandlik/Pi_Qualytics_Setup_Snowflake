import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';

/**
 * GET /api/dq/sla-compliance
 * Fetches SLA compliance percentage for CURRENT_DATE
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

    // Try to get SLA compliance for current date first
    const todaySlaQuery = `
      SELECT
        ROUND(
          (SUM(CASE WHEN IS_SLA_MET THEN 1 ELSE 0 END) * 100.0)
          / NULLIF(COUNT(*), 0),
          2
        ) AS sla_compliance_pct
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      WHERE SUMMARY_DATE = CURRENT_DATE
    `;

    const todayResult = await executeQuery(connection, todaySlaQuery);
    let slaCompliancePct = null;

    if (todayResult.rows.length > 0 && todayResult.rows[0][0] !== null) {
      slaCompliancePct = todayResult.rows[0][0];
    } else {
      // Fallback to most recent date
      const fallbackQuery = `
        SELECT
          ROUND(
            (SUM(CASE WHEN IS_SLA_MET THEN 1 ELSE 0 END) * 100.0)
            / NULLIF(COUNT(*), 0),
            2
          ) AS sla_compliance_pct
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
        WHERE SUMMARY_DATE = (
          SELECT MAX(SUMMARY_DATE)
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
        )
      `;

      const fallbackResult = await executeQuery(connection, fallbackQuery);
      if (fallbackResult.rows.length > 0 && fallbackResult.rows[0][0] !== null) {
        slaCompliancePct = fallbackResult.rows[0][0];
      }
    }

    return NextResponse.json({
      success: true,
      data: {
        slaCompliancePct: slaCompliancePct || 0,
      },
    });
  } catch (error: any) {
    console.error('Error fetching SLA compliance:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to fetch SLA compliance',
      },
      { status: 500 }
    );
  }
}