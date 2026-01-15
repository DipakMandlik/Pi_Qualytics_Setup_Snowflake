import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';

/**
 * GET /api/dq/failures-by-rule-type
 * Fetches failure counts by rule type for the last 30 days
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

    // Get failures by rule type for the last 30 days
    const failuresByRuleTypeQuery = `
      SELECT
        RULE_TYPE,
        COUNT(*) AS FAILURE_COUNT
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
      WHERE CHECK_STATUS = 'FAILED'
        AND CHECK_TIMESTAMP >= CURRENT_DATE - 30
      GROUP BY RULE_TYPE
      ORDER BY FAILURE_COUNT DESC
    `;

    const result = await executeQuery(connection, failuresByRuleTypeQuery);
    const ruleTypes = result.rows.map(row => ({
      name: row[0], // RULE_TYPE
      failures: row[1] // FAILURE_COUNT
    }));

    return NextResponse.json({
      success: true,
      data: {
        ruleTypes: ruleTypes,
      },
    });
  } catch (error) {
    console.error('Error fetching failures by rule type:', error);
    return NextResponse.json(
      {
        success: false,
        error: 'Failed to fetch failures by rule type data',
      },
      { status: 500 }
    );
  }
}