import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';

/**
 * GET /api/dq/score-by-dataset
 * Fetches average DQ scores by dataset for the most recent date
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

    // Get scores by dataset for the most recent date
    const scoreByDatasetQuery = `
      SELECT
        DATASET_ID,
        ROUND(AVG(DQ_SCORE), 2) AS DQ_SCORE
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      WHERE SUMMARY_DATE = (
        SELECT MAX(SUMMARY_DATE)
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      )
      GROUP BY DATASET_ID
      ORDER BY DQ_SCORE DESC
    `;

    const result = await executeQuery(connection, scoreByDatasetQuery);
    const datasets = result.rows.map(row => ({
      name: row[0], // DATASET_ID
      score: row[1] // DQ_SCORE
    }));

    return NextResponse.json({
      success: true,
      data: {
        datasets: datasets,
      },
    });
  } catch (error: any) {
    console.error('Error fetching score by dataset:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to fetch score by dataset',
      },
      { status: 500 }
    );
  }
}