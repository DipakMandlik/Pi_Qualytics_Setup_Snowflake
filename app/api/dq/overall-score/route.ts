import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';

/**
 * GET /api/dq/overall-score
 * Fetches overall DQ score calculated as average of all DQ_SCORE values for CURRENT_DATE
 * Also returns yesterday's score for comparison
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

    // Try to get today's score first, fallback to most recent date
    const todayScoreQuery = `
      SELECT
        SUMMARY_DATE,
        ROUND(AVG(DQ_SCORE), 2) AS overall_dq_score
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      WHERE SUMMARY_DATE = CURRENT_DATE
      GROUP BY SUMMARY_DATE
    `;

    const todayResult = await executeQuery(connection, todayScoreQuery);

    let currentScore = null;
    let currentDate = null;
    
    if (todayResult.rows.length > 0 && todayResult.rows[0][1] !== null) {
      currentDate = todayResult.rows[0][0];
      currentScore = todayResult.rows[0][1];
    } else {
      // Fallback to most recent date
      const fallbackQuery = `
        SELECT
          SUMMARY_DATE,
          ROUND(AVG(DQ_SCORE), 2) AS overall_dq_score
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
        WHERE SUMMARY_DATE = (SELECT MAX(SUMMARY_DATE) FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY)
        GROUP BY SUMMARY_DATE
      `;
      const fallbackResult = await executeQuery(connection, fallbackQuery);
      if (fallbackResult.rows.length > 0 && fallbackResult.rows[0][1] !== null) {
        currentDate = fallbackResult.rows[0][0];
        currentScore = fallbackResult.rows[0][1];
      }
    }

    // Calculate previous day's score based on the date we're using
    let previousScore = null;
    let scoreDiff = undefined;

    if (currentDate && currentScore !== null) {
      // Convert date to string format for query
      const dateStr = currentDate instanceof Date 
        ? currentDate.toISOString().split('T')[0] 
        : String(currentDate).split('T')[0];
      
      const previousDayQuery = `
        SELECT
          ROUND(AVG(DQ_SCORE), 2) AS overall_dq_score
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
        WHERE SUMMARY_DATE = DATEADD(day, -1, '${dateStr}'::DATE)
      `;

      const previousResult = await executeQuery(connection, previousDayQuery);
      if (previousResult.rows.length > 0 && previousResult.rows[0][0] !== null) {
        previousScore = previousResult.rows[0][0];
        scoreDiff = Math.round((currentScore - previousScore) * 10) / 10;
      }
    }

    return NextResponse.json({
      success: true,
      data: {
        overallScore: currentScore,
        previousScore: previousScore,
        scoreDifference: scoreDiff,
        summaryDate: currentDate,
      },
    });
  } catch (error: any) {
    console.error('Error fetching overall DQ score:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to fetch overall DQ score',
      },
      { status: 500 }
    );
  }
}

