import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { createErrorResponse } from '@/lib/errors';
import { logger } from '@/lib/logger';
import { retryQuery } from '@/lib/retry';
import { cache, CacheTTL, generateCacheKey } from '@/lib/cache';

/**
 * GET /api/dq/overall-score
 * Fetches overall DQ score calculated as average of all DQ_SCORE values for CURRENT_DATE
 * Also returns yesterday's score for comparison
 * 
 * Optimizations:
 * - Cached for 60 seconds
 * - Retry logic for transient failures
 * - Structured error handling
 */
export async function GET(request: NextRequest) {
  const startTime = Date.now();
  const endpoint = '/api/dq/overall-score';

  try {
    logger.logApiRequest(endpoint, 'GET');

    // Try to get from cache
    const cacheKey = generateCacheKey(endpoint);
    const cachedData = cache.get(cacheKey);
    if (cachedData) {
      const duration = Date.now() - startTime;
      logger.logApiResponse(endpoint, true, duration);
      return NextResponse.json({
        success: true,
        data: cachedData,
        metadata: {
          cached: true,
          timestamp: new Date().toISOString(),
          queryTime: 0,
        },
      });
    }

    // Get config from server-side storage
    const config = getServerConfig();
    if (!config) {
      logger.warn('No Snowflake configuration found', { endpoint });
      return NextResponse.json(
        {
          success: false,
          error: {
            code: 'AUTH_FAILED',
            message: 'Not connected to Snowflake',
            userMessage: 'Please connect to Snowflake first.',
          },
        },
        { status: 401 }
      );
    }

    const connection = await snowflakePool.getConnection(config);
    await ensureConnectionContext(connection, config);

    // Fetch data with retry logic
    const queryStartTime = Date.now();

    const result = await retryQuery(async () => {
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

      // Calculate previous day's score
      let previousScore = null;
      let scoreDiff = undefined;

      if (currentDate && currentScore !== null) {
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

      return {
        overallScore: currentScore,
        previousScore: previousScore,
        scoreDifference: scoreDiff,
        summaryDate: currentDate,
      };
    }, 'overall-score');

    const queryTime = Date.now() - queryStartTime;
    logger.logQuery('Overall DQ Score', queryTime, 1);

    // Cache the result
    cache.set(cacheKey, result, CacheTTL.KPI_METRICS);

    const duration = Date.now() - startTime;
    logger.logApiResponse(endpoint, true, duration);

    return NextResponse.json({
      success: true,
      data: result,
      metadata: {
        cached: false,
        timestamp: new Date().toISOString(),
        queryTime,
      },
    });
  } catch (error: any) {
    const duration = Date.now() - startTime;
    logger.error('Error fetching overall DQ score', error, { endpoint });
    logger.logApiResponse(endpoint, false, duration);

    const errorResponse = createErrorResponse(error);
    return NextResponse.json(errorResponse, { status: 500 });
  }
}

