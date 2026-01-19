import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { createErrorResponse } from '@/lib/errors';
import { logger } from '@/lib/logger';
import { retryQuery } from '@/lib/retry';
import { cache, CacheTTL, generateCacheKey } from '@/lib/cache';

/**
 * GET /api/dq/total-checks
 * Fetches total checks executed for CURRENT_DATE
 * Falls back to most recent date if no data for today
 * 
 * Optimized with caching and retry logic
 */
export async function GET(request: NextRequest) {
  const startTime = Date.now();
  const endpoint = '/api/dq/total-checks';

  try {
    logger.logApiRequest(endpoint, 'GET');

    // Check cache first
    const cacheKey = generateCacheKey(endpoint);
    const cachedData = cache.get(cacheKey);
    if (cachedData) {
      logger.logApiResponse(endpoint, true, Date.now() - startTime);
      return NextResponse.json({
        success: true,
        data: cachedData,
        metadata: { cached: true, timestamp: new Date().toISOString() },
      });
    }

    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        { success: false, error: { code: 'AUTH_FAILED', message: 'Not connected', userMessage: 'Please connect to Snowflake first.' } },
        { status: 401 }
      );
    }

    const connection = await snowflakePool.getConnection(config);
    await ensureConnectionContext(connection, config);

    const result = await retryQuery(async () => {
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

      return { totalChecks: totalChecks || 0, checkDate };
    }, 'total-checks');

    cache.set(cacheKey, result, CacheTTL.KPI_METRICS);
    logger.logApiResponse(endpoint, true, Date.now() - startTime);

    return NextResponse.json({
      success: true,
      data: result,
      metadata: { cached: false, timestamp: new Date().toISOString() },
    });
  } catch (error: any) {
    logger.error('Error fetching total checks', error, { endpoint });
    return NextResponse.json(createErrorResponse(error), { status: 500 });
  }
}

