import { NextRequest, NextResponse } from "next/server";
import {
  snowflakePool,
  executeQuery,
  ensureConnectionContext,
} from "@/lib/snowflake";
import { getServerConfig } from "@/lib/server-config";
import { createErrorResponse } from '@/lib/errors';
import { logger } from '@/lib/logger';
import { retryQuery } from '@/lib/retry';
import { cache, CacheTTL, generateCacheKey } from '@/lib/cache';

/**
 * GET /api/dq/critical-failed-records
 * Fetches count of critical failed records for CURRENT_DATE
 * Falls back to most recent date if no data for today
 */
export async function GET(request: NextRequest) {
  const startTime = Date.now();
  const endpoint = '/api/dq/critical-failed-records';

  try {
    logger.logApiRequest(endpoint, 'GET');

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
        {
          success: false,
          error: "Not connected to Snowflake. Please connect first.",
        },
        { status: 401 }
      );
    }

    const connection = await snowflakePool.getConnection(config);
    await ensureConnectionContext(connection, config);

    // Try to get critical failed records for current date first
    const todayCriticalQuery = `
      SELECT
          COUNT(*) AS critical_failed_records
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_FAILED_RECORDS
        WHERE IS_CRITICAL = TRUE
          AND DETECTED_TS::DATE = (
            SELECT MAX(DETECTED_TS::DATE)
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_FAILED_RECORDS
            WHERE IS_CRITICAL = TRUE
          )
    `;

    const todayResult = await executeQuery(connection, todayCriticalQuery);
    let criticalFailedRecords = null;

    if (
      todayResult.rows.length > 0 &&
      todayResult.rows[0][0] !== null &&
      todayResult.rows[0][0] > 0
    ) {
      criticalFailedRecords = todayResult.rows[0][0];
    } else {
      // Fallback to most recent date
      const fallbackQuery = `
        
        SELECT
            COUNT(*) AS critical_failed_records
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_FAILED_RECORDS
        WHERE IS_CRITICAL = TRUE
            AND DETECTED_TS::DATE = CURRENT_DATE
      `;

      const fallbackResult = await executeQuery(connection, fallbackQuery);
      if (
        fallbackResult.rows.length > 0 &&
        fallbackResult.rows[0][0] !== null
      ) {
        criticalFailedRecords = fallbackResult.rows[0][0];
      }
    }

    const result = { criticalFailedRecords: criticalFailedRecords || 0 };
    cache.set(cacheKey, result, CacheTTL.QUICK_METRICS);
    logger.logApiResponse(endpoint, true, Date.now() - startTime);

    return NextResponse.json({
      success: true,
      data: result,
      metadata: { cached: false, timestamp: new Date().toISOString() },
    });
  } catch (error: any) {
    logger.error("Error fetching critical failed records", error, { endpoint });
    return NextResponse.json(createErrorResponse(error), { status: 500 });
  }
}
