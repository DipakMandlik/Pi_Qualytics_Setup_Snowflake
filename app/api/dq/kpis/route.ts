import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';

/**
 * GET /api/dq/kpis
 * Fetches aggregated KPI data for the dashboard
 * 
 * Query params:
 * - days: Number of days to aggregate (default: 1 for latest day)
 */
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const days = parseInt(searchParams.get('days') || '1');

    // Get latest daily summary for overall KPIs
    const summaryQuery = `
      SELECT 
        SUM(TOTAL_CHECKS) as TOTAL_CHECKS,
        SUM(PASSED_CHECKS) as PASSED_CHECKS,
        SUM(FAILED_CHECKS) as FAILED_CHECKS,
        SUM(WARNING_CHECKS) as WARNING_CHECKS,
        AVG(DQ_SCORE) as AVG_DQ_SCORE,
        SUM(TOTAL_RECORDS) as TOTAL_RECORDS,
        SUM(FAILED_RECORDS_COUNT) as FAILED_RECORDS,
        COUNT(DISTINCT TABLE_NAME) as TOTAL_TABLES,
        SUM(CASE WHEN IS_SLA_MET = TRUE THEN 1 ELSE 0 END) as SLA_MET_COUNT,
        COUNT(*) as TOTAL_DATASETS
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      WHERE SUMMARY_DATE >= DATEADD(day, -${days}, CURRENT_DATE())
        AND SUMMARY_DATE = (SELECT MAX(SUMMARY_DATE) FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY)
    `;

    // Get latest run control info
    const runControlQuery = `
      SELECT 
        RUN_ID,
        RUN_STATUS,
        START_TS,
        END_TS,
        TOTAL_CHECKS,
        PASSED_CHECKS,
        FAILED_CHECKS,
        WARNING_CHECKS
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
      WHERE START_TS = (SELECT MAX(START_TS) FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL)
      LIMIT 1
    `;

    // Get score trend
    const trendQuery = `
      SELECT 
        SUMMARY_DATE,
        AVG(DQ_SCORE) as AVG_SCORE,
        AVG(PREV_DAY_SCORE) as AVG_PREV_SCORE
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      WHERE SUMMARY_DATE >= DATEADD(day, -${days}, CURRENT_DATE())
      GROUP BY SUMMARY_DATE
      ORDER BY SUMMARY_DATE DESC
      LIMIT 2
    `;

    // Get quality grade distribution
    const gradeQuery = `
      SELECT 
        QUALITY_GRADE,
        COUNT(*) as COUNT
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      WHERE SUMMARY_DATE = (SELECT MAX(SUMMARY_DATE) FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY)
      GROUP BY QUALITY_GRADE
    `;

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

    // Ensure warehouse, database, and schema are set
    await ensureConnectionContext(connection, config);

    const [summaryResult, runControlResult, trendResult, gradeResult] = await Promise.all([
      executeQuery(connection, summaryQuery),
      executeQuery(connection, runControlQuery),
      executeQuery(connection, trendQuery),
      executeQuery(connection, gradeQuery),
    ]);

    // Process summary data
    const summary = summaryResult.rows[0] || [];
    const summaryObj: any = {};
    summaryResult.columns.forEach((col, idx) => {
      summaryObj[col] = summary[idx];
    });

    // Process run control data
    const runControl = runControlResult.rows[0] || [];
    const runControlObj: any = {};
    if (runControl.length > 0) {
      runControlResult.columns.forEach((col, idx) => {
        runControlObj[col] = runControl[idx];
      });
    }

    // Calculate SLA compliance percentage
    const slaCompliance = summaryObj.TOTAL_DATASETS > 0
      ? (summaryObj.SLA_MET_COUNT / summaryObj.TOTAL_DATASETS) * 100
      : 0;

    // Calculate score trend
    let scoreTrend = 'STABLE';
    if (trendResult.rows.length >= 2) {
      const current = trendResult.rows[0][1] || 0;
      const previous = trendResult.rows[1][1] || 0;
      if (current > previous) {
        scoreTrend = 'IMPROVING';
      } else if (current < previous) {
        scoreTrend = 'DECLINING';
      }
    }

    // Get most common quality grade
    let qualityGrade = 'N/A';
    if (gradeResult.rows.length > 0) {
      const grades = gradeResult.rows.map((row: any[]) => ({
        grade: row[0],
        count: row[1],
      }));
      grades.sort((a, b) => (b.count || 0) - (a.count || 0));
      qualityGrade = grades[0].grade || 'N/A';
    }

    const kpis = {
      overallDQScore: summaryObj.AVG_DQ_SCORE || 0,
      totalChecks: summaryObj.TOTAL_CHECKS || 0,
      passedChecks: summaryObj.PASSED_CHECKS || 0,
      failedChecks: summaryObj.FAILED_CHECKS || 0,
      warningChecks: summaryObj.WARNING_CHECKS || 0,
      totalRecords: summaryObj.TOTAL_RECORDS || 0,
      failedRecords: summaryObj.FAILED_RECORDS || 0,
      totalTables: summaryObj.TOTAL_TABLES || 0,
      lastRunStatus: runControlObj.RUN_STATUS || 'UNKNOWN',
      lastRunTime: runControlObj.START_TS || null,
      lastRunId: runControlObj.RUN_ID || null,
      qualityGrade,
      slaCompliance: Math.round(slaCompliance * 100) / 100,
      scoreTrend,
      lastRunChecks: {
        total: runControlObj.TOTAL_CHECKS || 0,
        passed: runControlObj.PASSED_CHECKS || 0,
        failed: runControlObj.FAILED_CHECKS || 0,
        warning: runControlObj.WARNING_CHECKS || 0,
      },
    };

    return NextResponse.json({
      success: true,
      data: kpis,
    });
  } catch (error: any) {
    console.error('Error fetching KPIs:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to fetch KPIs',
      },
      { status: 500 }
    );
  }
}

