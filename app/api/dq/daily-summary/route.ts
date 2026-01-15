import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';

/**
 * GET /api/dq/daily-summary
 * Fetches aggregated DQ score trend data for the last 30 days
 * 
 * Returns the average DQ_SCORE for each day, grouped by SUMMARY_DATE
 * Used for displaying trend visualization in the dashboard
 */
export async function GET(request: NextRequest) {
  try {
    // Build aggregated query to calculate average DQ scores per day
    // This query calculates the average DQ_SCORE for each day over the last 30 days
    // Results are grouped by SUMMARY_DATE and ordered chronologically for line chart visualization
    const query = `
      SELECT
        SUMMARY_DATE,
        ROUND(AVG(DQ_SCORE), 2) AS DQ_SCORE
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      WHERE SUMMARY_DATE BETWEEN DATEADD(days, -30, CURRENT_DATE()) AND CURRENT_DATE()
      GROUP BY SUMMARY_DATE
      ORDER BY SUMMARY_DATE
    `;

    // Retrieve Snowflake connection configuration from server-side storage
    // This configuration contains the authentication credentials and connection details
    const config = getServerConfig();
    
    // Validate that we have a valid Snowflake connection configuration
    // Return 401 Unauthorized error if the user hasn't connected to Snowflake yet
    if (!config) {
      return NextResponse.json(
        {
          success: false,
          error: 'Not connected to Snowflake. Please connect first.',
        },
        { status: 401 }
      );
    }

    // Acquire a database connection from the connection pool
    // The pool manages connection reuse and prevents resource exhaustion
    const connection = await snowflakePool.getConnection(config);
    
    // Ensure the connection context is properly set up with the correct warehouse, database, and schema
    // This step configures the connection to query the correct objects in Snowflake
    await ensureConnectionContext(connection, config);
    
    // Execute the SQL query against Snowflake
    // Returns an object containing rows, columns metadata, and row count
    const result = await executeQuery(connection, query);

    // Transform the result set into an array of objects
    // Maps each row (array format) into a key-value object using column names as keys
    // This makes the response more intuitive and easier to consume in frontend applications
    return NextResponse.json({
      success: true,
      data: result.rows.map((row) => {
        const obj: any = {};
        // Iterate through column names and pair them with their corresponding values from each row
        result.columns.forEach((col, idx) => {
          obj[col] = row[idx];
        });
        return obj;
      }),
      rowCount: result.rowCount,
    });
  } catch (error: any) {
    // Log the error to server console for debugging and monitoring purposes
    console.error('Error fetching daily summary:', error);
    
    // Return a 500 Internal Server Error response with error details
    // The error message is included to help clients understand what went wrong
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to fetch daily summary',
      },
      { status: 500 }
    );
  }
}

