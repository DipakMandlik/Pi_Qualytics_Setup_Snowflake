import { NextRequest, NextResponse } from 'next/server';
import { getServerConfig } from '@/lib/server-config';

export const runtime = 'nodejs';

/**
 * GET /api/dq/datasets
 * Fetches available datasets (tables) from BANKING_DW.BRONZE schema
 */
export async function GET(request: NextRequest) {
  try {
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

    const { snowflakePool, executeQuery, ensureConnectionContext } = await import('@/lib/snowflake');

    const connection = await snowflakePool.getConnection(config);
    
    // Use BANKING_DW database and BRONZE schema
    await executeQuery(connection, 'USE DATABASE BANKING_DW');
    await executeQuery(connection, 'USE SCHEMA BRONZE');

    // Fetch table names that match the pattern STG_*
    const query = `
      SELECT TABLE_NAME
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = 'BRONZE'
        AND TABLE_CATALOG = 'BANKING_DW'
        AND TABLE_NAME LIKE 'STG_%'
      ORDER BY TABLE_NAME
    `;

    const result = await executeQuery(connection, query);

    const datasets = result.rows.map((row) => row[0]).filter(Boolean);

    return NextResponse.json({
      success: true,
      data: datasets,
      rowCount: datasets.length,
    });
  } catch (error: any) {
    console.error('Error fetching datasets:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to fetch datasets',
      },
      { status: 500 }
    );
  }
}

