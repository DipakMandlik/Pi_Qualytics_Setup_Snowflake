import { NextRequest, NextResponse } from 'next/server';
import type { SnowflakeConfig } from '@/lib/snowflake';
import { setServerConfig } from '@/lib/server-config';

export const runtime = 'nodejs';

/**
 * POST /api/snowflake/connect
 * Tests the Snowflake connection with provided credentials
 * 
 * Request body:
 * {
 *   "accountUrl": "...",
 *   "username": "...",
 *   "password": "..." or "token": "...",
 *   "role": "..." // optional
 * }
 * 
 * Note: Warehouse, database, and schema are optional and can be set later
 */
export async function POST(request: NextRequest) {
  try {
    const config: SnowflakeConfig = await request.json();

    // Validate required fields
    if (!config.accountUrl && !config.account) {
      return NextResponse.json(
        { success: false, error: 'Account URL or Account is required' },
        { status: 400 }
      );
    }

    if (!config.username || (!config.password && !config.token)) {
      return NextResponse.json(
        { success: false, error: 'Missing required fields: username and password/token' },
        { status: 400 }
      );
    }

    // Import Snowflake helpers at runtime to avoid bundler/Turbopack issues
    const { snowflakePool, executeQuery } = await import('@/lib/snowflake');

    // Test connection by executing a simple query
    // Note: We don't require warehouse/database/schema for initial connection
    // These can be set later when user selects them from the sidebar
    const connection = await snowflakePool.getConnection(config);
    
    // Query to test connection and get account info
    const result = await executeQuery(
      connection, 
      'SELECT CURRENT_VERSION() as version, CURRENT_ACCOUNT() as account, CURRENT_USER() as user, CURRENT_ROLE() as role'
    );

    // Store config server-side for future API calls
    setServerConfig(config);

    return NextResponse.json({
      success: true,
      message: 'Connection successful! Select warehouse, database, and schema from the sidebar to start.',
      data: result,
    });
  } catch (error: any) {
    console.error('Snowflake connection test error:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to connect to Snowflake',
      },
      { status: 500 }
    );
  }
}

