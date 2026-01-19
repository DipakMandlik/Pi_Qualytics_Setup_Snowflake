import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

/**
 * GET /api/snowflake/table-metadata
 * Fetches comprehensive table metadata from Snowflake INFORMATION_SCHEMA
 * 
 * Query Parameters:
 * - database: Database name (required)
 * - schema: Schema name (required)
 * - table: Table name (required)
 */
export async function GET(request: NextRequest) {
    try {
        const searchParams = request.nextUrl.searchParams;
        const database = searchParams.get("database");
        const schema = searchParams.get("schema");
        const table = searchParams.get("table");

        // Validate required parameters
        if (!database || !schema || !table) {
            return NextResponse.json(
                {
                    success: false,
                    error: "Missing required parameters: database, schema, and table are required",
                },
                { status: 400 }
            );
        }

        // Get server configuration
        const config = getServerConfig();
        if (!config) {
            return NextResponse.json(
                {
                    success: false,
                    error: "No Snowflake connection found. Please connect first.",
                },
                { status: 401 }
            );
        }

        // Get connection from pool
        const connection = await snowflakePool.getConnection(config);

        // Set database context first
        await new Promise<void>((resolve, reject) => {
            connection.execute({
                sqlText: `USE DATABASE ${database.toUpperCase()}`,
                complete: (err: any) => {
                    if (err) reject(err);
                    else resolve();
                },
            });
        });

        // Query to fetch table metadata
        const query = `
      SELECT 
        t.ROW_COUNT,
        t.BYTES,
        t.CREATED,
        t.LAST_ALTERED,
        (SELECT COUNT(*) FROM ${database.toUpperCase()}.INFORMATION_SCHEMA.COLUMNS c 
         WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA 
           AND c.TABLE_NAME = t.TABLE_NAME
           AND c.TABLE_CATALOG = t.TABLE_CATALOG) as COLUMN_COUNT,
        (SELECT COUNT(*) FROM ${database.toUpperCase()}.INFORMATION_SCHEMA.COLUMNS c 
         WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA 
           AND c.TABLE_NAME = t.TABLE_NAME 
           AND c.TABLE_CATALOG = t.TABLE_CATALOG
           AND c.IS_NULLABLE = 'YES') as NULLABLE_COLUMN_COUNT
      FROM ${database.toUpperCase()}.INFORMATION_SCHEMA.TABLES t
      WHERE t.TABLE_SCHEMA = '${schema.toUpperCase()}'
        AND t.TABLE_NAME = '${table.toUpperCase()}'
        AND t.TABLE_CATALOG = '${database.toUpperCase()}'
    `;

        // Execute query
        const result = await new Promise<any>((resolve, reject) => {
            connection.execute({
                sqlText: query,
                complete: (err: any, stmt: any, rows: any) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(rows);
                    }
                },
            });
        });

        // Process results
        if (result && result.length > 0) {
            const row = result[0];
            const bytes = row.BYTES || 0;
            const bytesFormatted = formatBytes(bytes);

            return NextResponse.json({
                success: true,
                data: {
                    rowCount: row.ROW_COUNT || 0,
                    bytes: bytes,
                    bytesFormatted: bytesFormatted,
                    created: row.CREATED,
                    lastAltered: row.LAST_ALTERED,
                    columnCount: row.COLUMN_COUNT || 0,
                    nullableColumnCount: row.NULLABLE_COLUMN_COUNT || 0,
                },
            });
        }

        return NextResponse.json(
            {
                success: false,
                error: "Table not found",
            },
            { status: 404 }
        );
    } catch (error: any) {
        console.error("Error fetching table metadata:", error);
        return NextResponse.json(
            {
                success: false,
                error: error.message || "Failed to fetch table metadata",
            },
            { status: 500 }
        );
    }
}

/**
 * Helper function to format bytes into human-readable format
 */
function formatBytes(bytes: number): string {
    if (bytes === 0) return "0 Bytes";
    const k = 1024;
    const sizes = ["Bytes", "KB", "MB", "GB", "TB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + " " + sizes[i];
}
