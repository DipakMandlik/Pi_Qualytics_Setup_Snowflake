import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

// POST /api/dq/run-profiling
// Executes table profiling and stores results in DQ_COLUMN_PROFILE
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { dataset_id, database, schema, table, profile_level = "BASIC", triggered_by } = body;
    const dqDatabase = (body.dqDatabase || "DATA_QUALITY_DB").toUpperCase();
    const dqMetricsSchema = (body.dqMetricsSchema || "DQ_METRICS").toUpperCase();

    if (!database || !schema || !table) {
      return NextResponse.json(
        { success: false, error: "Missing required parameters: database, schema, table" },
        { status: 400 }
      );
    }

    let conn: any;
    try {
      const config = getServerConfig();
      conn = await snowflakePool.getConnection(config || undefined);
    } catch (e: any) {
      return NextResponse.json(
        { success: false, error: `Unable to establish Snowflake connection: ${e?.message || e}` },
        { status: 401 }
      );
    }

    const startTime = Date.now();

    // Generate unique run_id
    const runId = `DQ_PROFILE_${Date.now()}_${Math.random().toString(36).substring(2, 8).toUpperCase()}`;

    // Resolve dataset_id if not provided - DO NOT use fallback to ensure consistency
    let resolvedDatasetId = dataset_id;
    if (!resolvedDatasetId) {
      try {
        const datasetSQL = `
          SELECT DATASET_ID 
          FROM ${dqDatabase}.DQ_CONFIG.DATASETS
          WHERE UPPER(DATABASE_NAME) = UPPER(?)
            AND UPPER(SCHEMA_NAME) = UPPER(?)
            AND UPPER(TABLE_NAME) = UPPER(?)
          LIMIT 1
        `;
        const datasetResult = await new Promise<any[]>((resolve, reject) => {
          conn.execute({
            sqlText: datasetSQL,
            binds: [database, schema, table],
            complete: (err: any, _stmt: any, rows: any) => {
              if (err) reject(err);
              else resolve(rows || []);
            },
          });
        });
        if (datasetResult.length > 0 && datasetResult[0].DATASET_ID) {
          resolvedDatasetId = datasetResult[0].DATASET_ID;
        } else {
          // No fallback - dataset must be registered for consistency
          console.warn(`Dataset not registered for ${database}.${schema}.${table}`);
          resolvedDatasetId = null;
        }
      } catch (lookupErr: any) {
        console.warn("Dataset lookup failed:", lookupErr.message);
        resolvedDatasetId = null;
      }
    }

    // Insert run control record - use triggered_by param if provided, else CURRENT_USER()
    const triggeredByValue = triggered_by ? `'${triggered_by}'` : 'CURRENT_USER()';
    const insertRunSQL = `
      INSERT INTO ${dqDatabase}.${dqMetricsSchema}.DQ_RUN_CONTROL (
        RUN_ID, TRIGGERED_BY, START_TS, RUN_STATUS, TOTAL_CHECKS, CREATED_TS
      ) VALUES (?, ${triggeredByValue}, CURRENT_TIMESTAMP(), 'RUNNING', 0, CURRENT_TIMESTAMP())
    `;
    await new Promise<void>((resolve, reject) => {
      conn.execute({
        sqlText: insertRunSQL,
        binds: [runId],
        complete: (err: any) => {
          if (err) reject(err);
          else resolve();
        },
      });
    });

    // Fetch columns from INFORMATION_SCHEMA
    const columnsSQL = `
      SELECT 
        COLUMN_NAME, 
        DATA_TYPE, 
        IS_NULLABLE, 
        ORDINAL_POSITION
      FROM ${database}.INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_CATALOG = ?
        AND TABLE_SCHEMA = ?
        AND TABLE_NAME = ?
      ORDER BY ORDINAL_POSITION
    `;
    const columnsResult = await new Promise<any[]>((resolve, reject) => {
      conn.execute({
        sqlText: columnsSQL,
        binds: [database.toUpperCase(), schema.toUpperCase(), table.toUpperCase()],
        complete: (err: any, _stmt: any, rows: any) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      });
    });

    if (columnsResult.length === 0) {
      // Update run control to FAILED
      await updateRunControl(conn, dqDatabase, dqMetricsSchema, runId, "FAILED", 0, 0, 0, 0, startTime);
      return NextResponse.json(
        { success: false, error: `No columns found for table ${database}.${schema}.${table}` },
        { status: 404 }
      );
    }

    const profileResults: any[] = [];
    let processedColumns = 0;

    // Profile each column
    for (const col of columnsResult) {
      const columnName = col.COLUMN_NAME;
      const dataType = col.DATA_TYPE.toUpperCase();

      try {
        let profileSQL: string;
        let isNumeric = false;
        let isString = false;
        let isDate = false;

        // Determine data type category
        if (dataType.includes("NUMBER") || dataType.includes("INT") || dataType.includes("FLOAT") ||
          dataType.includes("DECIMAL") || dataType.includes("DOUBLE") || dataType.includes("REAL")) {
          isNumeric = true;
        } else if (dataType.includes("VARCHAR") || dataType.includes("CHAR") || dataType.includes("TEXT") ||
          dataType.includes("STRING")) {
          isString = true;
        } else if (dataType.includes("DATE") || dataType.includes("TIME") || dataType.includes("TIMESTAMP")) {
          isDate = true;
        }

        // Build profiling query based on data type
        if (isNumeric) {
          profileSQL = `
            SELECT
              COUNT(*) AS total_records,
              COUNT(*) - COUNT("${columnName}") AS null_count,
              COUNT(DISTINCT "${columnName}") AS distinct_count,
              MIN("${columnName}")::VARCHAR AS min_value,
              MAX("${columnName}")::VARCHAR AS max_value,
              AVG("${columnName}") AS avg_value,
              STDDEV("${columnName}") AS stddev_value,
              NULL AS min_length,
              NULL AS max_length,
              NULL AS avg_length,
              NULL AS future_date_count
            FROM ${database}.${schema}."${table}"
          `;
        } else if (isString) {
          profileSQL = `
            SELECT
              COUNT(*) AS total_records,
              COUNT(*) - COUNT("${columnName}") AS null_count,
              COUNT(DISTINCT "${columnName}") AS distinct_count,
              MIN("${columnName}")::VARCHAR AS min_value,
              MAX("${columnName}")::VARCHAR AS max_value,
              NULL AS avg_value,
              NULL AS stddev_value,
              MIN(LENGTH("${columnName}")) AS min_length,
              MAX(LENGTH("${columnName}")) AS max_length,
              AVG(LENGTH("${columnName}")) AS avg_length,
              NULL AS future_date_count
            FROM ${database}.${schema}."${table}"
          `;
        } else if (isDate) {
          profileSQL = `
            SELECT
              COUNT(*) AS total_records,
              COUNT(*) - COUNT("${columnName}") AS null_count,
              COUNT(DISTINCT "${columnName}") AS distinct_count,
              MIN("${columnName}")::VARCHAR AS min_value,
              MAX("${columnName}")::VARCHAR AS max_value,
              NULL AS avg_value,
              NULL AS stddev_value,
              NULL AS min_length,
              NULL AS max_length,
              NULL AS avg_length,
              SUM(CASE WHEN "${columnName}" > CURRENT_TIMESTAMP() THEN 1 ELSE 0 END) AS future_date_count
            FROM ${database}.${schema}."${table}"
          `;
        } else {
          // Default: treat as string
          profileSQL = `
            SELECT
              COUNT(*) AS total_records,
              COUNT(*) - COUNT("${columnName}") AS null_count,
              COUNT(DISTINCT "${columnName}") AS distinct_count,
              NULL AS min_value,
              NULL AS max_value,
              NULL AS avg_value,
              NULL AS stddev_value,
              NULL AS min_length,
              NULL AS max_length,
              NULL AS avg_length,
              NULL AS future_date_count
            FROM ${database}.${schema}."${table}"
          `;
        }

        const profileRow = await new Promise<any[]>((resolve, reject) => {
          conn.execute({
            sqlText: profileSQL,
            complete: (err: any, _stmt: any, rows: any) => {
              if (err) reject(err);
              else resolve(rows || []);
            },
          });
        });

        if (profileRow.length > 0) {
          const stats = profileRow[0];

          // Insert into DQ_COLUMN_PROFILE
          const insertProfileSQL = `
            INSERT INTO ${dqDatabase}.${dqMetricsSchema}.DQ_COLUMN_PROFILE (
              RUN_ID, DATASET_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
              COLUMN_NAME, DATA_TYPE,
              TOTAL_RECORDS, NULL_COUNT, DISTINCT_COUNT,
              MIN_VALUE, MAX_VALUE, AVG_VALUE, STDDEV_VALUE,
              MIN_LENGTH, MAX_LENGTH, AVG_LENGTH,
              FUTURE_DATE_COUNT, PROFILE_TS
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP())
          `;

          await new Promise<void>((resolve, reject) => {
            conn.execute({
              sqlText: insertProfileSQL,
              binds: [
                runId,
                resolvedDatasetId,
                database.toUpperCase(),
                schema.toUpperCase(),
                table.toUpperCase(),
                columnName,
                dataType,
                stats.TOTAL_RECORDS,
                stats.NULL_COUNT,
                stats.DISTINCT_COUNT,
                stats.MIN_VALUE,
                stats.MAX_VALUE,
                stats.AVG_VALUE,
                stats.STDDEV_VALUE,
                stats.MIN_LENGTH,
                stats.MAX_LENGTH,
                stats.AVG_LENGTH,
                stats.FUTURE_DATE_COUNT,
              ],
              complete: (err: any) => {
                if (err) reject(err);
                else resolve();
              },
            });
          });

          profileResults.push({
            column_name: columnName,
            data_type: dataType,
            total_records: stats.TOTAL_RECORDS,
            null_count: stats.NULL_COUNT,
            distinct_count: stats.DISTINCT_COUNT,
            min_value: stats.MIN_VALUE,
            max_value: stats.MAX_VALUE,
            avg_value: stats.AVG_VALUE,
            stddev_value: stats.STDDEV_VALUE,
            min_length: stats.MIN_LENGTH,
            max_length: stats.MAX_LENGTH,
            avg_length: stats.AVG_LENGTH,
            future_date_count: stats.FUTURE_DATE_COUNT,
          });

          processedColumns++;
        }
      } catch (colErr: any) {
        console.warn(`Error profiling column ${columnName}:`, colErr.message);
        profileResults.push({
          column_name: columnName,
          data_type: dataType,
          error: colErr.message,
        });
      }
    }

    // Insert synthetic checks into DQ_CHECK_RESULTS based on profiling flags
    // This makes profiling visible in Run Results modal and Activity tab
    let passedChecks = 0;
    let failedChecks = 0;
    let warningChecks = 0;

    for (const result of profileResults) {
      if (result.error) continue; // Skip columns with errors

      const totalRecords = result.total_records || 0;
      const nullCount = result.null_count || 0;
      const distinctCount = result.distinct_count || 0;
      const nullPercent = totalRecords > 0 ? (nullCount / totalRecords) * 100 : 0;
      const distinctPercent = totalRecords > 0 ? (distinctCount / totalRecords) * 100 : 0;
      const futureDateCount = result.future_date_count || 0;

      // Define synthetic profiling checks
      const syntheticChecks: Array<{
        ruleName: string;
        ruleType: string;
        status: "PASSED" | "FAILED" | "WARNING";
        passRate: number;
        threshold: number;
        totalRecords: number;
        invalidRecords: number;
        failureReason: string | null;
      }> = [];

      // Check 1: High Nulls (>20% nulls = FAILED, >5% = WARNING)
      if (nullPercent > 20) {
        syntheticChecks.push({
          ruleName: "PROFILE_HIGH_NULLS",
          ruleType: "COMPLETENESS",
          status: "FAILED",
          passRate: 100 - nullPercent,
          threshold: 80,
          totalRecords,
          invalidRecords: nullCount,
          failureReason: `${nullPercent.toFixed(1)}% null values detected`,
        });
        failedChecks++;
      } else if (nullPercent > 5) {
        syntheticChecks.push({
          ruleName: "PROFILE_NULL_CHECK",
          ruleType: "COMPLETENESS",
          status: "WARNING",
          passRate: 100 - nullPercent,
          threshold: 95,
          totalRecords,
          invalidRecords: nullCount,
          failureReason: `${nullPercent.toFixed(1)}% null values detected`,
        });
        warningChecks++;
      } else {
        syntheticChecks.push({
          ruleName: "PROFILE_NULL_CHECK",
          ruleType: "COMPLETENESS",
          status: "PASSED",
          passRate: 100 - nullPercent,
          threshold: 95,
          totalRecords,
          invalidRecords: nullCount,
          failureReason: null,
        });
        passedChecks++;
      }

      // Check 2: Low Cardinality (<10% distinct and >1 distinct = WARNING)
      if (distinctPercent < 10 && distinctCount > 1 && totalRecords > 10) {
        syntheticChecks.push({
          ruleName: "PROFILE_LOW_CARDINALITY",
          ruleType: "UNIQUENESS",
          status: "WARNING",
          passRate: distinctPercent,
          threshold: 10,
          totalRecords,
          invalidRecords: totalRecords - distinctCount,
          failureReason: `Only ${distinctCount} distinct values (${distinctPercent.toFixed(1)}%)`,
        });
        warningChecks++;
      }

      // Check 3: Future Dates (any future dates = FAILED)
      if (futureDateCount > 0) {
        syntheticChecks.push({
          ruleName: "PROFILE_FUTURE_DATES",
          ruleType: "VALIDITY",
          status: "FAILED",
          passRate: ((totalRecords - futureDateCount) / totalRecords) * 100,
          threshold: 100,
          totalRecords,
          invalidRecords: futureDateCount,
          failureReason: `${futureDateCount} records with future dates`,
        });
        failedChecks++;
      }

      // Insert synthetic checks into DQ_CHECK_RESULTS
      for (const check of syntheticChecks) {
        try {
          const insertCheckSQL = `
            INSERT INTO ${dqDatabase}.${dqMetricsSchema}.DQ_CHECK_RESULTS (
              RUN_ID, DATASET_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
              COLUMN_NAME, RULE_NAME, RULE_TYPE, CHECK_STATUS,
              PASS_RATE, THRESHOLD, TOTAL_RECORDS, INVALID_RECORDS,
              FAILURE_REASON, CREATED_TS
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP())
          `;
          await new Promise<void>((resolve, reject) => {
            conn.execute({
              sqlText: insertCheckSQL,
              binds: [
                runId,
                resolvedDatasetId,
                database.toUpperCase(),
                schema.toUpperCase(),
                table.toUpperCase(),
                result.column_name,
                check.ruleName,
                check.ruleType,
                check.status,
                check.passRate,
                check.threshold,
                check.totalRecords,
                check.invalidRecords,
                check.failureReason,
              ],
              complete: (err: any) => {
                if (err) reject(err);
                else resolve();
              },
            });
          });
        } catch (checkErr: any) {
          console.warn(`Error inserting synthetic check for ${result.column_name}:`, checkErr.message);
        }
      }
    }

    // Determine final run status based on checks
    const finalStatus = failedChecks > 0 ? "COMPLETED_WITH_FAILURES" : "COMPLETED";
    const totalChecks = passedChecks + failedChecks + warningChecks;

    // Update run control with final status and check counts
    await updateRunControl(
      conn, dqDatabase, dqMetricsSchema, runId, finalStatus,
      totalChecks, passedChecks, failedChecks, warningChecks, startTime
    );

    const durationSeconds = (Date.now() - startTime) / 1000;

    return NextResponse.json({
      success: true,
      data: {
        run_id: runId,
        dataset_id: resolvedDatasetId,
        database,
        schema,
        table,
        profile_level,
        columns_profiled: processedColumns,
        total_columns: columnsResult.length,
        total_checks: totalChecks,
        passed_checks: passedChecks,
        failed_checks: failedChecks,
        warning_checks: warningChecks,
        duration_seconds: durationSeconds,
        results: profileResults,
      },
    });
  } catch (error: any) {
    console.error("POST /api/dq/run-profiling error:", error);
    return NextResponse.json(
      { success: false, error: error.message || "Failed to run profiling" },
      { status: 500 }
    );
  }
}

async function updateRunControl(
  conn: any,
  dqDatabase: string,
  dqMetricsSchema: string,
  runId: string,
  status: string,
  totalChecks: number,
  passedChecks: number,
  failedChecks: number,
  warningChecks: number,
  startTime: number
) {
  const durationSeconds = (Date.now() - startTime) / 1000;
  const updateSQL = `
    UPDATE ${dqDatabase}.${dqMetricsSchema}.DQ_RUN_CONTROL
    SET RUN_STATUS = ?,
        END_TS = CURRENT_TIMESTAMP(),
        DURATION_SECONDS = ?,
        TOTAL_CHECKS = ?,
        PASSED_CHECKS = ?,
        FAILED_CHECKS = ?,
        WARNING_CHECKS = ?
    WHERE RUN_ID = ?
  `;
  await new Promise<void>((resolve, reject) => {
    conn.execute({
      sqlText: updateSQL,
      binds: [status, durationSeconds, totalChecks, passedChecks, failedChecks, warningChecks, runId],
      complete: (err: any) => {
        if (err) reject(err);
        else resolve();
      },
    });
  });
}
