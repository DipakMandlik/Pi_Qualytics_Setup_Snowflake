'use client';

import { useState, useEffect } from 'react';
import { ColumnOverview } from './sections/ColumnOverview';
import { ColumnStatistics } from './sections/ColumnStatistics';
import { ColumnCheckResults } from './sections/ColumnCheckResults';
import { ColumnAnomaliesDrift } from './sections/ColumnAnomaliesDrift';
import { RunCheckDialog } from './RunCheckDialog';
import { Button } from '@/components/ui/button';
import { ExternalLink, BarChart3 } from 'lucide-react';

interface ColumnDetailPanelProps {
    database: string;
    schema: string;
    table: string;
    column: string | null;
}

export function ColumnDetailPanel({ database, schema, table, column }: ColumnDetailPanelProps) {
    const [details, setDetails] = useState<any>(null);
    const [checks, setChecks] = useState<any[]>([]);
    const [events, setEvents] = useState<any[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [isRunCheckOpen, setIsRunCheckOpen] = useState(false);
    const [refreshKey, setRefreshKey] = useState(0);

    const [scanResults, setScanResults] = useState<any[] | null>(null);

    useEffect(() => {
        if (!column) {
            setDetails(null);
            return;
        }

        const fetchData = async () => {
            setIsLoading(true);
            try {
                // Fetch Details
                const detailsRes = await fetch(
                    `/api/v1/assets/${encodeURIComponent(table)}/columns/${encodeURIComponent(column)}?database=${encodeURIComponent(database)}&schema=${encodeURIComponent(schema)}&ts=${Date.now()}`
                );
                if (detailsRes.ok) {
                    const detailsJson = await detailsRes.json();
                    if (detailsJson.success) setDetails(detailsJson.data);
                } else {
                    console.error("Failed to fetch details:", detailsRes.status, detailsRes.statusText);
                }

                // Fetch Checks
                const checksRes = await fetch(
                    `/api/v1/assets/${encodeURIComponent(table)}/columns/${encodeURIComponent(column)}/checks?database=${encodeURIComponent(database)}&schema=${encodeURIComponent(schema)}`
                );
                if (checksRes.ok) {
                    const checksJson = await checksRes.json();
                    if (checksJson.success) setChecks(checksJson.data);
                }

                // Fetch Events
                const eventsRes = await fetch(
                    `/api/v1/assets/${encodeURIComponent(table)}/columns/${encodeURIComponent(column)}/events?database=${encodeURIComponent(database)}&schema=${encodeURIComponent(schema)}`
                );
                if (eventsRes.ok) {
                    const eventsJson = await eventsRes.json();
                    if (eventsJson.success) setEvents(eventsJson.data);
                }

            } catch (e) {
                console.error("Failed to fetch column details", e);
            } finally {
                setIsLoading(false);
            }
        };

        fetchData();
    }, [database, schema, table, column, refreshKey]);

    const handleRunCheckSuccess = (results: any[]) => {
        if (!results) return;

        // Map API results to Component format
        const mappedResults = results.map(r => {
            let row = r.daa?.[0] || {};

            // Attempt to parse any string fields that might be JSON (e.g. SP return value)
            const extractedFields: Record<string, any> = {};
            Object.keys(row).forEach(key => {
                const val = row[key];
                if (typeof val === 'string' && (val.trim().startsWith('{') || val.trim().startsWith('['))) {
                    try {
                        const parsed = JSON.parse(val);
                        if (parsed && typeof parsed === 'object') {
                            Object.assign(extractedFields, parsed);
                        }
                    } catch (e) { /* ignore */ }
                }
            });

            // Merge extracted fields into a combined details object for easy access
            const combinedDetails = { ...row, ...extractedFields };

            // Look for common Status/Error keys (case-insensitive)
            const getField = (obj: any, keys: string[]) => {
                for (const k of keys) {
                    // direct match
                    if (obj[k]) return obj[k];
                    // lowercase match search
                    const foundKey = Object.keys(obj).find(ok => ok.toUpperCase() === k);
                    if (foundKey) return obj[foundKey];
                }
                return null;
            };

            const status = getField(combinedDetails, ['STATUS', 'CHECK_STATUS']) || (r.success ? 'PASSED' : 'FAILED');
            const errorMsg = getField(combinedDetails, ['FAILURE_REASON', 'ERROR_MESSAGE', 'ERROR', 'CHECK_OUTPUT']);

            return {
                checkName: r.rule,
                status: status,
                errorMessage: errorMsg || null,
                executedAt: new Date().toISOString(),
                details: combinedDetails // Pass the enriched details
            };
        });

        setScanResults(mappedResults);
        setIsRunCheckOpen(false);
    };

    if (!column) {
        return (
            <div className="flex-1 flex flex-col items-center justify-center bg-slate-50/50 p-8 text-center h-full">
                <div className="w-16 h-16 bg-slate-100 rounded-full flex items-center justify-center mb-4">
                    <ExternalLink className="w-8 h-8 text-slate-300" />
                </div>
                <h3 className="text-lg font-medium text-slate-900">No Column Selected</h3>
                <p className="text-slate-500 max-w-sm mt-2">
                    Select a column from the list on the left to view detailed metadata, statistics, performance checks, and anomaly history.
                </p>
            </div>
        );
    }

    if (isLoading || !details) {
        return (
            <div className="flex-1 p-8">
                <div className="h-8 bg-slate-200 rounded w-1/3 mb-6 animate-pulse"></div>
                <div className="grid grid-cols-4 gap-4 mb-8">
                    {[...Array(4)].map((_, i) => (
                        <div key={i} className="h-24 bg-slate-100 rounded animate-pulse"></div>
                    ))}
                </div>
                <div className="h-64 bg-slate-50 rounded animate-pulse"></div>
            </div>
        );
    }

    // Results View - Exclusive Mode
    if (scanResults) {
        return (
            <div className="flex-1 overflow-y-auto h-full p-8 bg-white">
                <div className="flex items-center justify-between mb-8">
                    <div>
                        <h2 className="text-2xl font-bold text-slate-900">{details.metadata.columnName}</h2>
                        <p className="text-slate-500 text-sm">Scan Execution Results</p>
                    </div>
                    <Button variant="outline" onClick={() => setScanResults(null)}>
                        Back to Dashboard
                    </Button>
                </div>

                <ColumnCheckResults checks={scanResults} />
            </div>
        );
    }

    // Check if profiling is needed
    const needsProfiling = (details as any).needsProfiling === true;

    return (
        <div className="flex-1 overflow-y-auto h-full p-8 bg-white">
            <div className="flex items-center justify-between mb-8">
                <div>
                    <h2 className="text-2xl font-bold text-slate-900">{details.metadata.columnName}</h2>
                    <p className="text-slate-500 text-sm">Column Entity Profile</p>
                </div>
                <Button variant="outline" size="sm" onClick={() => setIsRunCheckOpen(true)} className="bg-indigo-50 border-indigo-200 text-indigo-700 hover:bg-indigo-100">
                    Run Check
                </Button>
            </div>

            {needsProfiling ? (
                <div className="flex flex-col items-center justify-center py-16 px-8 bg-slate-50 rounded-lg border-2 border-dashed border-slate-200">
                    <div className="w-16 h-16 bg-indigo-100 rounded-full flex items-center justify-center mb-4">
                        <BarChart3 className="w-8 h-8 text-indigo-600" />
                    </div>
                    <h3 className="text-lg font-semibold text-slate-900 mb-2">No Profiling Data Available</h3>
                    <p className="text-slate-600 text-center max-w-md mb-6">
                        This column hasn't been profiled yet. Run profiling to view detailed statistics, completeness metrics, and historical trends.
                    </p>
                    <div className="flex gap-3">
                        <Button
                            variant="default"
                            onClick={() => {
                                // Navigate to Overview tab to run profiling
                                window.location.hash = 'overview';
                            }}
                            className="bg-indigo-600 hover:bg-indigo-700">
                            Go to Overview to Run Profiling
                        </Button>
                    </div>

                    {/* Show basic metadata */}
                    <div className="mt-8 w-full max-w-md">
                        <h4 className="text-sm font-semibold text-slate-700 mb-3">Basic Column Information</h4>
                        <div className="bg-white rounded-lg border border-slate-200 p-4 space-y-2">
                            <div className="flex justify-between">
                                <span className="text-sm text-slate-500">Column Name:</span>
                                <span className="text-sm font-medium text-slate-900">{details.metadata.columnName}</span>
                            </div>
                            <div className="flex justify-between">
                                <span className="text-sm text-slate-500">Data Type:</span>
                                <span className="text-sm font-mono font-medium text-slate-900">{details.metadata.dataType}</span>
                            </div>
                            <div className="flex justify-between">
                                <span className="text-sm text-slate-500">Nullable:</span>
                                <span className="text-sm font-medium text-slate-900">{details.metadata.isNullable ? 'Yes' : 'No'}</span>
                            </div>
                        </div>
                    </div>
                </div>
            ) : (
                <div className="space-y-8">
                    <ColumnOverview metadata={details.metadata} stats={details.currentStats} />

                    <ColumnStatistics history={details.history} stats={details.currentStats} />

                    <ColumnAnomaliesDrift events={events} />
                </div>
            )}

            <RunCheckDialog
                isOpen={isRunCheckOpen}
                onClose={() => setIsRunCheckOpen(false)}
                database={database}
                schema={schema}
                table={table}
                column={column}
                onSuccess={handleRunCheckSuccess}
            />
        </div>
    );
}
