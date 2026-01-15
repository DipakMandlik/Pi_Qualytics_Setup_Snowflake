'use client';

import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { CheckCircle2, XCircle, AlertTriangle, Clock } from 'lucide-react';
import { format } from 'date-fns';
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table";

interface CheckResult {
    checkName: string;
    status: string; // 'PASSED' | 'FAILED' | 'WARNING'
    errorMessage: string | null;
    executedAt: string;
    details?: Record<string, any>;
}

interface ColumnCheckResultsProps {
    checks: CheckResult[];
}

export function ColumnCheckResults({ checks }: ColumnCheckResultsProps) {
    if (!checks || checks.length === 0) {
        return null;
    }

    return (
        <div className="space-y-4">
            {checks.map((check, idx) => {
                // Filter out keys already displayed or internal
                const detailKeys = check.details
                    ? Object.keys(check.details).filter(k =>
                        !['STATUS', 'FAILURE_REASON', 'CHECK_OUTPUT', 'RULE_NAME', 'EXECUTED_AT'].includes(k.toUpperCase())
                    )
                    : [];

                return (
                    <Card key={idx} className="border-slate-200 shadow-sm p-6 transition-all hover:shadow-md">
                        <div className="flex flex-col gap-4">
                            {/* Header: Rule Name and Status */}
                            <div className="flex items-start justify-between">
                                <h3 className="text-xl font-bold text-slate-900">{check.checkName}</h3>
                                <Badge variant="outline" className={`h-8 px-3 gap-2 text-sm font-medium ${check.status === 'PASSED' ? 'bg-emerald-50 text-emerald-700 border-emerald-200' :
                                    check.status === 'FAILED' ? 'bg-red-50 text-red-700 border-red-200' :
                                        'bg-amber-50 text-amber-700 border-amber-200'
                                    }`}>
                                    {check.status === 'PASSED' && <CheckCircle2 className="w-4 h-4" />}
                                    {check.status === 'FAILED' && <XCircle className="w-4 h-4" />}
                                    {check.status === 'WARNING' && <AlertTriangle className="w-4 h-4" />}
                                    {check.status}
                                </Badge>
                            </div>

                            {/* Body: Message / Error */}
                            <div className={`p-4 rounded-lg border ${check.status === 'PASSED'
                                ? 'bg-slate-50 border-slate-100'
                                : 'bg-red-50 border-red-100'
                                }`}>
                                {check.errorMessage ? (
                                    <div className="flex items-start gap-3">
                                        <AlertTriangle className="w-5 h-5 text-red-600 mt-0.5 shrink-0" />
                                        <div>
                                            <p className="font-semibold text-red-900 mb-1">Validation Failed</p>
                                            <p className="text-red-700 leading-relaxed">{check.errorMessage}</p>
                                        </div>
                                    </div>
                                ) : check.status === 'PASSED' ? (
                                    <div className="flex items-center gap-3">
                                        <CheckCircle2 className="w-5 h-5 text-emerald-600" />
                                        <p className="text-emerald-900 font-medium">Validation passed successfully. No anomalies found in this data sample.</p>
                                    </div>
                                ) : (
                                    <p className="text-slate-500">No details available.</p>
                                )}
                            </div>

                            {/* Extra Details Grid */}
                            {(() => {
                                const ignoreKeys = ['STATUS', 'CHECK_STATUS', 'FAILURE_REASON', 'ERROR_MESSAGE', 'ERROR', 'CHECK_OUTPUT', 'RULE_NAME', 'EXECUTED_AT', 'RULE', 'SUCCESS', 'SP_RUN_CUSTOM_RULE'];
                                const itemsToDisplay: { label: string; value: any }[] = [];

                                if (check.details) {
                                    Object.entries(check.details).forEach(([key, value]) => {
                                        // Skip ignored keys
                                        if (ignoreKeys.includes(key.toUpperCase())) return;

                                        // Skip raw JSON strings if mapped (optional optimization, but let's keep it clean)
                                        // If the value is a large JSON string that we likely parsed already, maybe skip it?
                                        // keeping it simple: just show what's passed, assuming parent handled flattening.

                                        // Skip internal large objects or arrays to avoid clutter unless specific
                                        if (typeof value === 'object' && value !== null) return;

                                        itemsToDisplay.push({ label: key, value });
                                    });
                                }

                                if (itemsToDisplay.length === 0) return null;

                                return (
                                    <div className="grid grid-cols-2 sm:grid-cols-3 gap-4 p-4 bg-slate-50/50 rounded-lg border border-slate-100">
                                        {itemsToDisplay.map((item, i) => (
                                            <div key={i} className="bg-slate-50 p-3 rounded-md border border-slate-100">
                                                <p className="text-[10px] text-slate-400 font-bold uppercase tracking-wider mb-1">
                                                    {item.label.replace(/_/g, ' ')}
                                                </p>
                                                <p className="text-sm font-semibold text-slate-700 break-all">
                                                    {item.value !== null && item.value !== undefined ? String(item.value) : '-'}
                                                </p>
                                            </div>
                                        ))}
                                    </div>
                                );
                            })()}

                            {/* Footer: Metadata */}
                            <div className="flex items-center justify-end text-sm text-slate-400 gap-2 mt-2 pt-4 border-t border-slate-100">
                                <Clock className="w-4 h-4" />
                                <span>Executed at: </span>
                                <span className="font-mono text-slate-600 font-medium">
                                    {(() => {
                                        if (!check.executedAt) return '-';
                                        const date = new Date(check.executedAt);
                                        return !isNaN(date.getTime()) ? format(date, 'MMM d, yyyy h:mm:ss a') : '-';
                                    })()}
                                </span>
                            </div>
                        </div>
                    </Card>
                );
            })}
        </div>
    );
}
