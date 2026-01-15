'use client';

import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Database, FileText, Hash, Ruler, Copy, BarChart3, AlertCircle } from 'lucide-react';
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from 'recharts';

interface ColumnOverviewProps {
    metadata: {
        columnName: string;
        dataType: string;
        isNullable: boolean;
    };
    stats: {
        rowCount: number;
        nullCount: number;
        distinctCount: number;
    } | null;
}

export function ColumnOverview({ metadata, stats }: ColumnOverviewProps) {
    if (!stats) return null;

    // --- Derived Metrics ---
    const nonNullCount = stats.rowCount - stats.nullCount;

    // Completeness: (Non-Null / Total) * 100
    const completenessPct = stats.rowCount > 0 ? (nonNullCount / stats.rowCount) * 100 : 0;

    // Uniqueness: (Distinct / Non-Null) * 100. If 0 non-nulls, uniqueness is 0.
    const uniquenessPct = nonNullCount > 0 ? (stats.distinctCount / nonNullCount) * 100 : 0;

    // Duplicate Count: Non-Null - Distinct (Simplistic proxy for redundant records)
    const duplicateCount = Math.max(0, nonNullCount - stats.distinctCount);

    // Avg Frequency: Non-Null / Distinct (Avg times a value appears)
    const avgFrequency = stats.distinctCount > 0 ? (nonNullCount / stats.distinctCount).toFixed(1) : '0';

    // --- Chart Data ---
    const completenessData = [
        { name: 'Valid', value: completenessPct, color: '#10b981' }, // Emerald-500
        { name: 'Null', value: 100 - completenessPct, color: '#e2e8f0' }, // Slate-200
    ];

    const uniquenessData = [
        { name: 'Unique', value: uniquenessPct, color: '#6366f1' }, // Indigo-500
        { name: 'Recurring', value: 100 - uniquenessPct, color: '#e2e8f0' }, // Slate-200
    ];

    return (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">

            {/* 1. Completeness Score (Business) */}
            <Card className="p-4 flex flex-col justify-between relative overflow-hidden">
                <div className="flex justify-between items-start mb-2">
                    <div>
                        <div className="text-xs text-slate-500 font-bold uppercase tracking-wider">Completeness</div>
                        <div className="text-sm text-slate-400">Valid Data Ratio</div>
                    </div>
                    <Badge variant="outline" className="bg-emerald-50 text-emerald-700 border-emerald-200">Business</Badge>
                </div>
                <div className="flex items-center gap-4">
                    <div className="h-16 w-16">
                        <ResponsiveContainer width="100%" height="100%">
                            <PieChart>
                                <Pie
                                    data={completenessData}
                                    cx="50%"
                                    cy="50%"
                                    innerRadius={20}
                                    outerRadius={30}
                                    startAngle={90}
                                    endAngle={-270}
                                    dataKey="value"
                                >
                                    {completenessData.map((entry, index) => (
                                        <Cell key={`cell-${index}`} fill={entry.color} />
                                    ))}
                                </Pie>
                            </PieChart>
                        </ResponsiveContainer>
                    </div>
                    <div>
                        <div className="text-3xl font-bold text-slate-900">{completenessPct.toFixed(1)}%</div>
                        <div className="text-xs text-slate-500">Avg. Fill Rate</div>
                    </div>
                </div>
            </Card>

            {/* 2. Uniqueness Score (Technical) */}
            <Card className="p-4 flex flex-col justify-between relative overflow-hidden">
                <div className="flex justify-between items-start mb-2">
                    <div>
                        <div className="text-xs text-slate-500 font-bold uppercase tracking-wider">Uniqueness</div>
                        <div className="text-sm text-slate-400">Distinct Ratio</div>
                    </div>
                    <Badge variant="outline" className="bg-indigo-50 text-indigo-700 border-indigo-200">Technical</Badge>
                </div>
                <div className="flex items-center gap-4">
                    <div className="h-16 w-16">
                        <ResponsiveContainer width="100%" height="100%">
                            <PieChart>
                                <Pie
                                    data={uniquenessData}
                                    cx="50%"
                                    cy="50%"
                                    innerRadius={20}
                                    outerRadius={30}
                                    startAngle={90}
                                    endAngle={-270}
                                    dataKey="value"
                                >
                                    {uniquenessData.map((entry, index) => (
                                        <Cell key={`cell-${index}`} fill={entry.color} />
                                    ))}
                                </Pie>
                            </PieChart>
                        </ResponsiveContainer>
                    </div>
                    <div>
                        <div className="text-3xl font-bold text-slate-900">{uniquenessPct.toFixed(1)}%</div>
                        <div className="text-xs text-slate-500">Cardinality</div>
                    </div>
                </div>
            </Card>

            {/* 3. Avg Frequency */}
            <MetricCard
                label="Avg. Frequency"
                value={avgFrequency}
                subtext="Repeats per Value"
                icon={<BarChart3 className="w-4 h-4 text-blue-500" />}
                bgClass="bg-blue-50"
            />

            {/* 4. Duplication Factor */}
            <MetricCard
                label="Values Duplicated"
                value={duplicateCount.toLocaleString()}
                subtext="Redundant Records"
                icon={<Copy className="w-4 h-4 text-orange-500" />}
                bgClass="bg-orange-50"
            />

            {/* 5. Data Type */}
            <Card className="p-4 flex flex-col justify-between">
                <div className="flex items-center gap-2 mb-2">
                    <div className="p-1.5 bg-slate-100 rounded-md">
                        <Database className="w-4 h-4 text-slate-600" />
                    </div>
                    <span className="text-xs text-slate-500 font-bold uppercase">Data Type</span>
                </div>
                <div>
                    <div className="text-2xl font-bold text-slate-900 font-mono tracking-tight">{metadata.dataType}</div>
                    <Badge variant="outline" className="mt-1 bg-white text-[10px] font-normal text-slate-500 border-slate-200">
                        {metadata.isNullable ? 'Nullable' : 'Not Nullable'}
                    </Badge>
                </div>
            </Card>

            {/* 6. Total Rows */}
            <MetricCard
                label="Total Rows"
                value={stats.rowCount.toLocaleString()}
                icon={<FileText className="w-4 h-4 text-slate-600" />}
                bgClass="bg-slate-100"
            />

            {/* 7. Distinct Values */}
            <MetricCard
                label="Distinct Values"
                value={stats.distinctCount.toLocaleString()}
                icon={<Hash className="w-4 h-4 text-emerald-600" />}
                bgClass="bg-emerald-50"
            />

            {/* 8. Null Count */}
            <MetricCard
                label="Null Count"
                value={stats.nullCount.toLocaleString()}
                icon={<AlertCircle className="w-4 h-4 text-amber-600" />}
                bgClass="bg-amber-50"
                valueClass={stats.nullCount > 0 ? "text-amber-600" : "text-slate-900"}
            />

        </div>
    );
}

function MetricCard({ label, value, subtext, icon, bgClass, valueClass = "text-slate-900" }: any) {
    return (
        <Card className="p-4 flex flex-col justify-between">
            <div className="flex items-center gap-2 mb-2">
                <div className={`p-1.5 rounded-md ${bgClass}`}>
                    {icon}
                </div>
                <span className="text-xs text-slate-500 font-bold uppercase">{label}</span>
            </div>
            <div>
                <div className={`text-2xl font-bold ${valueClass}`}>{value}</div>
                {subtext && <div className="text-xs text-slate-400">{subtext}</div>}
            </div>
        </Card>
    );
}
