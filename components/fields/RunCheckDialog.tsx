'use client';

import { useState, useEffect } from 'react';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter, DialogDescription } from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Loader2, CheckCircle2, AlertCircle } from 'lucide-react';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';

interface Rule {
    rule_id: number;
    rule_name: string;
    rule_type: string;
    rule_level: string;
    description: string;
}

interface RunCheckDialogProps {
    isOpen: boolean;
    onClose: () => void;
    database: string;
    schema: string;
    table: string;
    column: string;
    onSuccess?: (results: any[]) => void;
}

export function RunCheckDialog({ isOpen, onClose, database, schema, table, column, onSuccess }: RunCheckDialogProps) {
    const [rules, setRules] = useState<Rule[]>([]);
    const [selectedRules, setSelectedRules] = useState<number[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [isScanning, setIsScanning] = useState(false);
    const [error, setError] = useState<string | null>(null);

    // Fetch rules when dialog opens
    useEffect(() => {
        if (isOpen) {
            fetchRules();
            setSelectedRules([]); // Reset selection
            setError(null);
        }
    }, [isOpen]);

    const fetchRules = async () => {
        setIsLoading(true);
        try {
            // Rules are always in DATA_QUALITY_DB.DQ_CONFIG, regardless of the target table's database
            const res = await fetch(`/api/dq/rules?dqDatabase=DATA_QUALITY_DB&dqSchema=DQ_CONFIG`);
            const json = await res.json();

            if (json.success) {
                // Filter for Column level rules if possible, but for now show all or filter if 'COLUMN' is present
                // Assuming 'COLUMN' string for level.
                const columnRules = json.data.filter((r: Rule) =>
                    !r.rule_level || r.rule_level.toUpperCase() === 'COLUMN'
                );
                setRules(columnRules.length > 0 ? columnRules : json.data);
            } else {
                setError(json.error || 'Failed to load rules');
            }
        } catch (e) {
            setError('Failed to fetch rules');
        } finally {
            setIsLoading(false);
        }
    };

    const handleToggleRule = (id: number) => {
        setSelectedRules(prev =>
            prev.includes(id) ? prev.filter(r => r !== id) : [...prev, id]
        );
    };

    const handleRunScan = async () => {
        if (selectedRules.length === 0) return;

        setIsScanning(true);
        setError(null);

        try {
            // Map IDs to Names
            const selectedRuleNames = rules
                .filter(r => selectedRules.includes(r.rule_id))
                .map(r => r.rule_name);

            const payload = {
                dataset_id: `${database}.${schema}.${table}`,
                database,
                schema,
                table,
                column,
                rule_names: selectedRuleNames
            };

            const res = await fetch('/api/dq/run-custom-scan', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            const json = await res.json();

            if (json.success) {
                if (onSuccess) onSuccess(json.data.results);
                onClose(); // Close dialog to show results in main panel
            } else {
                setError(json.error || 'Scan failed to start');
            }
        } catch (e) {
            setError('Failed to execute scan');
        } finally {
            setIsScanning(false);
        }
    };

    return (
        <Dialog open={isOpen} onOpenChange={onClose}>
            <DialogContent className="sm:max-w-[600px] bg-white">
                <DialogHeader>
                    <DialogTitle>Run Profiling & Checks</DialogTitle>
                    <DialogDescription>
                        Select validation rules to run on column <span className="font-mono text-indigo-600 font-bold">{column}</span>
                    </DialogDescription>
                </DialogHeader>

                <div className="py-4">
                    {isLoading ? (
                        <div className="flex justify-center py-8">
                            <Loader2 className="w-8 h-8 animate-spin text-slate-400" />
                        </div>
                    ) : error ? (
                        <div className="flex items-center gap-2 p-4 text-red-600 bg-red-50 rounded-md">
                            <AlertCircle className="w-5 h-5" />
                            <p className="text-sm">{error}</p>
                        </div>
                    ) : rules.length === 0 ? (
                        <div className="text-center py-8 text-slate-500">
                            No applicable rules found for this column.
                        </div>
                    ) : (
                        <div className="max-h-[300px] overflow-y-auto space-y-2 pr-2">
                            {rules.map(rule => (
                                <div
                                    key={rule.rule_id}
                                    className={`
                                        flex items-start gap-3 p-3 rounded-lg border cursor-pointer transition-all
                                        ${selectedRules.includes(rule.rule_id)
                                            ? 'border-indigo-500 bg-indigo-50/50'
                                            : 'border-slate-200 hover:border-indigo-200 hover:bg-slate-50'}
                                    `}
                                    onClick={() => handleToggleRule(rule.rule_id)}
                                >
                                    <div className={`mt-0.5 w-4 h-4 rounded border flex items-center justify-center transition-colors ${selectedRules.includes(rule.rule_id) ? 'bg-indigo-600 border-indigo-600' : 'border-slate-300 bg-white'}`}>
                                        {selectedRules.includes(rule.rule_id) && <CheckCircle2 className="w-3 h-3 text-white" />}
                                    </div>
                                    <div className="flex-1">
                                        <div className="flex items-center justify-between mb-1">
                                            <span className="font-medium text-sm text-slate-900">{rule.rule_name}</span>
                                            <Badge variant="outline" className="text-[10px] h-5">{rule.rule_type}</Badge>
                                        </div>
                                        <p className="text-xs text-slate-500">{rule.description}</p>
                                    </div>
                                </div>
                            ))}
                        </div>
                    )}
                </div>

                <DialogFooter className="flex justify-between items-center sm:justify-between">
                    <div className="text-xs text-slate-500">
                        {selectedRules.length} rules selected
                    </div>
                    <div className="flex gap-2">
                        <Button variant="outline" onClick={onClose} disabled={isScanning}>Cancel</Button>
                        <Button
                            onClick={handleRunScan}
                            disabled={isScanning || selectedRules.length === 0}
                            className="bg-indigo-600 hover:bg-indigo-700 text-white"
                        >
                            {isScanning ? (
                                <>
                                    <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                                    Starting Scan...
                                </>
                            ) : (
                                'Run Scan'
                            )}
                        </Button>
                    </div>
                </DialogFooter>
            </DialogContent>
        </Dialog>
    );
}
