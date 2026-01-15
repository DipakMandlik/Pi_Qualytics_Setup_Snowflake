"use client";

import { useEffect, useState } from "react";
import { Navbar } from "@/components/Navbar";
import { SlicersPanel } from "@/components/SlicersPanel";
import { GaugeChart } from "@/components/GaugeChart";
import { TotalChecksCard } from "@/components/TotalChecksCard";
import { FailedChecksCard } from "@/components/FailedChecksCard";
import { SlaComplianceCard } from "@/components/SlaComplianceCard";
import { DataTrustLevelCard } from "@/components/DataTrustLevelCard";
import { CriticalFailedRecordsCard } from "@/components/CriticalFailedRecordsCard";
import { Card, CardContent } from "@/components/ui/card";
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  BarChart,
  Bar,
  Cell,
  PieChart,
  Pie,
  Legend,
} from "recharts";
import { LineChart as LineChartIcon, Table as TableIcon } from "lucide-react";
import { useAppStore } from "@/lib/store";

// Colors for pie chart
const COLORS = ['#6366f1', '#f59e0b', '#10b981', '#06b6d4', '#8b5cf6', '#ec4899'];

// Custom label renderer for pie chart
const RADIAN = Math.PI / 180;
const renderCustomizedLabel = ({ cx, cy, midAngle, innerRadius, outerRadius, percent }: any) => {
  const radius = innerRadius + (outerRadius - innerRadius) * 0.5;
  const x = cx + radius * Math.cos(-midAngle * RADIAN);
  const y = cy + radius * Math.sin(-midAngle * RADIAN);

  // Don't show label for very small slices to avoid clutter
  if (percent < 0.05) return null;

  return (
    <text x={x} y={y} fill="white" textAnchor="middle" dominantBaseline="central" className="text-xs font-bold pointer-events-none">
      {`${(percent * 100).toFixed(0)}%`}
    </text>
  );
};

export default function Home() {
  const { snowflakeConfig, setIsConnected, isConnected } = useAppStore();
  const [isCheckingConnection, setIsCheckingConnection] = useState(true);
  const [dqScore, setDqScore] = useState<number>(0);
  const [scoreDiff, setScoreDiff] = useState<number | undefined>(undefined);
  const [isLoadingScore, setIsLoadingScore] = useState(false);
  const [totalChecks, setTotalChecks] = useState<number>(0);
  const [isLoadingChecks, setIsLoadingChecks] = useState(false);
  const [totalFailedChecks, setTotalFailedChecks] = useState<number>(0);
  const [isLoadingFailedChecks, setIsLoadingFailedChecks] = useState(false);
  const [failedChecksDiff, setFailedChecksDiff] = useState<number | undefined>(
    undefined
  );
  const [slaCompliancePct, setSlaCompliancePct] = useState<number>(0);
  const [isLoadingSlaCompliance, setIsLoadingSlaCompliance] = useState(false);
  const [trustLevel, setTrustLevel] = useState<string>("Unknown");
  const [isLoadingTrustLevel, setIsLoadingTrustLevel] = useState(false);
  const [criticalFailedRecords, setCriticalFailedRecords] = useState<number>(0);
  const [isLoadingCriticalFailedRecords, setIsLoadingCriticalFailedRecords] =
    useState(false);
  const [scoreByDatasetData, setScoreByDatasetData] = useState<
    Array<{ name: string; score: number }>
  >([]);
  const [isLoadingScoreByDataset, setIsLoadingScoreByDataset] = useState(false);
  const [ruleTypeData, setRuleTypeData] = useState<
    Array<{ name: string; failures: number }>
  >([]);
  const [isLoadingRuleTypeData, setIsLoadingRuleTypeData] = useState(false);
  const [slaComplianceData, setSlaComplianceData] = useState<
    Array<{ name: string; score: number; slaTarget: number; status: string }>
  >([]);
  const [isLoadingSlaComplianceData, setIsLoadingSlaComplianceData] =
    useState(false);
  const [dqScoreTrendData, setDqScoreTrendData] = useState<
    Array<{ date: string; dq_score: number }>
  >([]);
  const [isLoadingDqScoreTrend, setIsLoadingDqScoreTrend] = useState(false);
  const [showTrendTable, setShowTrendTable] = useState(false);
  useEffect(() => {
    // Check if there's a server-side connection on mount
    const checkConnection = async () => {
      try {
        const response = await fetch("/api/snowflake/status");
        const data = await response.json();

        if (data.success && data.isConnected) {
          setIsConnected(true);
        } else if (snowflakeConfig) {
          // If we have local config but no server connection, try to reconnect
          // This handles page refreshes
          try {
            const connectResponse = await fetch("/api/snowflake/connect", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(snowflakeConfig),
            });
            const connectData = await connectResponse.json();
            if (connectData.success) {
              setIsConnected(true);
            }
          } catch (error) {
            // Silent fail - user can reconnect manually
          }
        }
      } catch (error) {
        // Silent fail - user can connect manually
      } finally {
        setIsCheckingConnection(false);
      }
    };

    checkConnection();
  }, [snowflakeConfig, setIsConnected]);

  // Fetch data quality score when connected
  useEffect(() => {
    if (!isConnected) {
      setDqScore(0);
      setScoreDiff(undefined);
      return;
    }

    const fetchDQScore = async () => {
      setIsLoadingScore(true);
      try {
        const response = await fetch("/api/dq/overall-score");
        const data = await response.json();

        if (data.success && data.data) {
          const overallScore = data.data.overallScore;
          const scoreDifference = data.data.scoreDifference;
          const previousScore = data.data.previousScore;

          if (overallScore !== null && overallScore !== undefined) {
            // DQ_SCORE might be 0-100 or 0-1, normalize to 0-100
            let currentScore = overallScore;
            if (currentScore > 0 && currentScore <= 1) {
              currentScore = currentScore * 100;
            }
            setDqScore(Math.round(currentScore));

            // Set score difference if available, otherwise use static value
            if (scoreDifference !== null && scoreDifference !== undefined) {
              setScoreDiff(Math.round(scoreDifference * 10) / 10); // One decimal place
            } else if (previousScore === null) {
              // Use static previous score of 75 when previousScore is null
              const staticPreviousScore = 75;
              const diff = currentScore - staticPreviousScore;
              setScoreDiff(Math.round(diff * 10) / 10); // One decimal place
            }
          } else {
            setDqScore(0);
            setScoreDiff(undefined);
          }
        }
      } catch (error) {
        console.error("Error fetching DQ score:", error);
        setDqScore(0);
        setScoreDiff(undefined);
      } finally {
        setIsLoadingScore(false);
      }
    };

    fetchDQScore();
  }, [isConnected]);

  // Fetch total checks when connected
  useEffect(() => {
    if (!isConnected) {
      setTotalChecks(0);
      return;
    }

    const fetchTotalChecks = async () => {
      setIsLoadingChecks(true);
      try {
        const response = await fetch("/api/dq/total-checks");
        const data = await response.json();

        if (data.success && data.data) {
          setTotalChecks(data.data.totalChecks || 0);
        }
      } catch (error) {
        console.error("Error fetching total checks:", error);
        setTotalChecks(0);
      } finally {
        setIsLoadingChecks(false);
      }
    };

    fetchTotalChecks();
  }, [isConnected]);

  // Fetch failed checks when connected
  useEffect(() => {
    if (!isConnected) {
      setTotalFailedChecks(0);
      return;
    }

    const fetchFailedChecks = async () => {
      setIsLoadingFailedChecks(true);
      try {
        const response = await fetch("/api/dq/failed-checks");
        const data = await response.json();

        if (data.success && data.data) {
          setTotalFailedChecks(data.data.totalFailedChecks || 0);
          setFailedChecksDiff(data.data.failedChecksDifference);
        }
      } catch (error) {
        console.error("Error fetching failed checks:", error);
        setTotalFailedChecks(0);
        setFailedChecksDiff(undefined);
      } finally {
        setIsLoadingFailedChecks(false);
      }
    };

    fetchFailedChecks();
  }, [isConnected]);

  // Fetch SLA compliance when connected
  useEffect(() => {
    if (!isConnected) {
      setSlaCompliancePct(0);
      return;
    }

    const fetchSlaCompliance = async () => {
      setIsLoadingSlaCompliance(true);
      try {
        const response = await fetch("/api/dq/sla-compliance");
        const data = await response.json();

        if (data.success && data.data) {
          setSlaCompliancePct(data.data.slaCompliancePct || 0);
        }
      } catch (error) {
        console.error("Error fetching SLA compliance:", error);
        setSlaCompliancePct(0);
      } finally {
        setIsLoadingSlaCompliance(false);
      }
    };

    fetchSlaCompliance();
  }, [isConnected]);

  // Fetch data trust level when connected
  useEffect(() => {
    if (!isConnected) {
      setTrustLevel("Unknown");
      return;
    }

    const fetchTrustLevel = async () => {
      setIsLoadingTrustLevel(true);
      try {
        const response = await fetch("/api/dq/data-trust-level");
        const data = await response.json();

        if (data.success && data.data) {
          setTrustLevel(data.data.trustLevel || "Unknown");
        }
      } catch (error) {
        console.error("Error fetching data trust level:", error);
        setTrustLevel("Unknown");
      } finally {
        setIsLoadingTrustLevel(false);
      }
    };

    fetchTrustLevel();
  }, [isConnected]);

  // Fetch critical failed records when connected
  useEffect(() => {
    if (!isConnected) {
      setCriticalFailedRecords(0);
      return;
    }

    const fetchCriticalFailedRecords = async () => {
      setIsLoadingCriticalFailedRecords(true);
      try {
        const response = await fetch("/api/dq/critical-failed-records");
        const data = await response.json();

        if (data.success && data.data) {
          setCriticalFailedRecords(data.data.criticalFailedRecords || 0);
        }
      } catch (error) {
        console.error("Error fetching critical failed records:", error);
        setCriticalFailedRecords(0);
      } finally {
        setIsLoadingCriticalFailedRecords(false);
      }
    };

    fetchCriticalFailedRecords();
  }, [isConnected]);

  // Fetch score by dataset when connected
  useEffect(() => {
    if (!isConnected) {
      setScoreByDatasetData([]);
      return;
    }

    const fetchScoreByDataset = async () => {
      setIsLoadingScoreByDataset(true);
      try {
        const response = await fetch("/api/dq/score-by-dataset");
        const data = await response.json();

        if (data.success && data.data) {
          setScoreByDatasetData(data.data.datasets || []);
        }
      } catch (error) {
        console.error("Error fetching score by dataset:", error);
        setScoreByDatasetData([]);
      } finally {
        setIsLoadingScoreByDataset(false);
      }
    };

    fetchScoreByDataset();
  }, [isConnected]);

  // Fetch failures by rule type when connected
  useEffect(() => {
    if (!isConnected) {
      setRuleTypeData([]);
      return;
    }

    const fetchRuleTypeData = async () => {
      setIsLoadingRuleTypeData(true);
      try {
        const response = await fetch("/api/dq/failures-by-rule-type");
        const data = await response.json();

        if (data.success && data.data) {
          setRuleTypeData(data.data.ruleTypes || []);
        }
      } catch (error) {
        console.error("Error fetching failures by rule type:", error);
        setRuleTypeData([]);
      } finally {
        setIsLoadingRuleTypeData(false);
      }
    };

    fetchRuleTypeData();
  }, [isConnected]);

  // Fetch SLA compliance data when connected
  useEffect(() => {
    if (!isConnected) {
      setSlaComplianceData([]);
      return;
    }

    const fetchSlaComplianceData = async () => {
      setIsLoadingSlaComplianceData(true);
      try {
        const response = await fetch("/api/dq/sla-compliance-monitor");
        const data = await response.json();

        if (data.success && data.data) {
          setSlaComplianceData(data.data.slaCompliance || []);
        }
      } catch (error) {
        console.error("Error fetching SLA compliance data:", error);
        setSlaComplianceData([]);
      } finally {
        setIsLoadingSlaComplianceData(false);
      }
    };

    fetchSlaComplianceData();
  }, [isConnected]);

  // Fetch DQ score trend data when connected
  useEffect(() => {
    if (!isConnected) {
      setDqScoreTrendData([]);
      return;
    }

    const fetchDqScoreTrend = async () => {
      setIsLoadingDqScoreTrend(true);
      try {
        const response = await fetch("/api/dq/daily-summary");
        const data = await response.json();

        if (data.success && data.data) {
          // Format the data for the chart
          const formattedData = data.data.map((row: any) => ({
            date: row.SUMMARY_DATE,
            dq_score: row.DQ_SCORE,
          }));
          setDqScoreTrendData(formattedData);
        }
      } catch (error) {
        console.error("Error fetching DQ score trend:", error);
        setDqScoreTrendData([]);
      } finally {
        setIsLoadingDqScoreTrend(false);
      }
    };

    fetchDqScoreTrend();
  }, [isConnected]);

  return (
    <div className="min-h-screen bg-slate-50 dark:bg-slate-900 flex flex-col">
      <Navbar />
      <div className="flex flex-1 overflow-hidden">
        <SlicersPanel />
        <div className="flex-1 overflow-y-auto">
          <div className="container mx-auto px-4 py-8">
            <h2 className="text-2xl font-semibold text-slate-900 dark:text-slate-100 mb-6">
              Data Quality Management Dashboard
            </h2>

            {/* Dashboard Cards Grid */}
            {/* Dashboard Cards Grid */}
            <div
              className="
                      grid 
                      grid-cols-4 
                      grid-rows-2 
                      gap-4
                      items-stretch
                    "
            >
              {/* Column 1: Data Quality Score (spans 2 rows) */}
              <Card className="row-span-2 border-l-4 border-l-blue-500 bg-white shadow-sm">
                <CardContent className="p-4 flex items-center justify-center h-full">
                  {isConnected && !isLoadingScore ? (
                    <GaugeChart
                      value={dqScore}
                      label="Data Quality Score"
                      change={scoreDiff}
                    />
                  ) : (
                    <div className="flex flex-col items-center justify-center text-gray-400">
                      <p className="text-sm">
                        Connect to Snowflake to view Data Quality Score
                      </p>
                    </div>
                  )}
                </CardContent>
              </Card>

              {/* Row 1 - Card 1: Total Checks */}
              {isConnected ? (
                <TotalChecksCard
                  totalChecks={totalChecks}
                  isLoading={isLoadingChecks}
                />
              ) : (
                <Card className="border-l-4 border-l-purple-500 bg-white shadow-sm">
                  <CardContent className="p-6 flex items-center justify-center h-full">
                    <p className="text-sm text-gray-400">
                      Connect to Snowflake to view Total Checks
                    </p>
                  </CardContent>
                </Card>
              )}

              {/* Row 1 - Card 2: Failed Checks */}
              {isConnected ? (
                <FailedChecksCard
                  totalFailedChecks={totalFailedChecks}
                  isLoading={isLoadingFailedChecks}
                  change={failedChecksDiff}
                />
              ) : (
                <Card className="border-l-4 border-l-red-500 bg-white shadow-sm">
                  <CardContent className="p-6 flex items-center justify-center h-full">
                    <p className="text-sm text-gray-400">
                      Connect to Snowflake to view Failed Checks
                    </p>
                  </CardContent>
                </Card>
              )}

              {/* Row 1 - Card 3: SLA Compliance */}
              {isConnected ? (
                <SlaComplianceCard
                  slaCompliancePct={slaCompliancePct}
                  isLoading={isLoadingSlaCompliance}
                />
              ) : (
                <Card className="border-l-4 border-l-green-500 bg-white shadow-sm">
                  <CardContent className="p-6 flex items-center justify-center h-full">
                    <p className="text-sm text-gray-400">
                      Connect to Snowflake to view SLA Compliance
                    </p>
                  </CardContent>
                </Card>
              )}

              {/* Row 2 - Card 4: Data Trust Level */}
              {isConnected ? (
                <DataTrustLevelCard
                  trustLevel={trustLevel}
                  isLoading={isLoadingTrustLevel}
                />
              ) : (
                <Card className="border-l-4 border-l-yellow-500 bg-white shadow-sm">
                  <CardContent className="p-6 flex items-center justify-center h-full">
                    <p className="text-sm text-gray-400">
                      Connect to Snowflake to view Data Trust Level
                    </p>
                  </CardContent>
                </Card>
              )}

              {/* Row 2 - Card 5: Critical Failed Records */}
              {isConnected ? (
                <CriticalFailedRecordsCard
                  criticalFailedRecords={criticalFailedRecords}
                  isLoading={isLoadingCriticalFailedRecords}
                />
              ) : (
                <Card className="border-l-4 border-l-orange-500 bg-white shadow-sm">
                  <CardContent className="p-6 flex items-center justify-center h-full">
                    <p className="text-sm text-gray-400">
                      Connect to Snowflake to view Critical Failed Records
                    </p>
                  </CardContent>
                </Card>
              )}

              {/* Row 2 - Card 6 (EMPTY SLOT / FUTURE KPI) */}
              <Card className="bg-slate-50 border-dashed border-2 border-slate-200">
                <CardContent className="p-6 flex items-center justify-center h-full text-slate-400">
                  Future KPI
                </CardContent>
              </Card>
            </div>

            {/* ROW 2 */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mt-6">
              {/* Data Quality Score Trend */}
              <Card className="col-span-1 p-6">
                <div className="flex justify-between items-center mb-4">
                  <h3 className="text-lg font-bold text-slate-800">
                    DQ Score Trend (30 Days)
                  </h3>
                  <button
                    onClick={() => setShowTrendTable(!showTrendTable)}
                    className="p-1.5 text-slate-500 hover:text-slate-700 hover:bg-slate-100 rounded-md transition-colors"
                    title={showTrendTable ? "Switch to Chart View" : "Switch to Table View"}
                  >
                    {showTrendTable ? <LineChartIcon size={20} /> : <TableIcon size={20} />}
                  </button>
                </div>
                <div className="h-72">
                  {isLoadingDqScoreTrend ? (
                    <div className="flex items-center justify-center h-full">
                      <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div>
                    </div>
                  ) : dqScoreTrendData.length === 0 ? (
                    <div className="flex items-center justify-center h-full text-gray-400">
                      <p className="text-sm">No data available</p>
                    </div>
                  ) : showTrendTable ? (
                    <div className="h-full overflow-y-auto custom-scrollbar border border-slate-100 rounded-lg">
                      <table className="w-full text-sm text-left">
                        <thead className="bg-slate-50 sticky top-0 z-10 text-xs uppercase text-slate-500">
                          <tr>
                            <th className="px-6 py-3 font-semibold text-slate-600">Summary Date</th>
                            <th className="px-6 py-3 font-semibold text-slate-600">Score</th>
                          </tr>
                        </thead>
                        <tbody>
                          {dqScoreTrendData.map((row, i) => (
                            <tr key={i} className="border-b border-slate-100 last:border-0 hover:bg-slate-50 transition-colors">
                              <td className="px-6 py-3 text-slate-600 font-mono">{row.date}</td>
                              <td className="px-6 py-3">
                                <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-bold ${
                                  row.dq_score >= 90 ? 'bg-emerald-100 text-emerald-800' : 
                                  row.dq_score >= 75 ? 'bg-amber-100 text-amber-800' : 'bg-rose-100 text-rose-800'
                                }`}>
                                  {row.dq_score}
                                </span>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={dqScoreTrendData}>
                        <CartesianGrid
                          strokeDasharray="3 3"
                          vertical={false}
                          stroke="#e2e8f0"
                        />
                        <XAxis
                          dataKey="date"
                          tickFormatter={(v) => v.slice(5)}
                          stroke="#64748b"
                          fontSize={12}
                        />
                        <YAxis stroke="#64748b" fontSize={12} />
                        <Tooltip
                          contentStyle={{
                            borderRadius: "8px",
                            border: "none",
                            boxShadow: "0 4px 6px -1px rgb(0 0 0 / 0.1)",
                          }}
                        />
                        <Line
                          type="monotone"
                          dataKey="dq_score"
                          stroke="#4f46e5"
                          strokeWidth={3}
                          dot={{ r: 3 }}
                          activeDot={{ r: 6 }}
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  )}
                </div>
              </Card>

              {/* Score by Dataset */}
              <Card className="col-span-1 p-6">
                <h3 className="text-lg font-bold text-slate-800 mb-4">
                  Score by Dataset
                </h3>
                <div className="h-72">
                  {isLoadingScoreByDataset ? (
                    <div className="flex items-center justify-center h-full">
                      <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div>
                    </div>
                  ) : scoreByDatasetData.length === 0 ? (
                    <div className="flex items-center justify-center h-full text-gray-400">
                      <p className="text-sm">No data available</p>
                    </div>
                  ) : (
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={scoreByDatasetData} layout="vertical">
                        <CartesianGrid
                          strokeDasharray="3 3"
                          horizontal={true}
                          vertical={false}
                          stroke="#e2e8f0"
                        />
                        <XAxis type="number" domain={[0, 100]} hide />
                        <YAxis
                          dataKey="name"
                          type="category"
                          width={170}
                          stroke="#64748b"
                          fontSize={12}
                          tickFormatter={(value) =>
                            value.length > 15
                              ? `${value.substring(0, 15)}...`
                              : value
                          }
                        />
                        <Tooltip cursor={{ fill: "#f1f5f9" }} />
                        <Bar dataKey="score" radius={[0, 4, 4, 0]} barSize={20}>
                          {scoreByDatasetData.map((entry, index) => (
                            <Cell
                              key={`cell-${index}`}
                              fill={
                                entry.score >= 90
                                  ? "#10b981"
                                  : entry.score >= 80
                                  ? "#f59e0b"
                                  : entry.score >= 70
                                  ? "#f97316"
                                  : "#ef4444"
                              }
                            />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  )}
                </div>
              </Card>
            </div>

            {/* ROW 3: Rule Impact */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mt-6">
              <Card className="p-6">
                <h3 className="text-lg font-bold text-slate-800 mb-4">
                  Failures by Rule Type
                </h3>
                <div className="h-72">
                  {isLoadingRuleTypeData ? (
                    <div className="flex items-center justify-center h-full">
                      <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div>
                    </div>
                  ) : ruleTypeData.length === 0 ? (
                    <div className="flex items-center justify-center h-full text-gray-400">
                      <p className="text-sm">No data available</p>
                    </div>
                  ) : (
                    <ResponsiveContainer
                      key={ruleTypeData.length}
                      width="100%"
                      height="100%"
                    >
                      <PieChart>
                        <Pie
                          data={ruleTypeData}
                          cx="50%"
                          cy="50%"
                          labelLine={false}
                          label={renderCustomizedLabel}
                          outerRadius={80}
                          dataKey="failures"
                        >
                          {ruleTypeData.map((entry, index) => (
                            <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                          ))}
                        </Pie>
                        <Tooltip />
                        <Legend verticalAlign="middle" align="right" layout="vertical" iconType="circle" />
                      </PieChart>
                    </ResponsiveContainer>
                  )}
                </div>
              </Card>

              {/* SLA Compliance Monitor */}
              <Card className="p-6 overflow-hidden">
                <h3 className="text-lg font-bold text-slate-800 mb-4">
                  SLA Compliance Monitor
                </h3>
                <div className="overflow-x-auto">
                  {isLoadingSlaComplianceData ? (
                    <div className="flex items-center justify-center h-32">
                      <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div>
                    </div>
                  ) : slaComplianceData.length === 0 ? (
                    <div className="flex items-center justify-center h-32 text-gray-400">
                      <p className="text-sm">No data available</p>
                    </div>
                  ) : (
                    <table className="w-full text-sm text-left">
                      <thead className="text-xs text-slate-500 uppercase bg-slate-50">
                        <tr>
                          <th className="px-4 py-3">Dataset</th>
                          <th className="px-4 py-3">Score</th>
                          <th className="px-4 py-3">SLA Target</th>
                          <th className="px-4 py-3">Status</th>
                        </tr>
                      </thead>
                      <tbody>
                        {slaComplianceData.map((row, i) => (
                          <tr
                            key={i}
                            className="border-b border-slate-100 last:border-0 hover:bg-slate-50"
                          >
                            <td className="px-4 py-3 font-medium text-slate-700">
                              {row.name}
                            </td>
                            <td className="px-4 py-3">{row.score}%</td>
                            <td className="px-4 py-3 text-slate-500">
                              {row.slaTarget}%
                            </td>
                            <td className="px-4 py-3">
                              <span
                                className={`px-2 py-1 rounded-full text-xs font-semibold ${
                                  row.status === "Met"
                                    ? "bg-emerald-100 text-emerald-700"
                                    : "bg-rose-100 text-rose-700"
                                }`}
                              >
                                {row.status}
                              </span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>
              </Card>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
