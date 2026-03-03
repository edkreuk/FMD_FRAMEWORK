// FMD Pipeline Dashboard - Pipeline Monitor View

import React, { useState, useMemo } from 'react';
import {
  Play,
  CheckCircle,
  XCircle,
  Clock,
  Filter,
  ChevronDown,
  TrendingUp,
  TrendingDown,
  Database,
  ArrowRight,
  Loader2,
  AlertCircle,
} from 'lucide-react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  Bar,
  Legend,
} from 'recharts';
import type {
  Pipeline,
  ExecutionTimeline,
  PipelineLayer,
  PipelineStatus,
} from '../types/dashboard';

// Helper functions
const formatDuration = (minutes: number): string => {
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  const mins = minutes % 60;
  return `${hours}h ${mins}m`;
};

const formatTime = (date: Date): string => {
  return date.toLocaleTimeString('en-US', {
    hour: '2-digit',
    minute: '2-digit',
  });
};

const formatNumber = (num: number): string => {
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
  if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
  return num.toString();
};

const layerColors: Record<PipelineLayer, string> = {
  ingestion: '#3b82f6',
  standardization: '#f59e0b',
  business_ready: '#8b5cf6',
  analytics: '#10b981',
};

const layerLabels: Record<PipelineLayer, string> = {
  ingestion: 'Ingestion',
  standardization: 'Standardization',
  business_ready: 'Business Ready',
  analytics: 'Analytics',
};

const statusConfig: Record<PipelineStatus, { label: string; color: string; bgColor: string; icon: React.ElementType }> = {
  success: {
    label: 'Success',
    color: 'text-emerald-600',
    bgColor: 'bg-emerald-50',
    icon: CheckCircle,
  },
  failed: {
    label: 'Failed',
    color: 'text-red-600',
    bgColor: 'bg-red-50',
    icon: XCircle,
  },
  running: {
    label: 'Running',
    color: 'text-blue-600',
    bgColor: 'bg-blue-50',
    icon: Loader2,
  },
  cancelled: {
    label: 'Cancelled',
    color: 'text-slate-500',
    bgColor: 'bg-slate-50',
    icon: AlertCircle,
  },
};

interface PipelineMonitorProps {
  activePipelines: Pipeline[];
  runHistory: Pipeline[];
  executionTimeline: ExecutionTimeline[];
  durationTrend: { date: string; duration: number }[];
  successRateTrend: { date: string; rate: number }[];
}

export const PipelineMonitor: React.FC<PipelineMonitorProps> = ({
  activePipelines,
  runHistory,
  executionTimeline,
  durationTrend,
  successRateTrend,
}) => {
  const [filters, setFilters] = useState({
    sourceSystem: 'all',
    layer: 'all',
    status: 'all',
  });
  const [showFilters, setShowFilters] = useState(false);

  const sourceSystems = useMemo(() => {
    const sources = new Set(runHistory.map((p) => p.sourceSystem));
    return ['all', ...Array.from(sources)];
  }, [runHistory]);

  const layers: string[] = ['all', 'ingestion', 'standardization', 'business_ready', 'analytics'];
  const statuses: string[] = ['all', 'success', 'failed', 'running', 'cancelled'];

  const filteredHistory = useMemo(() => {
    return runHistory.filter((pipeline) => {
      if (filters.sourceSystem !== 'all' && pipeline.sourceSystem !== filters.sourceSystem)
        return false;
      if (filters.layer !== 'all' && pipeline.layer !== filters.layer) return false;
      if (filters.status !== 'all' && pipeline.status !== filters.status) return false;
      return true;
    });
  }, [runHistory, filters]);

  // Calculate stats
  const stats = useMemo(() => {
    const total = runHistory.length;
    const successful = runHistory.filter((p) => p.status === 'success').length;
    const failed = runHistory.filter((p) => p.status === 'failed').length;
    const avgDuration = Math.round(
      runHistory.reduce((acc, p) => acc + (p.duration || 0), 0) / total
    );
    return { total, successful, failed, avgDuration };
  }, [runHistory]);

  return (
    <div className="space-y-6">
      {/* Page Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Pipeline Monitor</h1>
          <p className="text-slate-500 mt-1">
            Monitor pipeline execution and performance across all data layers
          </p>
        </div>
      </div>

      {/* Active Pipelines */}
      <div className="bg-white rounded-xl border border-slate-200 p-6">
        <h2 className="text-lg font-semibold text-slate-900 mb-4">Active Pipelines</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {activePipelines.map((pipeline) => (
            <div
              key={pipeline.id}
              className="bg-slate-50 rounded-lg p-4 border border-slate-200"
            >
              <div className="flex items-start justify-between mb-3">
                <div className="flex-1">
                  <h3 className="font-medium text-slate-900">{pipeline.name}</h3>
                  <p className="text-sm text-slate-500 mt-1">{pipeline.sourceSystem}</p>
                </div>
                <span className="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium bg-blue-50 text-blue-700">
                  <Loader2 className="w-3 h-3 mr-1 animate-spin" />
                  Running
                </span>
              </div>
              <div className="flex items-center justify-between text-sm">
                <div className="flex items-center text-slate-500">
                  <Clock className="w-4 h-4 mr-1" />
                  {formatTime(pipeline.startTime)}
                </div>
                <div className="text-slate-500">
                  {formatNumber(pipeline.recordsProcessed || 0)} records
                </div>
              </div>
              <div className="mt-3">
                <div className="flex items-center justify-between text-xs text-slate-500 mb-1">
                  <span>{layerLabels[pipeline.layer]}</span>
                  <span>In Progress</span>
                </div>
                <div className="w-full bg-slate-200 rounded-full h-2">
                  <div
                    className="bg-blue-500 h-2 rounded-full transition-all duration-500"
                    style={{ width: '68%' }}
                  ></div>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Execution Timeline */}
      <div className="bg-white rounded-xl border border-slate-200 p-6">
        <h2 className="text-lg font-semibold text-slate-900 mb-6">Execution Timeline</h2>
        <div className="flex items-center justify-between relative">
          {/* Connection Line */}
          <div className="absolute top-8 left-0 right-0 h-0.5 bg-slate-200 -z-10"></div>

          {executionTimeline.map((step, index) => {
            const isComplete = step.status === 'success';
            const isRunning = step.status === 'running';
            const StatusIcon = statusConfig[step.status].icon;

            return (
              <div key={step.layer} className="flex flex-col items-center">
                <div
                  className={`
                    w-16 h-16 rounded-full flex items-center justify-center border-4
                    ${
                      isComplete
                        ? 'bg-emerald-500 border-emerald-200'
                        : isRunning
                        ? 'bg-blue-500 border-blue-200'
                        : 'bg-slate-100 border-slate-200'
                    }
                  `}
                >
                  {isComplete ? (
                    <CheckCircle className="w-7 h-7 text-white" />
                  ) : isRunning ? (
                    <Loader2 className="w-7 h-7 text-white animate-spin" />
                  ) : (
                    <Database className="w-6 h-6 text-slate-400" />
                  )}
                </div>
                <div className="mt-3 text-center">
                  <p
                    className={`text-sm font-medium ${
                      isComplete || isRunning ? 'text-slate-900' : 'text-slate-400'
                    }`}
                  >
                    {layerLabels[step.layer]}
                  </p>
                  {isRunning && (
                    <p className="text-xs text-blue-600 mt-1">{step.progress}% complete</p>
                  )}
                  {isComplete && step.endTime && (
                    <p className="text-xs text-emerald-600 mt-1">Completed</p>
                  )}
                </div>
                {index < executionTimeline.length - 1 && (
                  <ArrowRight className="absolute top-8 text-slate-300 w-5 h-5" style={{ left: `calc(${(index + 0.5) * 25}% - 10px)` }} />
                )}
              </div>
            );
          })}
        </div>
      </div>

      {/* Filters */}
      <div className="bg-white rounded-xl border border-slate-200 p-4">
        <div className="flex items-center justify-between">
          <button
            onClick={() => setShowFilters(!showFilters)}
            className="flex items-center text-sm font-medium text-slate-700 hover:text-slate-900"
          >
            <Filter className="w-4 h-4 mr-2" />
            Filters
            <ChevronDown
              className={`w-4 h-4 ml-1 transition-transform ${showFilters ? 'rotate-180' : ''}`}
            />
          </button>
          <p className="text-sm text-slate-500">
            Showing {filteredHistory.length} of {runHistory.length} runs
          </p>
        </div>

        {showFilters && (
          <div className="mt-4 pt-4 border-t border-slate-200 grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-1">
                Source System
              </label>
              <select
                value={filters.sourceSystem}
                onChange={(e) => setFilters({ ...filters, sourceSystem: e.target.value })}
                className="w-full px-3 py-2 border border-slate-300 rounded-lg text-sm focus:ring-2 focus:ring-emerald-500 focus:border-emerald-500"
              >
                {sourceSystems.map((source) => (
                  <option key={source} value={source}>
                    {source === 'all' ? 'All Sources' : source}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-1">Layer</label>
              <select
                value={filters.layer}
                onChange={(e) => setFilters({ ...filters, layer: e.target.value })}
                className="w-full px-3 py-2 border border-slate-300 rounded-lg text-sm focus:ring-2 focus:ring-emerald-500 focus:border-emerald-500"
              >
                {layers.map((layer) => (
                  <option key={layer} value={layer}>
                    {layer === 'all' ? 'All Layers' : layerLabels[layer as PipelineLayer]}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-1">Status</label>
              <select
                value={filters.status}
                onChange={(e) => setFilters({ ...filters, status: e.target.value })}
                className="w-full px-3 py-2 border border-slate-300 rounded-lg text-sm focus:ring-2 focus:ring-emerald-500 focus:border-emerald-500"
              >
                {statuses.map((status) => (
                  <option key={status} value={status}>
                    {status === 'all' ? 'All Statuses' : statusConfig[status as PipelineStatus].label}
                  </option>
                ))}
              </select>
            </div>
          </div>
        )}
      </div>

      {/* Run History Table */}
      <div className="bg-white rounded-xl border border-slate-200 overflow-hidden">
        <div className="p-6 border-b border-slate-200">
          <h2 className="text-lg font-semibold text-slate-900">Run History</h2>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-slate-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                  Pipeline Name
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                  Source System
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                  Layer
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                  Start Time
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                  Duration
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                  Records
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                  Status
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-200">
              {filteredHistory.slice(0, 15).map((pipeline) => {
                const status = statusConfig[pipeline.status];
                const StatusIcon = status.icon;

                return (
                  <tr key={pipeline.id} className="hover:bg-slate-50">
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="text-sm font-medium text-slate-900">{pipeline.name}</div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="text-sm text-slate-600">{pipeline.sourceSystem}</div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <span
                        className="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium"
                        style={{
                          backgroundColor: `${layerColors[pipeline.layer]}15`,
                          color: layerColors[pipeline.layer],
                        }}
                      >
                        {layerLabels[pipeline.layer]}
                      </span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="text-sm text-slate-600">
                        {pipeline.startTime.toLocaleDateString('en-US', {
                          month: 'short',
                          day: 'numeric',
                          hour: '2-digit',
                          minute: '2-digit',
                        })}
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="text-sm text-slate-600">
                        {pipeline.duration ? formatDuration(pipeline.duration) : '-'}
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="text-sm text-slate-600">
                        {pipeline.recordsProcessed ? formatNumber(pipeline.recordsProcessed) : '-'}
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <span
                        className={`inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium ${status.bgColor} ${status.color}`}
                      >
                        <StatusIcon className="w-3 h-3 mr-1" />
                        {status.label}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        {filteredHistory.length > 15 && (
          <div className="px-6 py-4 border-t border-slate-200 text-center">
            <button className="text-sm text-emerald-600 hover:text-emerald-700 font-medium">
              View All {filteredHistory.length} Runs
            </button>
          </div>
        )}
      </div>

      {/* Trend Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Duration Trend */}
        <div className="bg-white rounded-xl border border-slate-200 p-6">
          <h3 className="text-lg font-semibold text-slate-900 mb-4">Duration Trend</h3>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={durationTrend}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis
                  dataKey="date"
                  tick={{ fontSize: 12, fill: '#64748b' }}
                  tickLine={false}
                  axisLine={{ stroke: '#e2e8f0' }}
                />
                <YAxis
                  tick={{ fontSize: 12, fill: '#64748b' }}
                  tickLine={false}
                  axisLine={{ stroke: '#e2e8f0' }}
                  label={{ value: 'Minutes', angle: -90, position: 'insideLeft', fill: '#64748b' }}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: '#fff',
                    border: '1px solid #e2e8f0',
                    borderRadius: '8px',
                    boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                  }}
                />
                <Line
                  type="monotone"
                  dataKey="duration"
                  stroke="#3b82f6"
                  strokeWidth={2}
                  dot={false}
                  activeDot={{ r: 6, fill: '#3b82f6' }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Success Rate Trend */}
        <div className="bg-white rounded-xl border border-slate-200 p-6">
          <h3 className="text-lg font-semibold text-slate-900 mb-4">Success Rate Trend</h3>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={successRateTrend}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis
                  dataKey="date"
                  tick={{ fontSize: 12, fill: '#64748b' }}
                  tickLine={false}
                  axisLine={{ stroke: '#e2e8f0' }}
                />
                <YAxis
                  tick={{ fontSize: 12, fill: '#64748b' }}
                  tickLine={false}
                  axisLine={{ stroke: '#e2e8f0' }}
                  domain={[80, 100]}
                  label={{ value: 'Success Rate %', angle: -90, position: 'insideLeft', fill: '#64748b' }}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: '#fff',
                    border: '1px solid #e2e8f0',
                    borderRadius: '8px',
                    boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                  }}
                  formatter={(value: number) => [`${value}%`, 'Success Rate']}
                />
                <Bar dataKey="rate" fill="#10b981" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  );
};

export default PipelineMonitor;
