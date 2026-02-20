// FMD Pipeline Dashboard - Error Intelligence View

import React, { useState } from 'react';
import {
  AlertCircle,
  AlertTriangle,
  Info,
  ChevronDown,
  ChevronUp,
  Server,
  Wrench,
  ExternalLink,
  Clock,
  Repeat,
  Eye,
  EyeOff,
  CheckCircle,
} from 'lucide-react';
import type { ErrorSummary, ErrorSeverity } from '../types/dashboard';

const severityConfig: Record<ErrorSeverity, { label: string; color: string; bgColor: string; borderColor: string; icon: React.ElementType }> = {
  critical: {
    label: 'Critical',
    color: 'text-red-700',
    bgColor: 'bg-red-50',
    borderColor: 'border-red-200',
    icon: AlertCircle,
  },
  warning: {
    label: 'Warning',
    color: 'text-amber-700',
    bgColor: 'bg-amber-50',
    borderColor: 'border-amber-200',
    icon: AlertTriangle,
  },
  info: {
    label: 'Info',
    color: 'text-blue-700',
    bgColor: 'bg-blue-50',
    borderColor: 'border-blue-200',
    icon: Info,
  },
};

interface ErrorIntelligenceProps {
  errors: ErrorSummary[];
  totalErrors: number;
  criticalCount: number;
  warningCount: number;
  infoCount: number;
  errorPattern: { pattern: string; count: number }[];
}

export const ErrorIntelligence: React.FC<ErrorIntelligenceProps> = ({
  errors,
  totalErrors,
  criticalCount,
  warningCount,
  infoCount,
  errorPattern,
}) => {
  const [expandedError, setExpandedError] = useState<string | null>(null);
  const [showRawError, setShowRawError] = useState<{ [key: string]: boolean }>({});
  const [filterSeverity, setFilterSeverity] = useState<ErrorSeverity | 'all'>('all');

  const filteredErrors = filterSeverity === 'all'
    ? errors
    : errors.filter(e => e.severity === filterSeverity);

  const toggleExpand = (id: string) => {
    setExpandedError(expandedError === id ? null : id);
  };

  const toggleRawError = (id: string) => {
    setShowRawError(prev => ({ ...prev, [id]: !prev[id] }));
  };

  const formatTimeAgo = (date: Date): string => {
    const now = new Date();
    const diff = now.getTime() - date.getTime();
    const hours = Math.floor(diff / (1000 * 60 * 60));
    const minutes = Math.floor(diff / (1000 * 60));

    if (hours > 0) return `${hours}h ago`;
    if (minutes > 0) return `${minutes}m ago`;
    return 'Just now';
  };

  return (
    <div className="space-y-6">
      {/* Page Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Error Intelligence</h1>
          <p className="text-slate-500 mt-1">
            AI-powered error analysis with actionable insights and resolution steps
          </p>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-white rounded-xl border border-slate-200 p-5">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-slate-500">Total Errors</p>
              <p className="text-3xl font-bold text-slate-900 mt-1">{totalErrors}</p>
            </div>
            <div className="w-12 h-12 bg-slate-100 rounded-lg flex items-center justify-center">
              <AlertCircle className="w-6 h-6 text-slate-600" />
            </div>
          </div>
        </div>

        <div className="bg-white rounded-xl border border-red-200 p-5">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-red-600">Critical</p>
              <p className="text-3xl font-bold text-red-700 mt-1">{criticalCount}</p>
            </div>
            <div className="w-12 h-12 bg-red-100 rounded-lg flex items-center justify-center">
              <AlertCircle className="w-6 h-6 text-red-600" />
            </div>
          </div>
        </div>

        <div className="bg-white rounded-xl border border-amber-200 p-5">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-amber-600">Warnings</p>
              <p className="text-3xl font-bold text-amber-700 mt-1">{warningCount}</p>
            </div>
            <div className="w-12 h-12 bg-amber-100 rounded-lg flex items-center justify-center">
              <AlertTriangle className="w-6 h-6 text-amber-600" />
            </div>
          </div>
        </div>

        <div className="bg-white rounded-xl border border-blue-200 p-5">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-blue-600">Info</p>
              <p className="text-3xl font-bold text-blue-700 mt-1">{infoCount}</p>
            </div>
            <div className="w-12 h-12 bg-blue-100 rounded-lg flex items-center justify-center">
              <Info className="w-6 h-6 text-blue-600" />
            </div>
          </div>
        </div>
      </div>

      {/* Error Pattern Analysis */}
      <div className="bg-white rounded-xl border border-slate-200 p-6">
        <h2 className="text-lg font-semibold text-slate-900 mb-4">Error Pattern Analysis</h2>
        <div className="grid grid-cols-1 md:grid-cols-5 gap-3">
          {errorPattern.map((pattern, index) => (
            <div
              key={index}
              className="bg-slate-50 rounded-lg p-4 border border-slate-200"
            >
              <p className="text-sm font-medium text-slate-900">{pattern.pattern}</p>
              <p className="text-2xl font-bold text-slate-700 mt-2">{pattern.count}</p>
              <p className="text-xs text-slate-500 mt-1">occurrences</p>
            </div>
          ))}
        </div>
      </div>

      {/* Severity Filter */}
      <div className="flex items-center space-x-2">
        <span className="text-sm font-medium text-slate-700">Filter by severity:</span>
        {(['all', 'critical', 'warning', 'info'] as const).map((severity) => {
          const isActive = filterSeverity === severity;

          return (
            <button
              key={severity}
              onClick={() => setFilterSeverity(severity)}
              className={`
                px-3 py-1.5 rounded-full text-sm font-medium transition-colors
                ${isActive
                  ? severity === 'all'
                    ? 'bg-slate-800 text-white'
                    : severity === 'critical'
                      ? 'bg-red-50 text-red-700'
                      : severity === 'warning'
                        ? 'bg-amber-50 text-amber-700'
                        : 'bg-blue-50 text-blue-700'
                  : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                }
              `}
            >
              {severity === 'all' ? 'All' : severityConfig[severity].label}
            </button>
          );
        })}
      </div>

      {/* Error Cards */}
      <div className="space-y-4">
        {filteredErrors.map((error) => {
          const config = severityConfig[error.severity];
          const SeverityIcon = config.icon;
          const isExpanded = expandedError === error.id;

          return (
            <div
              key={error.id}
              className={`
                bg-white rounded-xl border-2 overflow-hidden transition-all duration-200
                ${config.borderColor} ${isExpanded ? 'shadow-lg' : 'shadow-sm'}
              `}
            >
              {/* Error Header */}
              <div
                className={`p-5 cursor-pointer ${config.bgColor}`}
                onClick={() => toggleExpand(error.id)}
              >
                <div className="flex items-start justify-between">
                  <div className="flex items-start space-x-4">
                    <div className={`
                      w-10 h-10 rounded-lg flex items-center justify-center
                      ${error.severity === 'critical' ? 'bg-red-100' : error.severity === 'warning' ? 'bg-amber-100' : 'bg-blue-100'}
                    `}>
                      <SeverityIcon className={`w-5 h-5 ${config.color}`} />
                    </div>
                    <div>
                      <div className="flex items-center space-x-2">
                        <h3 className="text-lg font-semibold text-slate-900">{error.title}</h3>
                        <span className={`
                          inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium
                          ${config.bgColor} ${config.color}
                        `}>
                          {config.label}
                        </span>
                        {error.occurrenceCount > 1 && (
                          <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-slate-100 text-slate-600">
                            <Repeat className="w-3 h-3 mr-1" />
                            {error.occurrenceCount}x
                          </span>
                        )}
                      </div>
                      <p className="text-sm text-slate-600 mt-1">{error.description}</p>
                    </div>
                  </div>
                  <div className="flex items-center space-x-4">
                    <div className="text-right">
                      <p className="text-sm text-slate-500 flex items-center">
                        <Clock className="w-4 h-4 mr-1" />
                        {formatTimeAgo(error.timestamp)}
                      </p>
                      <p className="text-sm text-slate-500 mt-1">{error.affectedPipeline}</p>
                    </div>
                    {isExpanded ? (
                      <ChevronUp className="w-5 h-5 text-slate-400" />
                    ) : (
                      <ChevronDown className="w-5 h-5 text-slate-400" />
                    )}
                  </div>
                </div>
              </div>

              {/* Expanded Content */}
              {isExpanded && (
                <div className="p-5 border-t border-slate-200">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    {/* What Happened & Why */}
                    <div>
                      <h4 className="text-sm font-semibold text-slate-900 mb-3">Analysis</h4>
                      <div className="space-y-4">
                        <div>
                          <p className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">
                            What Happened
                          </p>
                          <p className="text-sm text-slate-700">{error.description}</p>
                        </div>
                        <div>
                          <p className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">
                            Why It Happened
                          </p>
                          <p className="text-sm text-slate-700">{error.reason}</p>
                        </div>
                      </div>
                    </div>

                    {/* Source System & Suggested Fix */}
                    <div>
                      <h4 className="text-sm font-semibold text-slate-900 mb-3">Resolution</h4>
                      <div className="space-y-4">
                        <div className="flex items-center p-3 bg-slate-50 rounded-lg">
                          <Server className="w-5 h-5 text-slate-500 mr-3" />
                          <div>
                            <p className="text-xs text-slate-500">Source System</p>
                            <p className="text-sm font-medium text-slate-900">{error.sourceSystem}</p>
                          </div>
                        </div>
                        <div>
                          <p className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-2 flex items-center">
                            <Wrench className="w-4 h-4 mr-1" />
                            Suggested Fix
                          </p>
                          <ol className="space-y-2">
                            {error.suggestedFix.map((fix, index) => (
                              <li key={index} className="flex items-start text-sm">
                                <span className="flex-shrink-0 w-5 h-5 bg-emerald-100 text-emerald-700 rounded-full flex items-center justify-center text-xs font-medium mr-2">
                                  {index + 1}
                                </span>
                                <span className="text-slate-700">{fix}</span>
                              </li>
                            ))}
                          </ol>
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* Raw Error Toggle */}
                  {error.rawError && (
                    <div className="mt-4 pt-4 border-t border-slate-200">
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          toggleRawError(error.id);
                        }}
                        className="flex items-center text-sm text-slate-500 hover:text-slate-700"
                      >
                        {showRawError[error.id] ? (
                          <>
                            <EyeOff className="w-4 h-4 mr-1" />
                            Hide Technical Details
                          </>
                        ) : (
                          <>
                            <Eye className="w-4 h-4 mr-1" />
                            Show Technical Details
                          </>
                        )}
                      </button>
                      {showRawError[error.id] && (
                        <pre className="mt-2 p-4 bg-slate-900 rounded-lg text-slate-300 text-xs overflow-x-auto">
                          {error.rawError}
                        </pre>
                      )}
                    </div>
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>

      {filteredErrors.length === 0 && (
        <div className="bg-white rounded-xl border border-slate-200 p-12 text-center">
          <div className="w-16 h-16 bg-emerald-100 rounded-full flex items-center justify-center mx-auto">
            <CheckCircle className="w-8 h-8 text-emerald-600" />
          </div>
          <h3 className="text-lg font-semibold text-slate-900 mt-4">No Errors Found</h3>
          <p className="text-slate-500 mt-2">
            {filterSeverity === 'all'
              ? 'All pipelines are running successfully.'
              : `No ${filterSeverity} errors found in the selected filter.`}
          </p>
        </div>
      )}
    </div>
  );
};

export default ErrorIntelligence;
