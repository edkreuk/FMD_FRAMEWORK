// FMD Pipeline Dashboard - TopBar Component

import React from 'react';
import {
  CheckCircle,
  AlertTriangle,
  XCircle,
  Bell,
  User,
  RefreshCw,
} from 'lucide-react';
import type { ViewType } from '../types/dashboard';

interface TopBarProps {
  systemHealth: 'healthy' | 'degraded' | 'critical';
  activePipelines: number;
  failedPipelines: number;
  onRefresh?: () => void;
}

const healthConfig = {
  healthy: {
    label: 'System Healthy',
    color: 'bg-emerald-500',
    bgColor: 'bg-emerald-500/10',
    borderColor: 'border-emerald-500/20',
    textColor: 'text-emerald-600',
    icon: CheckCircle,
    description: 'All systems operational',
  },
  degraded: {
    label: 'System Degraded',
    color: 'bg-amber-500',
    bgColor: 'bg-amber-500/10',
    borderColor: 'border-amber-500/20',
    textColor: 'text-amber-600',
    icon: AlertTriangle,
    description: 'Some pipelines experiencing issues',
  },
  critical: {
    label: 'System Critical',
    color: 'bg-red-500',
    bgColor: 'bg-red-500/10',
    borderColor: 'border-red-500/20',
    textColor: 'text-red-600',
    icon: XCircle,
    description: 'Immediate attention required',
  },
};

export const TopBar: React.FC<TopBarProps> = ({
  systemHealth,
  activePipelines,
  failedPipelines,
  onRefresh,
}) => {
  const config = healthConfig[systemHealth];
  const HealthIcon = config.icon;

  return (
    <header className="h-16 bg-white border-b border-slate-200 flex items-center justify-between px-6 sticky top-0 z-40">
      {/* Left Section - Page Title Placeholder */}
      <div className="flex items-center">
        <h2 className="text-slate-900 font-semibold text-lg">
          Financial Data Pipeline Dashboard
        </h2>
      </div>

      {/* Right Section - Status & Actions */}
      <div className="flex items-center space-x-4">
        {/* Health Status Indicator */}
        <div
          className={`
            flex items-center px-4 py-2 rounded-lg border ${config.bgColor} ${config.borderColor}
          `}
        >
          <HealthIcon className={`w-5 h-5 ${config.textColor} mr-2`} />
          <div>
            <p className={`text-sm font-semibold ${config.textColor}`}>
              {config.label}
            </p>
            <p className="text-xs text-slate-500">{config.description}</p>
          </div>
        </div>

        {/* Quick Stats */}
        <div className="flex items-center space-x-3 px-4 py-2 bg-slate-50 rounded-lg border border-slate-200">
          <div className="flex items-center">
            <div className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse mr-2"></div>
            <span className="text-sm text-slate-600">
              <span className="font-semibold text-slate-900">{activePipelines}</span> Running
            </span>
          </div>
          <div className="w-px h-4 bg-slate-300"></div>
          <div className="flex items-center">
            <div className="w-2 h-2 rounded-full bg-red-500 mr-2"></div>
            <span className="text-sm text-slate-600">
              <span className="font-semibold text-slate-900">{failedPipelines}</span> Failed
            </span>
          </div>
        </div>

        {/* Actions */}
        <div className="flex items-center space-x-2">
          {/* Refresh Button */}
          <button
            onClick={onRefresh}
            className="p-2 text-slate-500 hover:text-slate-700 hover:bg-slate-100 rounded-lg transition-colors"
            title="Refresh Data"
          >
            <RefreshCw className="w-5 h-5" />
          </button>

          {/* Notifications */}
          <button className="relative p-2 text-slate-500 hover:text-slate-700 hover:bg-slate-100 rounded-lg transition-colors">
            <Bell className="w-5 h-5" />
            <span className="absolute top-1 right-1 w-2 h-2 bg-red-500 rounded-full"></span>
          </button>

          {/* User Profile */}
          <button className="flex items-center space-x-2 px-3 py-2 text-slate-500 hover:text-slate-700 hover:bg-slate-100 rounded-lg transition-colors">
            <div className="w-8 h-8 bg-slate-200 rounded-full flex items-center justify-center">
              <User className="w-4 h-4 text-slate-600" />
            </div>
          </button>
        </div>
      </div>
    </header>
  );
};

export default TopBar;
