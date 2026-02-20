// FMD Pipeline Dashboard - Sidebar Component

import React from 'react';
import {
  Activity,
  AlertCircle,
  Settings,
  LayoutDashboard,
  ChevronRight,
} from 'lucide-react';
import type { ViewType } from '../types/dashboard';

interface SidebarProps {
  currentView: ViewType;
  onViewChange: (view: ViewType) => void;
}

const navigationItems = [
  {
    id: 'pipeline-monitor' as ViewType,
    label: 'Pipeline Monitor',
    icon: Activity,
  },
  {
    id: 'error-intelligence' as ViewType,
    label: 'Error Intelligence',
    icon: AlertCircle,
  },
  {
    id: 'admin-governance' as ViewType,
    label: 'Admin & Governance',
    icon: Settings,
  },
];

export const Sidebar: React.FC<SidebarProps> = ({ currentView, onViewChange }) => {
  return (
    <aside className="fixed left-0 top-0 h-screen w-60 bg-slate-900 flex flex-col border-r border-slate-800">
      {/* Logo Section */}
      <div className="h-16 flex items-center px-6 border-b border-slate-800">
        <LayoutDashboard className="w-7 h-7 text-emerald-500 mr-3" />
        <div>
          <h1 className="text-white font-semibold text-lg leading-tight">FMD</h1>
          <p className="text-slate-400 text-xs">Pipeline Intelligence</p>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 py-6 px-3">
        <ul className="space-y-1">
          {navigationItems.map((item) => {
            const isActive = currentView === item.id;
            const Icon = item.icon;

            return (
              <li key={item.id}>
                <button
                  onClick={() => onViewChange(item.id)}
                  className={`
                    w-full flex items-center px-4 py-3 rounded-lg text-sm font-medium
                    transition-all duration-200 group
                    ${
                      isActive
                        ? 'bg-emerald-500/10 text-emerald-500 border border-emerald-500/20'
                        : 'text-slate-400 hover:bg-slate-800 hover:text-white'
                    }
                  `}
                >
                  <Icon
                    className={`w-5 h-5 mr-3 ${isActive ? 'text-emerald-500' : 'text-slate-500 group-hover:text-white'}`}
                  />
                  <span className="flex-1 text-left">{item.label}</span>
                  {isActive && (
                    <ChevronRight className="w-4 h-4 text-emerald-500" />
                  )}
                </button>
              </li>
            );
          })}
        </ul>
      </nav>

      {/* Footer */}
      <div className="p-4 border-t border-slate-800">
        <div className="bg-slate-800/50 rounded-lg p-4">
          <p className="text-slate-400 text-xs mb-2">Last Updated</p>
          <p className="text-white text-sm font-medium">
            {new Date().toLocaleTimeString('en-US', {
              hour: '2-digit',
              minute: '2-digit',
            })}
          </p>
          <p className="text-slate-500 text-xs mt-1">
            {new Date().toLocaleDateString('en-US', {
              month: 'short',
              day: 'numeric',
            })}
          </p>
        </div>
      </div>
    </aside>
  );
};

export default Sidebar;
