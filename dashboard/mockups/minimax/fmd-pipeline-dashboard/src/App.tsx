// FMD Pipeline Dashboard - Main Application

import React, { useState, useEffect, useMemo } from 'react';
import { Sidebar } from './components/Sidebar';
import { TopBar } from './components/TopBar';
import { PipelineMonitor } from './components/PipelineMonitor';
import { ErrorIntelligence } from './components/ErrorIntelligence';
import { AdminGovernance } from './components/AdminGovernance';
import {
  getPipelineMonitorData,
  getErrorIntelligenceData,
  getAdminGovernanceData,
  getSystemHealth,
} from './data/mockData';
import type { ViewType } from './types/dashboard';

function App() {
  const [currentView, setCurrentView] = useState<ViewType>('pipeline-monitor');
  const [isLoading, setIsLoading] = useState(true);
  const [lastRefresh, setLastRefresh] = useState(new Date());

  // Load data
  const pipelineMonitorData = useMemo(() => getPipelineMonitorData(), []);
  const errorIntelligenceData = useMemo(() => getErrorIntelligenceData(), []);
  const adminGovernanceData = useMemo(() => getAdminGovernanceData(), []);
  const systemHealth = useMemo(() => getSystemHealth(), []);

  // Simulate loading
  useEffect(() => {
    const timer = setTimeout(() => {
      setIsLoading(false);
    }, 500);
    return () => clearTimeout(timer);
  }, []);

  const handleRefresh = () => {
    setIsLoading(true);
    setLastRefresh(new Date());
    // Simulate refresh delay
    setTimeout(() => {
      setIsLoading(false);
    }, 800);
  };

  const renderView = () => {
    switch (currentView) {
      case 'pipeline-monitor':
        return (
          <PipelineMonitor
            activePipelines={pipelineMonitorData.activePipelines}
            runHistory={pipelineMonitorData.runHistory}
            executionTimeline={pipelineMonitorData.executionTimeline}
            durationTrend={pipelineMonitorData.durationTrend}
            successRateTrend={pipelineMonitorData.successRateTrend}
          />
        );
      case 'error-intelligence':
        return (
          <ErrorIntelligence
            errors={errorIntelligenceData.errors}
            totalErrors={errorIntelligenceData.totalErrors}
            criticalCount={errorIntelligenceData.criticalCount}
            warningCount={errorIntelligenceData.warningCount}
            infoCount={errorIntelligenceData.infoCount}
            errorPattern={errorIntelligenceData.errorPattern}
          />
        );
      case 'admin-governance':
        return (
          <AdminGovernance
            healthScorecard={adminGovernanceData.healthScorecard}
            sourceSystems={adminGovernanceData.sourceSystems}
            entityInventory={adminGovernanceData.entityInventory}
            dataLineage={adminGovernanceData.dataLineage}
          />
        );
      default:
        return null;
    }
  };

  if (isLoading) {
    return (
      <div className="min-h-screen bg-slate-50 flex items-center justify-center">
        <div className="text-center">
          <div className="w-16 h-16 border-4 border-emerald-500 border-t-transparent rounded-full animate-spin mx-auto"></div>
          <p className="mt-4 text-slate-600 font-medium">Loading dashboard...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-slate-50">
      {/* Sidebar */}
      <Sidebar currentView={currentView} onViewChange={setCurrentView} />

      {/* Main Content Area */}
      <div className="ml-60">
        {/* Top Bar */}
        <TopBar
          systemHealth={systemHealth}
          activePipelines={pipelineMonitorData.activePipelines.length}
          failedPipelines={adminGovernanceData.healthScorecard.failedPipelines}
          onRefresh={handleRefresh}
        />

        {/* Page Content */}
        <main className="p-6">
          {renderView()}
        </main>
      </div>
    </div>
  );
}

export default App;
