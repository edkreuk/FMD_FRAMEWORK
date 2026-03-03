// FMD Pipeline Dashboard - Type Definitions

// ============================================
// Core Types
// ============================================

export type PipelineStatus = 'success' | 'failed' | 'running' | 'cancelled';
export type ErrorSeverity = 'critical' | 'warning' | 'info';
export type PipelineLayer = 'ingestion' | 'standardization' | 'business_ready' | 'analytics';
export type ConnectionHealth = 'healthy' | 'degraded' | 'critical';

export interface Pipeline {
  id: string;
  name: string;
  status: PipelineStatus;
  layer: PipelineLayer;
  sourceSystem: string;
  startTime: Date;
  endTime?: Date;
  duration?: number;
  recordsProcessed?: number;
  errorCount?: number;
}

export interface ExecutionTimeline {
  layer: PipelineLayer;
  status: PipelineStatus;
  startTime?: Date;
  endTime?: Date;
  progress: number;
}

export interface ErrorSummary {
  id: string;
  title: string;
  description: string;
  reason: string;
  sourceSystem: string;
  suggestedFix: string[];
  severity: ErrorSeverity;
  timestamp: Date;
  occurrenceCount: number;
  affectedPipeline: string;
  rawError?: string;
}

export interface SourceSystem {
  id: string;
  name: string;
  type: string;
  connectionHealth: ConnectionHealth;
  lastSyncTime: Date;
  recordCount: number;
}

export interface EntityInventory {
  layer: string;
  entityCount: number;
  lastUpdated: Date;
}

export interface HealthScorecard {
  successRate: number;
  dataFreshness: number;
  averageRuntime: number;
  totalPipelines: number;
  activePipelines: number;
  failedPipelines: number;
}

export interface DataLineageNode {
  id: string;
  name: string;
  layer: string;
  type: 'source' | 'landing' | 'bronze' | 'silver' | 'gold';
}

export interface DataLineageConnection {
  source: string;
  target: string;
  flow: number;
}

// ============================================
// View Types
// ============================================

export interface PipelineMonitorViewData {
  activePipelines: Pipeline[];
  runHistory: Pipeline[];
  executionTimeline: ExecutionTimeline[];
  filters: {
    dateRange: { from: Date; to: Date };
    sourceSystem: string;
    layer: string;
  };
  durationTrend: { date: string; duration: number }[];
  successRateTrend: { date: string; rate: number }[];
}

export interface ErrorIntelligenceViewData {
  errors: ErrorSummary[];
  totalErrors: number;
  criticalCount: number;
  warningCount: number;
  infoCount: number;
  errorPattern: { pattern: string; count: number }[];
}

export interface AdminGovernanceViewData {
  healthScorecard: HealthScorecard;
  sourceSystems: SourceSystem[];
  entityInventory: EntityInventory[];
  dataLineage: {
    nodes: DataLineageNode[];
    connections: DataLineageConnection[];
  };
}

// ============================================
// Navigation Types
// ============================================

export type ViewType = 'pipeline-monitor' | 'error-intelligence' | 'admin-governance';

export interface NavigationItem {
  id: ViewType;
  label: string;
  icon: string;
}
