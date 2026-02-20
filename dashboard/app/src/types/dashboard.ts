export type PipelineStatus = 'success' | 'failed' | 'running' | 'cancelled' | 'warning';
export type ErrorSeverity = 'critical' | 'warning' | 'info';
export type PipelineLayer = 'ingestion' | 'standardization' | 'business_ready' | 'analytics';
export type ConnectionHealth = 'healthy' | 'degraded' | 'critical';

// Replit-style types for Pipeline Monitor
export type Layer = 'Ingestion' | 'Standardization' | 'Business Ready' | 'Analytics';
export type Status = 'success' | 'failed' | 'running' | 'cancelled' | 'warning';
export type Severity = 'Critical' | 'Warning' | 'Info';

export interface PipelineRun {
  id: string;
  name: string;
  sourceSystem: string;
  layer: Layer;
  status: Status;
  startTime: string;
  duration: string;
  successRate: number;
}

export interface PipelineError {
  id: string;
  pipelineName: string;
  sourceSystem: string;
  summary: string;
  rootCause: string;
  suggestedFix: string;
  severity: Severity;
  timestamp: string;
  frequency: number;
}

// MiniMax-style types for Error Intelligence (expanded view)
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

// Admin & Governance types
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

export interface SystemHealth {
  overallScore: number;
  freshness: string;
  lastRunTime: string;
  activePipelines: number;
  failedPipelines: number;
}

export type ViewType = 'pipeline-monitor' | 'error-intelligence' | 'admin-governance';
