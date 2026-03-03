import { addHours, subHours, subDays, format } from "date-fns";

export type Status = "success" | "failed" | "running" | "cancelled" | "warning";
export type Layer = "Ingestion" | "Standardization" | "Business Ready" | "Analytics";
export type Severity = "Critical" | "Warning" | "Info";

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

export interface SystemHealth {
  overallScore: number;
  freshness: string;
  lastRunTime: string;
  activePipelines: number;
  failedPipelines: number;
}

export const mockPipelines: PipelineRun[] = [
  {
    id: "p1",
    name: "Customer 360 - Daily Sync",
    sourceSystem: "Salesforce",
    layer: "Ingestion",
    status: "success",
    startTime: subHours(new Date(), 2).toISOString(),
    duration: "45m",
    successRate: 98,
  },
  {
    id: "p2",
    name: "ERP Transactions",
    sourceSystem: "SAP",
    layer: "Standardization",
    status: "running",
    startTime: subHours(new Date(), 0.5).toISOString(),
    duration: "Running (12m)",
    successRate: 95,
  },
  {
    id: "p3",
    name: "Web Events Stream",
    sourceSystem: "Google Analytics",
    layer: "Ingestion",
    status: "failed",
    startTime: subHours(new Date(), 4).toISOString(),
    duration: "Stopped after 5m",
    successRate: 88,
  },
  {
    id: "p4",
    name: "Financial Forecast Model",
    sourceSystem: "Oracle Finance",
    layer: "Analytics",
    status: "success",
    startTime: subDays(new Date(), 1).toISOString(),
    duration: "1h 20m",
    successRate: 99,
  },
  {
    id: "p5",
    name: "Inventory Snapshot",
    sourceSystem: "WMS",
    layer: "Business Ready",
    status: "success",
    startTime: subHours(new Date(), 6).toISOString(),
    duration: "30m",
    successRate: 97,
  },
  {
    id: "p6",
    name: "Marketing Campaign ROI",
    sourceSystem: "HubSpot",
    layer: "Analytics",
    status: "warning",
    startTime: subHours(new Date(), 8).toISOString(),
    duration: "55m",
    successRate: 92,
  },
];

export const mockErrors: PipelineError[] = [
  {
    id: "e1",
    pipelineName: "Web Events Stream",
    sourceSystem: "Google Analytics",
    summary: "Schema Mismatch in Event Payload",
    rootCause: "New field 'session_quality' detected in source but not in target schema.",
    suggestedFix: "Update the target schema to include 'session_quality' as a nullable integer or filter this field in the ingestion config.",
    severity: "Critical",
    timestamp: subHours(new Date(), 4).toISOString(),
    frequency: 12,
  },
  {
    id: "e2",
    pipelineName: "Marketing Campaign ROI",
    sourceSystem: "HubSpot",
    summary: "API Rate Limit Approaching",
    rootCause: "High volume of requests during peak hours triggered a warning threshold (85% of quota).",
    suggestedFix: "Implement exponential backoff in the extraction connector or schedule the run for off-peak hours (2 AM - 4 AM).",
    severity: "Warning",
    timestamp: subHours(new Date(), 8).toISOString(),
    frequency: 3,
  },
  {
    id: "e3",
    pipelineName: "ERP Transactions",
    sourceSystem: "SAP",
    summary: "Data Freshness Delay",
    rootCause: "Source system extract was generated 2 hours late.",
    suggestedFix: "Check SAP job scheduler logs for 'Daily_Extract_Job'. No action needed on pipeline side.",
    severity: "Info",
    timestamp: subDays(new Date(), 1).toISOString(),
    frequency: 1,
  },
];

export const mockHealth: SystemHealth = {
  overallScore: 94,
  freshness: "12m ago",
  lastRunTime: subHours(new Date(), 0.1).toISOString(),
  activePipelines: 42,
  failedPipelines: 1,
};
