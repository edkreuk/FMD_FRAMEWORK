// Mock data for the FMD Pipeline Dashboard

export type PipelineStatus = "success" | "failed" | "running" | "cancelled";
export type Severity = "critical" | "warning" | "info";
export type Layer = "Ingestion" | "Standardization" | "Business Ready" | "Analytics";

export interface PipelineRun {
  id: string;
  name: string;
  source: string;
  layer: Layer;
  status: PipelineStatus;
  startTime: string;
  duration: string;
  timestamp: string;
}

export interface ErrorItem {
  id: string;
  severity: Severity;
  summary: string;
  whatHappened: string;
  why: string;
  sourceSystem: string;
  suggestedFix: string;
  timestamp: string;
  frequency: number;
  layer: Layer;
}

export interface SourceSystem {
  id: string;
  name: string;
  type: string;
  health: "healthy" | "degraded" | "down";
  lastSync: string;
  recordsProcessed: number;
}

export interface EntityCount {
  layer: string;
  label: string;
  count: number;
  color: string;
}

export const pipelineRuns: PipelineRun[] = [
  { id: "1", name: "Customer Data Refresh", source: "Salesforce", layer: "Ingestion", status: "success", startTime: "08:00 AM", duration: "4m 12s", timestamp: "2026-02-18T08:04:12" },
  { id: "2", name: "Transaction Processing", source: "Payment Gateway", layer: "Standardization", status: "running", startTime: "08:15 AM", duration: "2m 34s", timestamp: "2026-02-18T08:17:34" },
  { id: "3", name: "Product Catalog Sync", source: "ERP System", layer: "Business Ready", status: "failed", startTime: "07:45 AM", duration: "6m 01s", timestamp: "2026-02-18T07:51:01" },
  { id: "4", name: "Marketing Attribution", source: "Ad Platform", layer: "Analytics", status: "success", startTime: "07:30 AM", duration: "3m 22s", timestamp: "2026-02-18T07:33:22" },
  { id: "5", name: "Inventory Update", source: "Warehouse System", layer: "Ingestion", status: "success", startTime: "07:00 AM", duration: "2m 45s", timestamp: "2026-02-18T07:02:45" },
  { id: "6", name: "User Activity Roll-up", source: "App Analytics", layer: "Analytics", status: "cancelled", startTime: "06:30 AM", duration: "1m 10s", timestamp: "2026-02-18T06:31:10" },
  { id: "7", name: "Financial Reconciliation", source: "Banking API", layer: "Standardization", status: "success", startTime: "06:00 AM", duration: "5m 30s", timestamp: "2026-02-18T06:05:30" },
  { id: "8", name: "Compliance Report Gen", source: "Regulatory Feed", layer: "Business Ready", status: "success", startTime: "05:30 AM", duration: "8m 15s", timestamp: "2026-02-18T05:38:15" },
];

export const errorItems: ErrorItem[] = [
  {
    id: "1", severity: "critical",
    summary: "Customer records failed to load — 12,400 records stuck in queue",
    whatHappened: "The customer data refresh pipeline could not complete because the source system returned incomplete records for a batch of 12,400 entries.",
    why: "The Salesforce API rate limit was exceeded during peak hours, causing partial data extraction.",
    sourceSystem: "Salesforce",
    suggestedFix: "Reschedule the customer refresh to run before 7:00 AM to avoid peak API traffic. Consider enabling batch retry with exponential backoff.",
    timestamp: "2026-02-18T07:51:01",
    frequency: 3, layer: "Ingestion"
  },
  {
    id: "2", severity: "critical",
    summary: "Product catalog has 340 mismatched SKUs between systems",
    whatHappened: "During standardization, 340 product SKUs from the ERP system did not match the expected format in the product master.",
    why: "A recent ERP update changed the SKU format from 8-digit to 10-digit without notification.",
    sourceSystem: "ERP System",
    suggestedFix: "Update the SKU mapping rules in the standardization layer. Contact the ERP team to confirm the new format and backfill historical records.",
    timestamp: "2026-02-18T07:45:00",
    frequency: 1, layer: "Standardization"
  },
  {
    id: "3", severity: "warning",
    summary: "Marketing spend data is 6 hours behind schedule",
    whatHappened: "The ad platform data pipeline completed but delivered data from 6 hours ago instead of the expected near-real-time feed.",
    why: "The ad platform API experienced intermittent delays in their reporting endpoint.",
    sourceSystem: "Ad Platform",
    suggestedFix: "No action needed if delay resolves within 2 hours. If persistent, switch to the secondary reporting endpoint.",
    timestamp: "2026-02-18T08:10:00",
    frequency: 5, layer: "Analytics"
  },
  {
    id: "4", severity: "warning",
    summary: "Duplicate transaction entries detected — 89 records affected",
    whatHappened: "The payment processing pipeline inserted 89 duplicate transaction records during the morning batch.",
    why: "A retry mechanism triggered twice due to a timeout, causing the same batch to be processed again.",
    sourceSystem: "Payment Gateway",
    suggestedFix: "Run the deduplication job manually. Review the retry configuration to add idempotency checks.",
    timestamp: "2026-02-18T08:05:00",
    frequency: 2, layer: "Standardization"
  },
  {
    id: "5", severity: "info",
    summary: "Warehouse inventory sync completed with minor schema drift",
    whatHappened: "The inventory update pipeline completed successfully but detected 3 new columns in the source data that are not yet mapped.",
    why: "The warehouse system added new tracking fields for lot numbers and expiration dates.",
    sourceSystem: "Warehouse System",
    suggestedFix: "Review the new columns and decide if they should be added to the data model. No data loss occurred.",
    timestamp: "2026-02-18T07:02:45",
    frequency: 1, layer: "Ingestion"
  },
];

export const sourceSystems: SourceSystem[] = [
  { id: "1", name: "Salesforce", type: "CRM", health: "degraded", lastSync: "12 min ago", recordsProcessed: 245_800 },
  { id: "2", name: "Payment Gateway", type: "Financial", health: "healthy", lastSync: "2 min ago", recordsProcessed: 1_230_500 },
  { id: "3", name: "ERP System", type: "Operations", health: "down", lastSync: "45 min ago", recordsProcessed: 89_200 },
  { id: "4", name: "Ad Platform", type: "Marketing", health: "healthy", lastSync: "8 min ago", recordsProcessed: 567_000 },
  { id: "5", name: "Warehouse System", type: "Logistics", health: "healthy", lastSync: "5 min ago", recordsProcessed: 134_600 },
  { id: "6", name: "App Analytics", type: "Product", health: "healthy", lastSync: "1 min ago", recordsProcessed: 2_100_000 },
  { id: "7", name: "Banking API", type: "Financial", health: "healthy", lastSync: "15 min ago", recordsProcessed: 78_400 },
  { id: "8", name: "Regulatory Feed", type: "Compliance", health: "healthy", lastSync: "30 min ago", recordsProcessed: 12_300 },
];

export const entityCounts: EntityCount[] = [
  { layer: "Landing", label: "Raw Extracts", count: 142, color: "primary" },
  { layer: "Bronze", label: "Cleaned Records", count: 98, color: "info" },
  { layer: "Silver", label: "Business Entities", count: 64, color: "warning" },
  { layer: "Gold", label: "Analytics Models", count: 28, color: "success" },
];

export const lineageFlows = [
  { source: "Salesforce", layers: ["Landing", "Bronze", "Silver", "Gold"], destination: "Customer 360" },
  { source: "Payment Gateway", layers: ["Landing", "Bronze", "Silver", "Gold"], destination: "Revenue Dashboard" },
  { source: "ERP System", layers: ["Landing", "Bronze", "Silver"], destination: "Product Master" },
  { source: "Ad Platform", layers: ["Landing", "Bronze", "Gold"], destination: "Marketing Analytics" },
  { source: "Warehouse System", layers: ["Landing", "Bronze", "Silver"], destination: "Inventory Report" },
];

export const healthMetrics = {
  overallSuccessRate: 94.2,
  pipelinesRunToday: 47,
  pipelinesFailed: 3,
  avgDuration: "4m 18s",
  dataFreshness: "98.1%",
  lastFullRun: "8:15 AM",
  activePipelines: 2,
  totalSources: 8,
};

export const durationTrend = [
  { time: "12 AM", duration: 3.2, successRate: 100 },
  { time: "2 AM", duration: 2.8, successRate: 100 },
  { time: "4 AM", duration: 4.1, successRate: 95 },
  { time: "6 AM", duration: 5.5, successRate: 90 },
  { time: "8 AM", duration: 4.3, successRate: 88 },
  { time: "10 AM", duration: 3.7, successRate: 96 },
  { time: "12 PM", duration: 3.1, successRate: 100 },
  { time: "2 PM", duration: 4.8, successRate: 92 },
];
