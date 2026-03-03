import { subHours, subDays } from "date-fns";
import type {
  PipelineRun, PipelineError, SystemHealth, Layer, Status, Severity,
  ErrorSummary, SourceSystem, EntityInventory, HealthScorecard,
  DataLineageNode, DataLineageConnection, ErrorSeverity,
} from "@/types/dashboard";

// ========================================
// Replit-style data (Pipeline Monitor + Error Intelligence)
// ========================================

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

// ========================================
// MiniMax-style data (Error Intelligence + Admin)
// ========================================

const hoursAgo = (hours: number): Date => {
  const date = new Date();
  date.setHours(date.getHours() - hours);
  return date;
};

const minutesAgo = (minutes: number): Date => {
  const date = new Date();
  date.setMinutes(date.getMinutes() - minutes);
  return date;
};

const daysAgo = (days: number): Date => {
  const date = new Date();
  date.setDate(date.getDate() - days);
  return date;
};

export const errorPatterns = [
  { pattern: 'Connection Timeout', count: 3 },
  { pattern: 'Duplicate Records', count: 12 },
  { pattern: 'Missing Required Field', count: 8 },
  { pattern: 'Currency Mismatch', count: 24 },
  { pattern: 'API Rate Limit', count: 2 },
];

export const errorSummaries: ErrorSummary[] = [
  {
    id: 'err-001',
    title: 'Connection Timeout to SAP System',
    description: 'The pipeline failed to complete data extraction from SAP ERP due to connection timeout.',
    reason: 'SAP server response time exceeded the configured 30-second threshold. This typically indicates high load on the SAP system or network latency issues.',
    sourceSystem: 'SAP ERP',
    suggestedFix: [
      'Check SAP system workload using transaction SM37',
      'Verify network connectivity to SAP server',
      'Consider increasing timeout configuration',
      'Schedule extraction during off-peak hours',
    ],
    severity: 'critical',
    timestamp: hoursAgo(1),
    occurrenceCount: 3,
    affectedPipeline: 'Monthly Revenue Aggregation',
  },
  {
    id: 'err-002',
    title: 'Duplicate Record Detected in Customer Data',
    description: 'Standardization layer detected duplicate customer records that could not be automatically merged.',
    reason: 'Multiple customer records with matching names but different account IDs were found. This indicates data quality issues in the source CRM system.',
    sourceSystem: 'Salesforce CRM',
    suggestedFix: [
      'Review customer records in Salesforce',
      'Merge duplicate entries manually',
      'Implement deduplication rules in CRM',
      'Update customer matching criteria',
    ],
    severity: 'warning',
    timestamp: hoursAgo(3),
    occurrenceCount: 12,
    affectedPipeline: 'Customer Transaction Processing',
  },
  {
    id: 'err-003',
    title: 'Missing Required Field: GL Account Code',
    description: 'Expense report records are missing the required GL account code field.',
    reason: 'The source system sent expense records without mandatory GL account codes. This prevents proper categorization in the financial system.',
    sourceSystem: 'Workday HCM',
    suggestedFix: [
      'Verify expense report completeness in Workday',
      'Add validation rule to prevent submission without GL codes',
      'Run reconciliation for missing codes',
      'Contact expense report owners for clarification',
    ],
    severity: 'warning',
    timestamp: hoursAgo(5),
    occurrenceCount: 8,
    affectedPipeline: 'Expense Report Integration',
  },
  {
    id: 'err-004',
    title: 'Data Validation Warning: Currency Mismatch',
    description: 'Some transaction records have currency codes that do not match the company standard.',
    reason: 'International transactions are using inconsistent currency formats. This may cause reporting discrepancies.',
    sourceSystem: 'Oracle Financials',
    suggestedFix: [
      'Review transaction currency settings',
      'Update currency mapping table',
      'Re-process affected transactions',
      'Implement real-time currency validation',
    ],
    severity: 'info',
    timestamp: hoursAgo(8),
    occurrenceCount: 24,
    affectedPipeline: 'GL Balance Reconciliation',
  },
  {
    id: 'err-005',
    title: 'API Rate Limit Exceeded',
    description: 'Salesforce API calls exceeded the hourly rate limit.',
    reason: 'Too many concurrent requests to Salesforce. The integration made more calls than allowed by the API quota.',
    sourceSystem: 'Salesforce CRM',
    suggestedFix: [
      'Implement exponential backoff retry logic',
      'Reduce frequency of sync operations',
      'Request increased API limit from Salesforce',
      'Batch operations to reduce call count',
    ],
    severity: 'critical',
    timestamp: hoursAgo(2),
    occurrenceCount: 2,
    affectedPipeline: 'Customer Transaction Processing',
  },
  {
    id: 'err-006',
    title: 'Schema Change Detected',
    description: 'Source data schema has changed, causing mapping failures.',
    reason: 'A new field was added to the source system without updating the pipeline field mapping.',
    sourceSystem: 'AWS S3',
    suggestedFix: [
      'Review new fields in source data',
      'Update pipeline field mappings',
      'Add new fields to transformation logic',
      'Document schema change in governance log',
    ],
    severity: 'info',
    timestamp: daysAgo(1),
    occurrenceCount: 1,
    affectedPipeline: 'Inventory Valuation Update',
  },
];

export const generateSourceSystems = (): SourceSystem[] => [
  { id: 'src-001', name: 'SAP ERP', type: 'ERP System', connectionHealth: 'healthy', lastSyncTime: minutesAgo(15), recordCount: 1250000 },
  { id: 'src-002', name: 'Salesforce CRM', type: 'CRM Platform', connectionHealth: 'degraded', lastSyncTime: hoursAgo(2), recordCount: 890000 },
  { id: 'src-003', name: 'Oracle Financials', type: 'Financial System', connectionHealth: 'healthy', lastSyncTime: minutesAgo(30), recordCount: 560000 },
  { id: 'src-004', name: 'Workday HCM', type: 'HR System', connectionHealth: 'healthy', lastSyncTime: hoursAgo(1), recordCount: 340000 },
  { id: 'src-005', name: 'AWS S3', type: 'Data Lake', connectionHealth: 'healthy', lastSyncTime: minutesAgo(5), recordCount: 2100000 },
];

export const generateEntityInventory = (): EntityInventory[] => [
  { layer: 'Landing', entityCount: 245, lastUpdated: minutesAgo(5) },
  { layer: 'Bronze', entityCount: 189, lastUpdated: minutesAgo(15) },
  { layer: 'Silver', entityCount: 156, lastUpdated: minutesAgo(30) },
  { layer: 'Gold', entityCount: 87, lastUpdated: hoursAgo(1) },
];

export const generateHealthScorecard = (): HealthScorecard => ({
  successRate: 94.7,
  dataFreshness: 0.5,
  averageRuntime: 42,
  totalPipelines: 156,
  activePipelines: 3,
  failedPipelines: 8,
});

export const generateDataLineage = (): { nodes: DataLineageNode[]; connections: DataLineageConnection[] } => {
  const nodes: DataLineageNode[] = [
    { id: 'src-sap', name: 'SAP ERP', layer: 'source', type: 'source' },
    { id: 'src-salesforce', name: 'Salesforce CRM', layer: 'source', type: 'source' },
    { id: 'src-oracle', name: 'Oracle Financials', layer: 'source', type: 'source' },
    { id: 'src-workday', name: 'Workday HCM', layer: 'source', type: 'source' },
    { id: 'src-s3', name: 'AWS S3', layer: 'source', type: 'source' },
    { id: 'landing-revenue', name: 'Revenue Data', layer: 'landing', type: 'landing' },
    { id: 'landing-customer', name: 'Customer Data', layer: 'landing', type: 'landing' },
    { id: 'landing-expense', name: 'Expense Data', layer: 'landing', type: 'landing' },
    { id: 'bronze-revenue', name: 'Revenue Cleaned', layer: 'bronze', type: 'bronze' },
    { id: 'bronze-customer', name: 'Customer Standardized', layer: 'bronze', type: 'bronze' },
    { id: 'silver-revenue', name: 'Revenue Enriched', layer: 'silver', type: 'silver' },
    { id: 'silver-customer', name: 'Customer Aggregated', layer: 'silver', type: 'silver' },
    { id: 'gold-revenue', name: 'Revenue KPIs', layer: 'gold', type: 'gold' },
    { id: 'gold-customer', name: 'Customer Analytics', layer: 'gold', type: 'gold' },
  ];

  const connections: DataLineageConnection[] = [
    { source: 'src-sap', target: 'landing-revenue', flow: 45000 },
    { source: 'src-salesforce', target: 'landing-customer', flow: 23000 },
    { source: 'src-oracle', target: 'landing-expense', flow: 12000 },
    { source: 'src-workday', target: 'landing-customer', flow: 8000 },
    { source: 'src-s3', target: 'landing-revenue', flow: 35000 },
    { source: 'landing-revenue', target: 'bronze-revenue', flow: 44000 },
    { source: 'landing-customer', target: 'bronze-customer', flow: 22000 },
    { source: 'bronze-revenue', target: 'silver-revenue', flow: 42000 },
    { source: 'bronze-customer', target: 'silver-customer', flow: 21000 },
    { source: 'silver-revenue', target: 'gold-revenue', flow: 40000 },
    { source: 'silver-customer', target: 'gold-customer', flow: 20000 },
  ];

  return { nodes, connections };
};
