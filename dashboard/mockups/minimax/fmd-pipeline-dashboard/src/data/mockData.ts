// FMD Pipeline Dashboard - Mock Data Service

import type {
  Pipeline,
  ExecutionTimeline,
  ErrorSummary,
  SourceSystem,
  EntityInventory,
  HealthScorecard,
  DataLineageNode,
  DataLineageConnection,
  PipelineMonitorViewData,
  ErrorIntelligenceViewData,
  AdminGovernanceViewData,
  PipelineLayer,
  PipelineStatus,
  ErrorSeverity,
  ConnectionHealth,
} from '../types/dashboard';

// Helper to generate dates
const daysAgo = (days: number): Date => {
  const date = new Date();
  date.setDate(date.getDate() - days);
  return date;
};

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

// Business-friendly layer names
const layerNames: Record<PipelineLayer, string> = {
  ingestion: 'Ingestion',
  standardization: 'Standardization',
  business_ready: 'Business Ready',
  analytics: 'Analytics',
};

// Source systems with business names
const sourceSystems = ['SAP ERP', 'Salesforce CRM', 'Oracle Financials', 'Workday HCM', 'AWS S3'];

const pipelineNames = [
  'Monthly Revenue Aggregation',
  'Customer Transaction Processing',
  'GL Balance Reconciliation',
  'Expense Report Integration',
  'Inventory Valuation Update',
  'Account Receivables Sync',
  'Payroll Data Pipeline',
  'Tax Calculation Engine',
  'Budget Variance Analysis',
  'Cash Flow Forecasting',
];

// Generate active pipelines
export const generateActivePipelines = (): Pipeline[] => {
  const activePipelines: Pipeline[] = [
    {
      id: 'pipe-001',
      name: 'Monthly Revenue Aggregation',
      status: 'running',
      layer: 'analytics',
      sourceSystem: 'SAP ERP',
      startTime: minutesAgo(25),
      recordsProcessed: 45820,
    },
    {
      id: 'pipe-002',
      name: 'Customer Transaction Processing',
      status: 'running',
      layer: 'business_ready',
      sourceSystem: 'Salesforce CRM',
      startTime: minutesAgo(12),
      recordsProcessed: 2340,
    },
    {
      id: 'pipe-003',
      name: 'GL Balance Reconciliation',
      status: 'running',
      layer: 'standardization',
      sourceSystem: 'Oracle Financials',
      startTime: minutesAgo(8),
      recordsProcessed: 890,
    },
  ];

  return activePipelines;
};

// Generate execution timeline
export const generateExecutionTimeline = (): ExecutionTimeline[] => {
  return [
    {
      layer: 'ingestion',
      status: 'success',
      startTime: hoursAgo(2),
      endTime: hoursAgo(1.8),
      progress: 100,
    },
    {
      layer: 'standardization',
      status: 'success',
      startTime: hoursAgo(1.8),
      endTime: hoursAgo(1.2),
      progress: 100,
    },
    {
      layer: 'business_ready',
      status: 'success',
      startTime: hoursAgo(1.2),
      endTime: minutesAgo(35),
      progress: 100,
    },
    {
      layer: 'analytics',
      status: 'running',
      startTime: minutesAgo(25),
      progress: 68,
    },
  ];
};

// Generate run history
export const generateRunHistory = (): Pipeline[] => {
  const history: Pipeline[] = [];

  // Generate 50 historical runs
  for (let i = 0; i < 50; i++) {
    const status: PipelineStatus = Math.random() > 0.15
      ? 'success'
      : Math.random() > 0.5 ? 'failed' : 'cancelled';

    const layer: PipelineLayer = ['ingestion', 'standardization', 'business_ready', 'analytics'][
      Math.floor(Math.random() * 4)
    ] as PipelineLayer;

    const startTime = hoursAgo(Math.random() * 168); // Last week
    const duration = Math.floor(Math.random() * 120) + 5; // 5-125 minutes

    history.push({
      id: `history-${i.toString().padStart(3, '0')}`,
      name: pipelineNames[i % pipelineNames.length],
      status,
      layer,
      sourceSystem: sourceSystems[i % sourceSystems.length],
      startTime,
      endTime: new Date(startTime.getTime() + duration * 60000),
      duration,
      recordsProcessed: Math.floor(Math.random() * 100000) + 1000,
      errorCount: status === 'failed' ? Math.floor(Math.random() * 10) + 1 : 0,
    });
  }

  // Sort by most recent first
  return history.sort((a, b) => b.startTime.getTime() - a.startTime.getTime());
};

// Generate duration trend data (last 30 days)
export const generateDurationTrend = () => {
  const trend = [];
  for (let i = 29; i >= 0; i--) {
    const date = new Date();
    date.setDate(date.getDate() - i);
    trend.push({
      date: date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
      duration: Math.floor(Math.random() * 60) + 30, // 30-90 minutes
    });
  }
  return trend;
};

// Generate success rate trend
export const generateSuccessRateTrend = () => {
  const trend = [];
  for (let i = 29; i >= 0; i--) {
    const date = new Date();
    date.setDate(date.getDate() - i);
    trend.push({
      date: date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
      rate: Math.floor(Math.random() * 15) + 85, // 85-100%
    });
  }
  return trend;
};

// Generate error summaries
export const generateErrorSummaries = (): ErrorSummary[] => {
  const errors: ErrorSummary[] = [
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

  return errors.sort((a, b) => {
    const severityOrder: Record<ErrorSeverity, number> = { critical: 0, warning: 1, info: 2 };
    return severityOrder[a.severity] - severityOrder[b.severity];
  });
};

// Generate source systems
export const generateSourceSystems = (): SourceSystem[] => {
  return [
    {
      id: 'src-001',
      name: 'SAP ERP',
      type: 'ERP System',
      connectionHealth: 'healthy',
      lastSyncTime: minutesAgo(15),
      recordCount: 1250000,
    },
    {
      id: 'src-002',
      name: 'Salesforce CRM',
      type: 'CRM Platform',
      connectionHealth: 'degraded',
      lastSyncTime: hoursAgo(2),
      recordCount: 890000,
    },
    {
      id: 'src-003',
      name: 'Oracle Financials',
      type: 'Financial System',
      connectionHealth: 'healthy',
      lastSyncTime: minutesAgo(30),
      recordCount: 560000,
    },
    {
      id: 'src-004',
      name: 'Workday HCM',
      type: 'HR System',
      connectionHealth: 'healthy',
      lastSyncTime: hoursAgo(1),
      recordCount: 340000,
    },
    {
      id: 'src-005',
      name: 'AWS S3',
      type: 'Data Lake',
      connectionHealth: 'healthy',
      lastSyncTime: minutesAgo(5),
      recordCount: 2100000,
    },
  ];
};

// Generate entity inventory
export const generateEntityInventory = (): EntityInventory[] => {
  return [
    { layer: 'Landing', entityCount: 245, lastUpdated: minutesAgo(5) },
    { layer: 'Bronze', entityCount: 189, lastUpdated: minutesAgo(15) },
    { layer: 'Silver', entityCount: 156, lastUpdated: minutesAgo(30) },
    { layer: 'Gold', entityCount: 87, lastUpdated: hoursAgo(1) },
  ];
};

// Generate data lineage
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

// Generate health scorecard
export const generateHealthScorecard = (): HealthScorecard => {
  return {
    successRate: 94.7,
    dataFreshness: 0.5,
    averageRuntime: 42,
    totalPipelines: 156,
    activePipelines: 3,
    failedPipelines: 8,
  };
};

// Combined view data generators
export const getPipelineMonitorData = (): PipelineMonitorViewData => ({
  activePipelines: generateActivePipelines(),
  runHistory: generateRunHistory(),
  executionTimeline: generateExecutionTimeline(),
  filters: {
    dateRange: { from: daysAgo(7), to: new Date() },
    sourceSystem: 'all',
    layer: 'all',
  },
  durationTrend: generateDurationTrend(),
  successRateTrend: generateSuccessRateTrend(),
});

export const getErrorIntelligenceData = (): ErrorIntelligenceViewData => {
  const errors = generateErrorSummaries();
  return {
    errors,
    totalErrors: errors.reduce((acc, e) => acc + e.occurrenceCount, 0),
    criticalCount: errors.filter(e => e.severity === 'critical').length,
    warningCount: errors.filter(e => e.severity === 'warning').length,
    infoCount: errors.filter(e => e.severity === 'info').length,
    errorPattern: [
      { pattern: 'Connection Timeout', count: 3 },
      { pattern: 'Duplicate Records', count: 12 },
      { pattern: 'Missing Required Field', count: 8 },
      { pattern: 'Currency Mismatch', count: 24 },
      { pattern: 'API Rate Limit', count: 2 },
    ],
  };
};

export const getAdminGovernanceData = (): AdminGovernanceViewData => ({
  healthScorecard: generateHealthScorecard(),
  sourceSystems: generateSourceSystems(),
  entityInventory: generateEntityInventory(),
  dataLineage: generateDataLineage(),
});

// Get overall system health
export const getSystemHealth = (): 'healthy' | 'degraded' | 'critical' => {
  const scorecard = generateHealthScorecard();
  if (scorecard.successRate >= 95 && scorecard.failedPipelines <= 3) return 'healthy';
  if (scorecard.successRate >= 85 && scorecard.failedPipelines <= 10) return 'degraded';
  return 'critical';
};
