import { useState, useEffect, useRef } from "react";
import { mockErrors, errorPatterns, errorSummaries } from "@/lib/mockData";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  AlertTriangle, Info, AlertCircle, CheckCircle,
  ChevronDown, ChevronUp, ChevronRight, Server, Wrench, Clock, Repeat, Eye, EyeOff,
  MessageSquare, Brain, Zap, BarChart3,
} from "lucide-react";
import { cn } from "@/lib/utils";
import type { ErrorSeverity, ErrorSummary } from "@/types/dashboard";

// ===== AI Analysis Mock Responses (replace with real Claude API later) =====
const aiAnalysisResponses: Record<string, string> = {
  'err-001': `This is a recurring connection timeout to your SAP ERP system, and I've seen this pattern 3 times in the last 24 hours — each during the 8–10 AM window when SAP transaction volumes peak.

**Root cause analysis:** The 30-second timeout threshold is too aggressive for the Monthly Revenue Aggregation pipeline, which pulls ~45K records. SAP's response time degrades under load, and your extraction window collides with end-user activity.

**My recommendation:** Shift this pipeline's schedule to 5:00 AM UTC (before business hours), and increase the timeout to 90 seconds as a safety margin. If the issue persists after rescheduling, the SAP Basis team should investigate RFC connection pool sizing — this is a known bottleneck pattern with SAP RFC destinations under concurrent load.`,

  'err-002': `The duplicate customer records are a data quality issue originating in Salesforce, not a pipeline bug. I've traced the pattern: 12 occurrences all involve records where the Account Name matches but the Account ID differs — classic CRM merge failure.

**What's happening:** Sales reps are creating new accounts instead of updating existing ones, likely because Salesforce duplicate detection rules aren't enforcing strict matching on company name + domain.

**Impact:** These duplicates cascade through your Customer Transaction Processing pipeline into the Silver layer, where they inflate customer counts by approximately 2.3%. Your Gold layer KPIs will be slightly skewed until this is resolved.

**Fix path:** Enable Salesforce Duplicate Management rules with "Block" action (not just "Alert"), then run a one-time dedup job on the existing ~12 problematic records. I can help you draft the SOQL query to identify all affected records if needed.`,

  'err-003': `This is a data completeness issue — Workday is sending expense reports without GL account codes, which your Expense Report Integration pipeline correctly flags as a validation failure.

**Pattern:** All 8 occurrences are from the same cost center (CC-4200, Engineering). It looks like a recent Workday configuration change removed the GL code requirement for that cost center's expense type.

**Business impact:** Low — these are pending expense reports that haven't been approved yet. No financial data is corrupted, but the records are queued in your Bronze layer waiting for remediation.

**Quick fix:** Contact the Workday admin to restore the GL code requirement for CC-4200. For the 8 stuck records, you can run the pipeline's manual remediation flow to prompt expense owners for the missing codes.`,

  'err-004': `Currency mismatches are a common issue with international Oracle Financials data. I've analyzed the 24 occurrences and found they all involve transactions from 3 subsidiaries: UK (GBP), Germany (EUR), and Japan (JPY).

**The actual problem:** Your currency mapping table hasn't been updated since Q3 2025 and is missing the new ISO 4217 format that Oracle adopted in their January 2026 patch. The codes are technically valid but don't match your pipeline's expected format.

**This is low priority** — the transactions are still processing, just flagged. Update the currency mapping table with the new Oracle format codes and the warnings will clear on the next run.`,

  'err-005': `You're hitting Salesforce's API rate limit, which for your org tier is 100,000 calls per 24-hour rolling window. The Customer Transaction Processing pipeline made 87,000 calls in a 6-hour window — that's the problem.

**Why now:** Your Salesforce record count has grown 23% since this pipeline was configured. The original batch size and polling frequency were designed for ~720K records, but you're now at ~890K.

**Fix:** Implement bulk API calls instead of individual record queries. Switching from REST API to Salesforce Bulk API 2.0 would reduce your call count by ~95%. I'd also recommend adding exponential backoff as a safety net. This is the single highest-impact optimization you can make for this pipeline.`,

  'err-006': `A schema change was detected in your AWS S3 landing zone for the Inventory Valuation pipeline. A new field \`warehouse_zone_id\` appeared in the latest data drop.

**This is informational, not an error.** Your pipeline handled it gracefully by logging the change and continuing with the existing field mapping. The new field was simply ignored.

**Action needed:** If \`warehouse_zone_id\` is business-relevant (check with the warehouse team), add it to your Bronze layer schema and transformation logic. If not, no action needed — the pipeline will continue to work fine without it.`,
};

const weeklyInsight = `I've analyzed **47 pipeline errors** across your FMD framework this week. Here's what I found:

**3 critical patterns** are driving 78% of all failures:
1. **SAP connection timeouts** (3 occurrences) — all during peak business hours. Rescheduling to off-peak would eliminate these entirely.
2. **Salesforce API rate limiting** (2 occurrences) — your record volume has outgrown the REST API approach. Switching to Bulk API 2.0 is overdue.
3. **CRM duplicate records** (12 occurrences) — a Salesforce configuration gap, not a pipeline issue.

**Good news:** Your overall pipeline success rate is still 94.7%, and none of these issues caused data corruption. The fixes are all operational — no code changes needed in the FMD framework itself.`;

const severityConfig: Record<ErrorSeverity, { label: string; color: string; bgColor: string; borderColor: string; icon: React.ElementType }> = {
  critical: {
    label: 'Critical',
    color: 'text-red-700 dark:text-red-400',
    bgColor: 'bg-red-50 dark:bg-red-950/20',
    borderColor: 'border-red-200 dark:border-red-800',
    icon: AlertCircle,
  },
  warning: {
    label: 'Warning',
    color: 'text-amber-700 dark:text-amber-400',
    bgColor: 'bg-amber-50 dark:bg-amber-950/20',
    borderColor: 'border-amber-200 dark:border-amber-800',
    icon: AlertTriangle,
  },
  info: {
    label: 'Info',
    color: 'text-blue-700 dark:text-blue-400',
    bgColor: 'bg-blue-50 dark:bg-blue-950/20',
    borderColor: 'border-blue-200 dark:border-blue-800',
    icon: Info,
  },
};

// ===== AI Streaming Simulation Hook =====
function useAiStream(text: string, enabled: boolean) {
  const [displayed, setDisplayed] = useState("");
  const [isStreaming, setIsStreaming] = useState(false);
  const [isThinking, setIsThinking] = useState(false);

  useEffect(() => {
    if (!enabled) {
      setDisplayed("");
      setIsStreaming(false);
      setIsThinking(false);
      return;
    }

    setIsThinking(true);
    setDisplayed("");

    // Simulate thinking delay
    const thinkTimer = setTimeout(() => {
      setIsThinking(false);
      setIsStreaming(true);

      let i = 0;
      const interval = setInterval(() => {
        // Stream in chunks of 3-8 chars for natural feel
        const chunkSize = Math.floor(Math.random() * 6) + 3;
        i += chunkSize;
        if (i >= text.length) {
          setDisplayed(text);
          setIsStreaming(false);
          clearInterval(interval);
        } else {
          setDisplayed(text.slice(0, i));
        }
      }, 20);

      return () => clearInterval(interval);
    }, 1200);

    return () => clearTimeout(thinkTimer);
  }, [text, enabled]);

  return { displayed, isStreaming, isThinking };
}

// ===== Thinking Dots Component =====
function ThinkingDots() {
  return (
    <div className="flex items-center gap-1.5 py-2">
      <div className="w-2 h-2 rounded-full bg-primary animate-[cl-thinking_1.4s_ease-in-out_infinite]" />
      <div className="w-2 h-2 rounded-full bg-primary animate-[cl-thinking_1.4s_ease-in-out_0.2s_infinite]" />
      <div className="w-2 h-2 rounded-full bg-primary animate-[cl-thinking_1.4s_ease-in-out_0.4s_infinite]" />
    </div>
  );
}

// ===== AI Message Component =====
function AiMessage({ text, isStreaming }: { text: string; isStreaming?: boolean }) {
  // Parse simple markdown bold
  const parts = text.split(/(\*\*[^*]+\*\*)/g);
  return (
    <div className="font-serif text-[15px] leading-[1.65] text-foreground/90 whitespace-pre-line">
      {parts.map((part, i) =>
        part.startsWith('**') && part.endsWith('**') ? (
          <strong key={i} className="font-semibold text-foreground">{part.slice(2, -2)}</strong>
        ) : (
          <span key={i}>{part}</span>
        )
      )}
      {isStreaming && <span className="inline-block w-0.5 h-4 bg-primary ml-0.5 animate-[cl-pulse_1s_ease-in-out_infinite]" />}
    </div>
  );
}

export default function ErrorIntelligence() {
  const [expandedError, setExpandedError] = useState<string | null>(null);
  const [showRawError, setShowRawError] = useState<{ [key: string]: boolean }>({});
  const [filterSeverity, setFilterSeverity] = useState<ErrorSeverity | 'all'>('all');
  const [aiAnalysis, setAiAnalysis] = useState<{ [key: string]: boolean }>({});
  const [showWeeklyInsight, setShowWeeklyInsight] = useState(false);
  const [weeklyExpanded, setWeeklyExpanded] = useState(false);
  const [askInput, setAskInput] = useState("");
  const [hasLiveData, setHasLiveData] = useState<boolean | null>(null);
  const [demoMode, setDemoMode] = useState(false);

  // Check if live execution data exists
  useEffect(() => {
    fetch("http://localhost:8787/api/pipeline-executions")
      .then((r) => r.json())
      .then((data) => setHasLiveData(Array.isArray(data) && data.length > 0))
      .catch(() => setHasLiveData(false));
  }, []);

  const showMockData = demoMode || hasLiveData === true;

  const filteredErrors = !showMockData ? [] : filterSeverity === 'all'
    ? errorSummaries
    : errorSummaries.filter(e => e.severity === filterSeverity);

  const totalErrors = !showMockData ? 0 : errorSummaries.reduce((acc, e) => acc + e.occurrenceCount, 0);
  const criticalCount = !showMockData ? 0 : errorSummaries.filter(e => e.severity === 'critical').length;
  const warningCount = !showMockData ? 0 : errorSummaries.filter(e => e.severity === 'warning').length;
  const infoCount = !showMockData ? 0 : errorSummaries.filter(e => e.severity === 'info').length;

  const weeklyStream = useAiStream(weeklyInsight, showWeeklyInsight);

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
    <div className="space-y-8">
      {/* Page Header */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-xl font-display font-semibold tracking-tight text-foreground">Error Intelligence</h1>
          <p className="text-sm text-muted-foreground mt-1">AI-powered diagnostics and resolution guidance</p>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant="outline" className="gap-1.5 py-1">
            <Brain className="h-3 w-3" />
            Claude Opus 4.6
          </Badge>
          {showMockData && (
            <Badge variant="success" className="gap-1.5 py-1">
              <Zap className="h-3 w-3" />
              {errorSummaries.length} errors analyzed
            </Badge>
          )}
          {demoMode && (
            <Badge variant="outline" className="gap-1.5 py-1 text-amber-600 dark:text-amber-400 border-amber-300 dark:border-amber-700">
              Demo Mode
            </Badge>
          )}
        </div>
      </div>

      {/* ===== No Live Data Banner ===== */}
      {hasLiveData === false && !demoMode && (
        <div className="rounded-[var(--radius-xl)] border border-blue-200 dark:border-blue-800/50 bg-blue-50/50 dark:bg-blue-950/20 p-6">
          <div className="flex items-start gap-4">
            <div className="w-10 h-10 rounded-lg bg-blue-100 dark:bg-blue-900/30 flex items-center justify-center flex-shrink-0">
              <Info className="w-5 h-5 text-blue-600 dark:text-blue-400" />
            </div>
            <div className="flex-1">
              <h3 className="text-sm font-semibold text-foreground">Awaiting First Pipeline Execution</h3>
              <p className="text-sm text-muted-foreground mt-1">
                Error intelligence will activate once pipelines start running. The framework's logging tables
                (<span className="font-mono text-xs">logging.PipelineExecution</span>, <span className="font-mono text-xs">logging.CopyActivityExecution</span>, <span className="font-mono text-xs">logging.NotebookExecution</span>)
                are ready and will capture execution data, errors, and performance metrics automatically.
              </p>
              <button
                className="mt-3 text-xs font-medium text-primary hover:text-primary/80 transition-colors"
                onClick={() => setDemoMode(true)}
              >
                Preview with demo data →
              </button>
            </div>
          </div>
        </div>
      )}

      {/* ===== Claude Weekly Insights ===== */}
      <div className="rounded-[var(--radius-xl)] border border-border bg-card overflow-hidden shadow-[var(--shadow-card)]">
        <div
          className="flex items-center gap-3 px-5 py-3 bg-muted/50 cursor-pointer hover:bg-muted/70 transition-colors"
          onClick={() => { setWeeklyExpanded(!weeklyExpanded); if (!showWeeklyInsight) setShowWeeklyInsight(true); }}
        >
          <div className="h-7 w-7 rounded-[var(--radius-md)] bg-primary/10 border border-primary/20 flex items-center justify-center">
            <BarChart3 className="h-4 w-4 text-primary" />
          </div>
          <div className="flex-1">
            <span className="text-sm font-medium text-foreground">Weekly Error Analysis</span>
            <span className="text-[10px] text-muted-foreground ml-2 font-mono">auto-generated</span>
          </div>
          <span className="text-[10px] font-mono text-muted-foreground mr-2">Feb 18, 2026</span>
          {weeklyExpanded ? <ChevronDown className="h-4 w-4 text-muted-foreground" /> : <ChevronRight className="h-4 w-4 text-muted-foreground" />}
        </div>
        {weeklyExpanded && (
          <div className="p-5 border-t border-border">
            {weeklyStream.isThinking ? (
              <div className="flex items-center gap-3">
                <ThinkingDots />
                <span className="text-sm text-muted-foreground">Analyzing pipeline errors...</span>
              </div>
            ) : (
              <AiMessage text={weeklyStream.displayed} isStreaming={weeklyStream.isStreaming} />
            )}
          </div>
        )}
      </div>

      {/* ===== Summary Cards ===== */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <Card className="p-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider">Total Errors</p>
              <p className="text-2xl font-display font-semibold text-foreground mt-1">{totalErrors}</p>
            </div>
            <div className="w-10 h-10 bg-muted rounded-[var(--radius-md)] flex items-center justify-center">
              <AlertCircle className="w-5 h-5 text-muted-foreground" />
            </div>
          </div>
        </Card>
        <Card className={cn("p-4 border-red-200 dark:border-red-800/50")}>
          <div className="flex items-center justify-between">
            <div>
              <p className="text-[10px] font-medium text-red-600 dark:text-red-400 uppercase tracking-wider">Critical</p>
              <p className="text-2xl font-display font-semibold text-red-700 dark:text-red-400 mt-1">{criticalCount}</p>
            </div>
            <div className="w-10 h-10 bg-red-50 dark:bg-red-950/30 rounded-[var(--radius-md)] flex items-center justify-center">
              <AlertCircle className="w-5 h-5 text-red-600 dark:text-red-400" />
            </div>
          </div>
        </Card>
        <Card className={cn("p-4 border-amber-200 dark:border-amber-800/50")}>
          <div className="flex items-center justify-between">
            <div>
              <p className="text-[10px] font-medium text-amber-600 dark:text-amber-400 uppercase tracking-wider">Warnings</p>
              <p className="text-2xl font-display font-semibold text-amber-700 dark:text-amber-400 mt-1">{warningCount}</p>
            </div>
            <div className="w-10 h-10 bg-amber-50 dark:bg-amber-950/30 rounded-[var(--radius-md)] flex items-center justify-center">
              <AlertTriangle className="w-5 h-5 text-amber-600 dark:text-amber-400" />
            </div>
          </div>
        </Card>
        <Card className={cn("p-4 border-blue-200 dark:border-blue-800/50")}>
          <div className="flex items-center justify-between">
            <div>
              <p className="text-[10px] font-medium text-blue-600 dark:text-blue-400 uppercase tracking-wider">Info</p>
              <p className="text-2xl font-display font-semibold text-blue-700 dark:text-blue-400 mt-1">{infoCount}</p>
            </div>
            <div className="w-10 h-10 bg-blue-50 dark:bg-blue-950/30 rounded-[var(--radius-md)] flex items-center justify-center">
              <Info className="w-5 h-5 text-blue-600 dark:text-blue-400" />
            </div>
          </div>
        </Card>
      </div>

      {/* ===== MINIMAX Error Pattern Analysis - PRESERVED ===== */}
      {showMockData && <Card className="p-5">
        <h2 className="text-sm font-semibold text-foreground mb-4">Error Pattern Analysis</h2>
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
          {errorPatterns.map((pattern, index) => (
            <div key={index} className="bg-muted rounded-[var(--radius-md)] p-3 border border-border">
              <p className="text-xs font-medium text-foreground">{pattern.pattern}</p>
              <p className="text-xl font-display font-semibold text-foreground/80 mt-2">{pattern.count}</p>
              <p className="text-[10px] text-muted-foreground mt-0.5 uppercase tracking-wider">occurrences</p>
            </div>
          ))}
        </div>
      </Card>}

      {/* Severity Filter */}
      <div className="flex items-center gap-2">
        <span className="text-xs font-medium text-muted-foreground">Filter:</span>
        {(['all', 'critical', 'warning', 'info'] as const).map((severity) => {
          const isActive = filterSeverity === severity;
          return (
            <button
              key={severity}
              onClick={() => setFilterSeverity(severity)}
              className={cn(
                "px-3 py-1.5 rounded-full text-xs font-medium transition-all duration-[var(--duration-fast)]",
                isActive
                  ? severity === 'all'
                    ? 'bg-foreground text-background'
                    : severity === 'critical'
                      ? 'bg-red-50 text-red-700 dark:bg-red-950/30 dark:text-red-400'
                      : severity === 'warning'
                        ? 'bg-amber-50 text-amber-700 dark:bg-amber-950/30 dark:text-amber-400'
                        : 'bg-blue-50 text-blue-700 dark:bg-blue-950/30 dark:text-blue-400'
                  : 'bg-muted text-muted-foreground hover:bg-accent'
              )}
            >
              {severity === 'all' ? 'All' : severityConfig[severity].label}
            </button>
          );
        })}
      </div>

      {/* ===== Error Cards with AI Analysis ===== */}
      <div className="space-y-3">
        {filteredErrors.map((error) => {
          const config = severityConfig[error.severity];
          const SeverityIcon = config.icon;
          const isExpanded = expandedError === error.id;
          const showAi = aiAnalysis[error.id] || false;

          return (
            <ErrorCard
              key={error.id}
              error={error}
              config={config}
              SeverityIcon={SeverityIcon}
              isExpanded={isExpanded}
              showAi={showAi}
              showRawError={showRawError[error.id] || false}
              formatTimeAgo={formatTimeAgo}
              onToggleExpand={() => setExpandedError(isExpanded ? null : error.id)}
              onToggleRaw={() => setShowRawError(prev => ({ ...prev, [error.id]: !prev[error.id] }))}
              onToggleAi={() => setAiAnalysis(prev => ({ ...prev, [error.id]: !prev[error.id] }))}
            />
          );
        })}
      </div>

      {filteredErrors.length === 0 && (
        <Card className="p-12 text-center">
          <div className="w-14 h-14 bg-emerald-50 dark:bg-emerald-950/30 rounded-full flex items-center justify-center mx-auto">
            <CheckCircle className="w-7 h-7 text-emerald-600 dark:text-emerald-400" />
          </div>
          <h3 className="text-sm font-semibold text-foreground mt-4">All Clear</h3>
          <p className="text-xs text-muted-foreground mt-1">
            {filterSeverity === 'all'
              ? 'All pipelines are running successfully.'
              : `No ${filterSeverity} errors found.`}
          </p>
        </Card>
      )}
    </div>
  );
}

// ===== Error Card Component =====
function ErrorCard({
  error, config, SeverityIcon, isExpanded, showAi, showRawError, formatTimeAgo,
  onToggleExpand, onToggleRaw, onToggleAi,
}: {
  error: ErrorSummary;
  config: { label: string; color: string; bgColor: string; borderColor: string };
  SeverityIcon: React.ElementType;
  isExpanded: boolean;
  showAi: boolean;
  showRawError: boolean;
  formatTimeAgo: (date: Date) => string;
  onToggleExpand: () => void;
  onToggleRaw: () => void;
  onToggleAi: () => void;
}) {
  const aiText = aiAnalysisResponses[error.id] || "No analysis available for this error yet.";
  const aiStream = useAiStream(aiText, showAi);

  return (
    <div className={cn(
      "rounded-[var(--radius-xl)] border overflow-hidden transition-all duration-[var(--duration-normal)]",
      config.borderColor,
      isExpanded ? 'shadow-[var(--shadow-card-hover)]' : 'shadow-[var(--shadow-card)]'
    )}>
      {/* Error Header */}
      <div
        className={cn("p-4 cursor-pointer", config.bgColor)}
        onClick={onToggleExpand}
      >
        <div className="flex items-start justify-between gap-4">
          <div className="flex items-start gap-3 min-w-0">
            <div className={cn(
              "w-9 h-9 rounded-[var(--radius-md)] flex items-center justify-center flex-shrink-0",
              error.severity === 'critical' ? 'bg-red-100 dark:bg-red-950/40' :
              error.severity === 'warning' ? 'bg-amber-100 dark:bg-amber-950/40' : 'bg-blue-100 dark:bg-blue-950/40'
            )}>
              <SeverityIcon className={cn("w-4 h-4", config.color)} />
            </div>
            <div className="min-w-0">
              <div className="flex items-center gap-2 flex-wrap">
                <h3 className="text-sm font-semibold text-foreground">{error.title}</h3>
                <Badge variant="outline" className={cn("text-[10px]", config.color)}>
                  {config.label}
                </Badge>
                {error.occurrenceCount > 1 && (
                  <Badge variant="secondary" className="text-[10px] gap-0.5">
                    <Repeat className="w-2.5 h-2.5" />
                    {error.occurrenceCount}x
                  </Badge>
                )}
              </div>
              <p className="text-xs text-muted-foreground mt-1 line-clamp-1">{error.description}</p>
            </div>
          </div>
          <div className="flex items-center gap-3 flex-shrink-0">
            <div className="text-right hidden sm:block">
              <p className="text-[10px] text-muted-foreground font-mono flex items-center justify-end">
                <Clock className="w-3 h-3 mr-1" />
                {formatTimeAgo(error.timestamp)}
              </p>
              <p className="text-[10px] text-muted-foreground mt-0.5">{error.affectedPipeline}</p>
            </div>
            {isExpanded ? (
              <ChevronUp className="w-4 h-4 text-muted-foreground" />
            ) : (
              <ChevronDown className="w-4 h-4 text-muted-foreground" />
            )}
          </div>
        </div>
      </div>

      {/* Expanded Content */}
      {isExpanded && (
        <div className="border-t border-border">
          <div className="p-4">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
              {/* Analysis */}
              <div>
                <h4 className="text-xs font-semibold text-foreground mb-3">Analysis</h4>
                <div className="space-y-3">
                  <div>
                    <p className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-1">What Happened</p>
                    <p className="text-xs text-foreground/80 leading-relaxed">{error.description}</p>
                  </div>
                  <div>
                    <p className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-1">Why It Happened</p>
                    <p className="text-xs text-foreground/80 leading-relaxed">{error.reason}</p>
                  </div>
                </div>
              </div>

              {/* Resolution */}
              <div>
                <h4 className="text-xs font-semibold text-foreground mb-3">Resolution</h4>
                <div className="space-y-3">
                  <div className="flex items-center p-2.5 bg-muted rounded-[var(--radius-md)]">
                    <Server className="w-4 h-4 text-muted-foreground mr-2.5" />
                    <div>
                      <p className="text-[10px] text-muted-foreground">Source System</p>
                      <p className="text-xs font-medium text-foreground">{error.sourceSystem}</p>
                    </div>
                  </div>
                  <div>
                    <p className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-2 flex items-center">
                      <Wrench className="w-3 h-3 mr-1" />
                      Suggested Fix
                    </p>
                    <ol className="space-y-1.5">
                      {error.suggestedFix.map((fix, index) => (
                        <li key={index} className="flex items-start text-xs">
                          <span className="flex-shrink-0 w-4 h-4 bg-emerald-50 dark:bg-emerald-950/30 text-emerald-700 dark:text-emerald-400 rounded-full flex items-center justify-center text-[9px] font-medium mr-2 mt-0.5">
                            {index + 1}
                          </span>
                          <span className="text-foreground/80">{fix}</span>
                        </li>
                      ))}
                    </ol>
                  </div>
                </div>
              </div>
            </div>

            {/* Action Bar */}
            <div className="mt-4 pt-3 border-t border-border flex items-center justify-between">
              <div className="flex items-center gap-2">
                {error.rawError && (
                  <button
                    onClick={(e) => { e.stopPropagation(); onToggleRaw(); }}
                    className="flex items-center text-[10px] text-muted-foreground hover:text-foreground transition-colors"
                  >
                    {showRawError ? <EyeOff className="w-3 h-3 mr-1" /> : <Eye className="w-3 h-3 mr-1" />}
                    {showRawError ? 'Hide' : 'Show'} Technical Details
                  </button>
                )}
              </div>
              <Button
                size="sm"
                onClick={(e) => { e.stopPropagation(); onToggleAi(); }}
                className={cn(
                  "gap-1.5 text-xs h-7",
                  showAi ? "bg-primary/10 text-primary hover:bg-primary/20" : ""
                )}
                variant={showAi ? "ghost" : "default"}
              >
                <MessageSquare className="w-3 h-3" />
                {showAi ? 'Hide AI Analysis' : 'Analyze with Claude'}
              </Button>
            </div>

            {/* Raw Error */}
            {showRawError && error.rawError && (
              <pre className="mt-3 p-3 bg-surface rounded-[var(--radius-md)] text-muted-foreground text-[10px] font-mono overflow-x-auto border border-border">
                {error.rawError}
              </pre>
            )}
          </div>

          {/* AI Analysis Panel */}
          {showAi && (
            <div className="border-t border-border bg-muted/30">
              <div className="flex items-center gap-2 px-4 py-2 border-b border-border">
                <div className="h-5 w-5 rounded-full bg-primary flex items-center justify-center">
                  <Brain className="h-3 w-3 text-primary-foreground" />
                </div>
                <span className="text-[10px] font-medium text-foreground">Claude's Analysis</span>
                {aiStream.isStreaming && (
                  <span className="text-[10px] text-muted-foreground animate-[cl-pulse_2s_ease-in-out_infinite]">streaming...</span>
                )}
              </div>
              <div className="p-4">
                {aiStream.isThinking ? (
                  <div className="flex items-center gap-3">
                    <ThinkingDots />
                    <span className="text-xs text-muted-foreground">Analyzing error context...</span>
                  </div>
                ) : (
                  <AiMessage text={aiStream.displayed} isStreaming={aiStream.isStreaming} />
                )}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
