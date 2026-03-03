import { useState, useEffect, useCallback } from "react";
import { Input } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { RefreshCw, Filter, Loader2, XCircle, Clock, Info } from "lucide-react";
import { cn } from "@/lib/utils";
import { Bar, BarChart, ResponsiveContainer, XAxis, YAxis, Tooltip } from "recharts";

// ── Types ──

interface Pipeline {
  PipelineId: string;
  PipelineGuid: string;
  WorkspaceGuid: string;
  Name: string;
  IsActive: string;
}

interface PipelineExecution {
  // These columns will populate once pipelines run
  [key: string]: string | null;
}

interface Workspace {
  WorkspaceId: string;
  WorkspaceGuid: string;
  Name: string;
}

// ── Fabric Icon ──

function FabricIcon({ name, className = 'w-5 h-5' }: { name: string; className?: string }) {
  return <img src={`/icons/${name}.svg`} alt={name} className={className} />;
}

// ── API ──

const API = 'http://localhost:8787/api';

async function fetchJson<T>(path: string): Promise<T> {
  const res = await fetch(`${API}${path}`);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

// ── Pipeline categorization ──

type PipelineCategory = 'Landing Zone' | 'Bronze' | 'Silver' | 'Orchestration' | 'Utility';

function categorize(name: string): PipelineCategory {
  if (name.includes('_LDZ_') || name.includes('LANDING')) return 'Landing Zone';
  if (name.includes('_BRONZE_') || name.includes('_BRZ_')) return 'Bronze';
  if (name.includes('_SILVER_') || name.includes('_SLV_')) return 'Silver';
  if (name.includes('_LOAD_') || name.includes('_TASKFLOW_')) return 'Orchestration';
  return 'Utility';
}

const categoryColors: Record<PipelineCategory, string> = {
  'Landing Zone': 'bg-blue-500',
  'Bronze': 'bg-amber-500',
  'Silver': 'bg-purple-500',
  'Orchestration': 'bg-emerald-500',
  'Utility': 'bg-slate-500',
};

const categoryBadgeColors: Record<PipelineCategory, string> = {
  'Landing Zone': 'text-blue-700 dark:text-blue-300 bg-blue-50 dark:bg-blue-950/20',
  'Bronze': 'text-amber-700 dark:text-amber-300 bg-amber-50 dark:bg-amber-950/20',
  'Silver': 'text-purple-700 dark:text-purple-300 bg-purple-50 dark:bg-purple-950/20',
  'Orchestration': 'text-emerald-700 dark:text-emerald-300 bg-emerald-50 dark:bg-emerald-950/20',
  'Utility': 'text-slate-700 dark:text-slate-300 bg-slate-50 dark:bg-slate-950/20',
};

// ── Medallion progress indicator ──

const layers = ['Landing Zone', 'Bronze', 'Silver', 'Gold'] as const;

function MedallionProgress({ category }: { category: PipelineCategory }) {
  const layerIndex = category === 'Landing Zone' ? 0
    : category === 'Bronze' ? 1
    : category === 'Silver' ? 2
    : category === 'Orchestration' ? 3 : -1;

  return (
    <div className="flex flex-col gap-1.5 mt-3 w-full max-w-md">
      <div className="flex items-center justify-between text-[10px] uppercase tracking-wider text-muted-foreground font-semibold">
        <span>Landing</span>
        <span>Gold</span>
      </div>
      <div className="flex items-center gap-1">
        {layers.map((layer, index) => {
          let color = "bg-muted";
          if (layerIndex >= 0 && index <= layerIndex) {
            color = index === layerIndex ? categoryColors[category] : "bg-emerald-500/80";
          }
          return (
            <div
              key={layer}
              className={cn("h-2 flex-1 rounded-full transition-all duration-500", color)}
              title={layer}
            />
          );
        })}
      </div>
    </div>
  );
}

export default function PipelineMonitor() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [executions, setExecutions] = useState<PipelineExecution[]>([]);
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [searchTerm, setSearchTerm] = useState("");
  const [categoryFilter, setCategoryFilter] = useState<PipelineCategory | "All">("All");

  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [pipes, execs, ws] = await Promise.all([
        fetchJson<Pipeline[]>('/pipelines'),
        fetchJson<PipelineExecution[]>('/pipeline-executions'),
        fetchJson<Workspace[]>('/workspaces'),
      ]);
      setPipelines(pipes);
      setExecutions(execs);
      setWorkspaces(ws);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load data');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadData(); }, [loadData]);

  const activePipelines = pipelines.filter(p => p.IsActive === 'True');

  const filteredPipelines = activePipelines.filter(p => {
    const category = categorize(p.Name);
    const matchesSearch = p.Name.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesCategory = categoryFilter === "All" || category === categoryFilter;
    return matchesSearch && matchesCategory;
  });

  // Category breakdown for the chart
  const categoryCounts = activePipelines.reduce<Record<string, number>>((acc, p) => {
    const cat = categorize(p.Name);
    acc[cat] = (acc[cat] || 0) + 1;
    return acc;
  }, {});

  const chartData = Object.entries(categoryCounts).map(([name, count]) => ({
    name: name === 'Landing Zone' ? 'LDZ' : name === 'Orchestration' ? 'Orch' : name,
    count,
  }));

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <Loader2 className="w-8 h-8 animate-spin text-primary" />
        <span className="ml-3 text-muted-foreground">Loading pipeline data...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center h-96 gap-4">
        <XCircle className="w-12 h-12 text-destructive" />
        <p className="text-destructive font-medium">{error}</p>
        <Button onClick={loadData} variant="outline" className="gap-2">
          <RefreshCw className="w-4 h-4" /> Retry
        </Button>
      </div>
    );
  }

  const hasExecutionData = executions.length > 0;

  return (
    <div className="space-y-8">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-display font-bold tracking-tight text-foreground">Pipeline Monitor</h1>
          <p className="text-muted-foreground mt-1">
            {hasExecutionData
              ? 'Real-time pipeline execution status'
              : `${activePipelines.length} registered pipelines across the FMD framework`
            }
          </p>
        </div>
        <Button onClick={loadData} variant="outline" size="sm" className="gap-2">
          <RefreshCw className="h-4 w-4" />
          Refresh
        </Button>
      </div>

      {/* Stats Overview — LIVE */}
      <div className="grid gap-4 md:grid-cols-4">
        <Card className="bg-card shadow-sm border-border/60 hover:shadow-md transition-shadow">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-1.5">
              <FabricIcon name="pipeline" className="w-4 h-4" />
              Total Pipelines
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold font-display">{activePipelines.length}</div>
            <p className="text-xs text-muted-foreground">{pipelines.length - activePipelines.length} inactive</p>
          </CardContent>
        </Card>
        <Card className="bg-card shadow-sm border-border/60 hover:shadow-md transition-shadow">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-1.5">
              <FabricIcon name="copy_job" className="w-4 h-4" />
              Landing Zone
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold font-display text-blue-600 dark:text-blue-400">{categoryCounts['Landing Zone'] || 0}</div>
            <p className="text-xs text-muted-foreground">Copy & ingestion pipelines</p>
          </CardContent>
        </Card>
        <Card className="bg-card shadow-sm border-border/60 hover:shadow-md transition-shadow">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-1.5">
              <FabricIcon name="notebook" className="w-4 h-4" />
              Transform
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold font-display text-amber-600 dark:text-amber-400">{(categoryCounts['Bronze'] || 0) + (categoryCounts['Silver'] || 0)}</div>
            <p className="text-xs text-muted-foreground">Bronze + Silver pipelines</p>
          </CardContent>
        </Card>
        <Card className="bg-card shadow-sm border-border/60 hover:shadow-md transition-shadow">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-1.5">
              <FabricIcon name="dataflow" className="w-4 h-4" />
              Orchestration
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold font-display text-emerald-600 dark:text-emerald-400">{categoryCounts['Orchestration'] || 0}</div>
            <p className="text-xs text-muted-foreground">Load & taskflow pipelines</p>
          </CardContent>
        </Card>
      </div>

      {/* Execution status banner */}
      {!hasExecutionData && (
        <div className="flex items-center gap-3 p-4 bg-blue-50 dark:bg-blue-950/20 border border-blue-200 dark:border-blue-800 rounded-lg">
          <Info className="w-5 h-5 text-blue-600 dark:text-blue-400 flex-shrink-0" />
          <div>
            <p className="text-sm font-medium text-blue-800 dark:text-blue-200">No execution data yet</p>
            <p className="text-xs text-blue-600 dark:text-blue-400">
              Pipeline execution logs will appear here once pipelines start running. The logging tables (PipelineExecution, CopyActivityExecution, NotebookExecution) are ready.
            </p>
          </div>
        </div>
      )}

      <div className="grid gap-8 md:grid-cols-3">
        {/* Main List */}
        <div className="md:col-span-2 space-y-6">
          <div className="flex flex-col sm:flex-row items-center gap-4 bg-card p-4 rounded-lg border border-border shadow-sm">
            <Input
              placeholder="Search pipelines..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="max-w-sm"
            />
            <Select value={categoryFilter} onValueChange={(v) => setCategoryFilter(v as PipelineCategory | "All")} className="w-[180px]">
              <option value="All">All Categories</option>
              <option value="Landing Zone">Landing Zone</option>
              <option value="Bronze">Bronze</option>
              <option value="Silver">Silver</option>
              <option value="Orchestration">Orchestration</option>
              <option value="Utility">Utility</option>
            </Select>
          </div>

          <div className="space-y-3">
            {filteredPipelines.map((pipeline) => {
              const category = categorize(pipeline.Name);
              const ws = workspaces.find(w => w.WorkspaceGuid === pipeline.WorkspaceGuid);

              return (
                <div
                  key={pipeline.PipelineId}
                  className="group flex flex-col p-4 bg-card rounded-xl border border-border shadow-sm hover:shadow-md transition-all duration-200 hover:border-primary/20"
                >
                  <div className="flex flex-col sm:flex-row sm:items-start justify-between gap-3">
                    <div className="space-y-1.5">
                      <div className="flex items-center gap-3">
                        <FabricIcon name="pipeline" className="w-4 h-4 opacity-70" />
                        <h3 className="font-semibold text-sm font-mono text-foreground group-hover:text-primary transition-colors">{pipeline.Name}</h3>
                        <span className={cn("text-[10px] font-medium px-2 py-0.5 rounded-full", categoryBadgeColors[category])}>
                          {category}
                        </span>
                      </div>
                      <div className="flex items-center gap-4 text-xs text-muted-foreground ml-7">
                        {ws && (
                          <span className="flex items-center gap-1">
                            <FabricIcon name="fabric" className="w-3 h-3" />
                            {ws.Name}
                          </span>
                        )}
                        <span className="text-border">|</span>
                        <span className="font-mono text-[10px]">{pipeline.PipelineGuid.substring(0, 8)}...</span>
                      </div>
                    </div>

                    <div className="flex items-center gap-4 self-end sm:self-auto">
                      {hasExecutionData ? (
                        // TODO: Show real execution stats when available
                        <div className="text-right">
                          <div className="text-sm font-medium text-foreground">-</div>
                          <div className="text-xs text-muted-foreground">Last run</div>
                        </div>
                      ) : (
                        <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                          <Clock className="w-3.5 h-3.5" />
                          <span>Awaiting first run</span>
                        </div>
                      )}
                    </div>
                  </div>

                  <MedallionProgress category={category} />
                </div>
              );
            })}
          </div>

          {filteredPipelines.length === 0 && (
            <div className="text-center py-12 text-muted-foreground">
              <p>No pipelines match your search</p>
            </div>
          )}
        </div>

        {/* Sidebar / Stats */}
        <div className="space-y-6">
          <Card className="bg-card shadow-sm border-border">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <FabricIcon name="data_factory" className="w-5 h-5" />
                Pipeline Distribution
              </CardTitle>
              <CardDescription>By category</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="h-[200px] w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={chartData}>
                    <XAxis dataKey="name" fontSize={11} tickLine={false} axisLine={false} stroke="hsl(var(--muted-foreground))" />
                    <YAxis fontSize={12} tickLine={false} axisLine={false} stroke="hsl(var(--muted-foreground))" />
                    <Tooltip
                      cursor={{ fill: 'transparent' }}
                      contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)', backgroundColor: 'hsl(var(--card))' }}
                    />
                    <Bar dataKey="count" fill="hsl(var(--primary))" radius={[4, 4, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>

          <Card className="bg-gradient-to-br from-primary/5 to-transparent border-primary/10 shadow-sm">
            <CardHeader>
              <CardTitle className="text-primary flex items-center gap-2">
                <FabricIcon name="fabric" className="w-5 h-5" />
                Framework Status
              </CardTitle>
              <CardDescription>Deployment overview</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              {[
                { label: "Pipelines Deployed", value: `${activePipelines.length}`, color: "text-emerald-600 dark:text-emerald-400" },
                { label: "Workspaces", value: `${workspaces.length}`, color: "text-blue-600 dark:text-blue-400" },
                { label: "Execution Logs", value: hasExecutionData ? `${executions.length} entries` : "Awaiting first run", color: hasExecutionData ? "text-emerald-600 dark:text-emerald-400" : "text-muted-foreground" },
              ].map((item) => (
                <div key={item.label} className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">{item.label}</span>
                  <span className={cn("text-sm font-semibold", item.color)}>{item.value}</span>
                </div>
              ))}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
