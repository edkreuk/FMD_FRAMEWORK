import { pipelineRuns, healthMetrics, durationTrend } from "@/data/mockData";
import { PipelineStatusBadge } from "@/components/StatusBadge";
import { Activity, CheckCircle, XCircle, Clock, TrendingUp } from "lucide-react";
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from "recharts";

const statCards = [
  { label: "Pipelines Today", value: healthMetrics.pipelinesRunToday, icon: Activity, accent: "text-primary" },
  { label: "Success Rate", value: `${healthMetrics.overallSuccessRate}%`, icon: CheckCircle, accent: "text-success" },
  { label: "Failed", value: healthMetrics.pipelinesFailed, icon: XCircle, accent: "text-critical" },
  { label: "Avg Duration", value: healthMetrics.avgDuration, icon: Clock, accent: "text-muted-foreground" },
];

const layers = ["Ingestion", "Standardization", "Business Ready", "Analytics"] as const;

export default function PipelineMonitor() {
  return (
    <div className="space-y-6 animate-fade-in">
      <div>
        <h2 className="text-xl font-semibold text-foreground">Pipeline Monitor</h2>
        <p className="text-sm text-muted-foreground mt-1">Real-time overview of all data pipeline activity</p>
      </div>

      {/* Stat cards */}
      <div className="grid grid-cols-4 gap-4">
        {statCards.map((s) => (
          <div key={s.label} className="glass-card p-4">
            <div className="flex items-center justify-between">
              <span className="text-xs text-muted-foreground">{s.label}</span>
              <s.icon className={`h-4 w-4 ${s.accent}`} />
            </div>
            <p className="mt-2 text-2xl font-semibold font-mono text-foreground">{s.value}</p>
          </div>
        ))}
      </div>

      {/* Execution timeline */}
      <div className="glass-card p-5">
        <h3 className="text-sm font-medium text-foreground mb-4 flex items-center gap-2">
          <TrendingUp className="h-4 w-4 text-primary" />
          Execution Timeline
        </h3>
        <div className="flex gap-1 items-stretch">
          {layers.map((layer, i) => {
            const layerRuns = pipelineRuns.filter((r) => r.layer === layer);
            const hasFailure = layerRuns.some((r) => r.status === "failed");
            const hasRunning = layerRuns.some((r) => r.status === "running");
            return (
              <div key={layer} className="flex-1 flex flex-col items-center">
                <div className={`w-full rounded-md border p-3 text-center transition-all ${
                  hasFailure ? "border-critical/40 bg-critical/5 glow-critical" :
                  hasRunning ? "border-info/40 bg-info/5" :
                  "border-success/30 bg-success/5 glow-success"
                }`}>
                  <p className="text-xs font-medium text-foreground">{layer}</p>
                  <p className="text-[10px] text-muted-foreground mt-1">{layerRuns.length} runs</p>
                  <div className="flex justify-center gap-1 mt-2">
                    {layerRuns.map((r) => (
                      <div key={r.id} className={`h-2 w-2 rounded-full ${
                        r.status === "success" ? "bg-success" :
                        r.status === "failed" ? "bg-critical" :
                        r.status === "running" ? "bg-info animate-pulse" :
                        "bg-cancelled"
                      }`} />
                    ))}
                  </div>
                </div>
                {i < layers.length - 1 && (
                  <div className="my-auto text-muted-foreground text-xs px-1">→</div>
                )}
              </div>
            );
          })}
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4">
        {/* Duration trend chart */}
        <div className="glass-card p-5">
          <h3 className="text-sm font-medium text-foreground mb-4">Duration & Success Rate</h3>
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={durationTrend}>
              <defs>
                <linearGradient id="durationGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="hsl(174,72%,46%)" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="hsl(174,72%,46%)" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(220,14%,20%)" />
              <XAxis dataKey="time" tick={{ fontSize: 10, fill: "hsl(215,15%,55%)" }} axisLine={false} />
              <YAxis tick={{ fontSize: 10, fill: "hsl(215,15%,55%)" }} axisLine={false} />
              <Tooltip
                contentStyle={{
                  background: "hsl(220,18%,13%)",
                  border: "1px solid hsl(220,14%,20%)",
                  borderRadius: "8px",
                  fontSize: "12px",
                  color: "hsl(210,20%,92%)",
                }}
              />
              <Area type="monotone" dataKey="duration" stroke="hsl(174,72%,46%)" fill="url(#durationGrad)" strokeWidth={2} name="Duration (min)" />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        {/* Run history */}
        <div className="glass-card p-5">
          <h3 className="text-sm font-medium text-foreground mb-4">Recent Runs</h3>
          <div className="space-y-2 max-h-[200px] overflow-auto">
            {pipelineRuns.map((run) => (
              <div key={run.id} className="flex items-center justify-between rounded-md bg-muted/30 px-3 py-2">
                <div className="flex-1 min-w-0">
                  <p className="text-xs font-medium text-foreground truncate">{run.name}</p>
                  <p className="text-[10px] text-muted-foreground">{run.source} · {run.layer}</p>
                </div>
                <div className="flex items-center gap-3">
                  <span className="text-[10px] text-muted-foreground font-mono">{run.duration}</span>
                  <PipelineStatusBadge status={run.status} />
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
