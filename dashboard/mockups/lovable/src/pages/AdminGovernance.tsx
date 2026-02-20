import { healthMetrics, sourceSystems, entityCounts, lineageFlows } from "@/data/mockData";
import { Shield, CheckCircle, Clock, Database, ArrowRight, Server, Layers } from "lucide-react";

function HealthScorecard() {
  const m = healthMetrics;
  const metrics = [
    { label: "Success Rate", value: `${m.overallSuccessRate}%`, good: m.overallSuccessRate > 90 },
    { label: "Data Freshness", value: m.dataFreshness, good: true },
    { label: "Last Full Run", value: m.lastFullRun, good: true },
    { label: "Failed Today", value: m.pipelinesFailed.toString(), good: m.pipelinesFailed < 5 },
    { label: "Total Sources", value: m.totalSources.toString(), good: true },
    { label: "Avg Duration", value: m.avgDuration, good: true },
  ];

  return (
    <div className="glass-card p-5">
      <h3 className="text-sm font-medium text-foreground mb-4 flex items-center gap-2">
        <Shield className="h-4 w-4 text-primary" />
        Health Scorecard
      </h3>
      <div className="grid grid-cols-3 gap-3">
        {metrics.map((m) => (
          <div key={m.label} className="rounded-md bg-muted/30 p-3">
            <p className="text-[10px] text-muted-foreground">{m.label}</p>
            <p className={`text-lg font-semibold font-mono mt-1 ${m.good ? "text-success" : "text-warning"}`}>{m.value}</p>
          </div>
        ))}
      </div>
    </div>
  );
}

function SourceRegistry() {
  return (
    <div className="glass-card p-5">
      <h3 className="text-sm font-medium text-foreground mb-4 flex items-center gap-2">
        <Server className="h-4 w-4 text-primary" />
        Source Systems
      </h3>
      <div className="space-y-2">
        {sourceSystems.map((s) => (
          <div key={s.id} className="flex items-center gap-3 rounded-md bg-muted/30 px-3 py-2.5">
            <div className={`h-2 w-2 rounded-full ${
              s.health === "healthy" ? "bg-success" : s.health === "degraded" ? "bg-warning" : "bg-critical"
            }`} />
            <div className="flex-1 min-w-0">
              <p className="text-xs font-medium text-foreground">{s.name}</p>
              <p className="text-[10px] text-muted-foreground">{s.type}</p>
            </div>
            <div className="text-right">
              <p className="text-[10px] text-muted-foreground flex items-center gap-1">
                <Clock className="h-3 w-3" /> {s.lastSync}
              </p>
              <p className="text-[10px] text-muted-foreground font-mono">{s.recordsProcessed.toLocaleString()} records</p>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function EntityInventory() {
  return (
    <div className="glass-card p-5">
      <h3 className="text-sm font-medium text-foreground mb-4 flex items-center gap-2">
        <Layers className="h-4 w-4 text-primary" />
        Entity Inventory
      </h3>
      <div className="flex gap-3">
        {entityCounts.map((e, i) => (
          <div key={e.layer} className="flex-1 flex flex-col items-center">
            <div className="w-full rounded-lg border border-border bg-muted/20 p-4 text-center">
              <p className="text-2xl font-semibold font-mono text-foreground">{e.count}</p>
              <p className="text-[10px] text-muted-foreground mt-1">{e.label}</p>
              <p className={`text-xs font-medium mt-1 text-${e.color}`}>{e.layer}</p>
            </div>
            {i < entityCounts.length - 1 && (
              <ArrowRight className="h-3.5 w-3.5 text-muted-foreground my-2 rotate-0" />
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

function DataLineage() {
  const allLayers = ["Landing", "Bronze", "Silver", "Gold"];

  return (
    <div className="glass-card p-5">
      <h3 className="text-sm font-medium text-foreground mb-4 flex items-center gap-2">
        <Database className="h-4 w-4 text-primary" />
        Data Lineage
      </h3>
      <div className="space-y-2">
        {lineageFlows.map((flow) => (
          <div key={flow.source} className="flex items-center gap-2 rounded-md bg-muted/20 px-3 py-2.5">
            <span className="text-xs font-medium text-foreground w-28 flex-shrink-0 truncate">{flow.source}</span>
            <div className="flex items-center gap-1 flex-1">
              {allLayers.map((layer) => {
                const active = flow.layers.includes(layer);
                return (
                  <div key={layer} className="flex items-center gap-1 flex-1">
                    <div className={`h-1.5 flex-1 rounded-full ${active ? "bg-primary/60" : "bg-border/40"}`} />
                  </div>
                );
              })}
            </div>
            <ArrowRight className="h-3 w-3 text-muted-foreground flex-shrink-0" />
            <span className="text-xs text-primary font-medium w-32 flex-shrink-0 truncate text-right">{flow.destination}</span>
          </div>
        ))}
        <div className="flex items-center gap-1 px-3 mt-1">
          <span className="w-28 flex-shrink-0" />
          {allLayers.map((l) => (
            <span key={l} className="flex-1 text-center text-[9px] text-muted-foreground">{l}</span>
          ))}
          <span className="w-3 flex-shrink-0" />
          <span className="w-32 flex-shrink-0" />
        </div>
      </div>
    </div>
  );
}

export default function AdminGovernance() {
  return (
    <div className="space-y-6 animate-fade-in">
      <div>
        <h2 className="text-xl font-semibold text-foreground">Admin & Governance</h2>
        <p className="text-sm text-muted-foreground mt-1">System health, source registry, and data lineage overview</p>
      </div>
      <HealthScorecard />
      <div className="grid grid-cols-2 gap-4">
        <SourceRegistry />
        <EntityInventory />
      </div>
      <DataLineage />
    </div>
  );
}
