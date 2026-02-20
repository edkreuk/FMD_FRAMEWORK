import { errorItems } from "@/data/mockData";
import { SeverityBadge } from "@/components/StatusBadge";
import { Brain, AlertTriangle, ChevronDown, Repeat, Lightbulb, Search } from "lucide-react";
import { useState } from "react";

export default function ErrorIntelligence() {
  const [expanded, setExpanded] = useState<string | null>(null);
  const [filter, setFilter] = useState<"all" | "critical" | "warning" | "info">("all");

  const filtered = filter === "all" ? errorItems : errorItems.filter((e) => e.severity === filter);
  const critCount = errorItems.filter((e) => e.severity === "critical").length;
  const warnCount = errorItems.filter((e) => e.severity === "warning").length;
  const infoCount = errorItems.filter((e) => e.severity === "info").length;

  return (
    <div className="space-y-6 animate-fade-in">
      <div>
        <h2 className="text-xl font-semibold text-foreground flex items-center gap-2">
          <Brain className="h-5 w-5 text-primary" />
          Error Intelligence
        </h2>
        <p className="text-sm text-muted-foreground mt-1">AI-generated summaries of pipeline issues — no raw logs</p>
      </div>

      {/* Summary cards */}
      <div className="flex gap-3">
        {[
          { label: "Critical", count: critCount, color: "border-critical/40 bg-critical/5 text-critical" },
          { label: "Warning", count: warnCount, color: "border-warning/40 bg-warning/5 text-warning" },
          { label: "Info", count: infoCount, color: "border-info/40 bg-info/5 text-info" },
        ].map((s) => (
          <button
            key={s.label}
            onClick={() => setFilter(filter === s.label.toLowerCase() as any ? "all" : s.label.toLowerCase() as any)}
            className={`glass-card flex items-center gap-2 rounded-lg border px-4 py-3 transition-all hover:scale-[1.02] ${
              filter === s.label.toLowerCase() ? s.color : ""
            }`}
          >
            <span className="text-lg font-semibold font-mono">{s.count}</span>
            <span className="text-xs text-muted-foreground">{s.label}</span>
          </button>
        ))}
      </div>

      {/* Error list */}
      <div className="space-y-3">
        {filtered.map((err) => (
          <div
            key={err.id}
            className={`glass-card overflow-hidden transition-all ${
              err.severity === "critical" ? "glow-critical border-critical/20" :
              err.severity === "warning" ? "glow-warning border-warning/20" : ""
            }`}
          >
            <button
              onClick={() => setExpanded(expanded === err.id ? null : err.id)}
              className="flex w-full items-start gap-3 p-4 text-left"
            >
              <AlertTriangle className={`mt-0.5 h-4 w-4 flex-shrink-0 ${
                err.severity === "critical" ? "text-critical" :
                err.severity === "warning" ? "text-warning" : "text-info"
              }`} />
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium text-foreground">{err.summary}</p>
                <div className="mt-1.5 flex items-center gap-3">
                  <SeverityBadge severity={err.severity} />
                  <span className="text-[10px] text-muted-foreground">{err.sourceSystem}</span>
                  <span className="text-[10px] text-muted-foreground">·</span>
                  <span className="text-[10px] text-muted-foreground">{err.layer}</span>
                  {err.frequency > 1 && (
                    <span className="flex items-center gap-1 text-[10px] text-warning">
                      <Repeat className="h-3 w-3" /> {err.frequency}x this week
                    </span>
                  )}
                </div>
              </div>
              <ChevronDown className={`h-4 w-4 text-muted-foreground transition-transform ${expanded === err.id ? "rotate-180" : ""}`} />
            </button>

            {expanded === err.id && (
              <div className="animate-fade-in border-t border-border px-4 pb-4 pt-3 space-y-3">
                <div>
                  <p className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-1">What happened</p>
                  <p className="text-xs text-foreground/80">{err.whatHappened}</p>
                </div>
                <div>
                  <p className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-1">Why</p>
                  <p className="text-xs text-foreground/80">{err.why}</p>
                </div>
                <div>
                  <p className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-1 flex items-center gap-1">
                    <Search className="h-3 w-3" /> Check this source
                  </p>
                  <p className="text-xs text-primary font-medium">{err.sourceSystem}</p>
                </div>
                <div className="rounded-md bg-success/5 border border-success/20 p-3">
                  <p className="text-[10px] font-medium text-success uppercase tracking-wider mb-1 flex items-center gap-1">
                    <Lightbulb className="h-3 w-3" /> Suggested fix
                  </p>
                  <p className="text-xs text-foreground/80">{err.suggestedFix}</p>
                </div>
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
