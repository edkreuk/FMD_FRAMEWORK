import { healthMetrics } from "@/data/mockData";
import { Activity, CheckCircle, AlertTriangle, Clock, Sun, Moon } from "lucide-react";
import { useTheme } from "@/hooks/use-theme";

export function HealthBar() {
  const m = healthMetrics;
  const overallHealth = m.pipelinesFailed === 0 ? "healthy" : m.pipelinesFailed <= 3 ? "degraded" : "critical";
  const { theme, toggleTheme } = useTheme();

  return (
    <div className="flex items-center justify-between border-b border-border bg-health-bar px-6 py-2.5">
      <div className="flex items-center gap-2">
        <div className={`h-2.5 w-2.5 rounded-full ${
          overallHealth === "healthy" ? "bg-success" : overallHealth === "degraded" ? "bg-warning" : "bg-critical"
        }`} />
        <span className="text-sm font-medium text-foreground">
          System {overallHealth === "healthy" ? "Healthy" : overallHealth === "degraded" ? "Attention Needed" : "Critical Issues"}
        </span>
      </div>

      <div className="flex items-center gap-6 text-xs text-muted-foreground">
        <div className="flex items-center gap-1.5">
          <Activity className="h-3.5 w-3.5 text-primary" />
          <span className="font-mono">{m.activePipelines}</span>
          <span>running</span>
        </div>
        <div className="flex items-center gap-1.5">
          <CheckCircle className="h-3.5 w-3.5 text-success" />
          <span className="font-mono">{m.overallSuccessRate}%</span>
          <span>success rate</span>
        </div>
        <div className="flex items-center gap-1.5">
          <AlertTriangle className="h-3.5 w-3.5 text-warning" />
          <span className="font-mono">{m.pipelinesFailed}</span>
          <span>failed today</span>
        </div>
        <div className="flex items-center gap-1.5">
          <Clock className="h-3.5 w-3.5 text-muted-foreground" />
          <span>Last run: <span className="font-mono">{m.lastFullRun}</span></span>
        </div>
        <button
          onClick={toggleTheme}
          className="ml-2 rounded-md p-1.5 text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
          aria-label="Toggle theme"
        >
          {theme === "dark" ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
        </button>
      </div>
    </div>
  );
}
