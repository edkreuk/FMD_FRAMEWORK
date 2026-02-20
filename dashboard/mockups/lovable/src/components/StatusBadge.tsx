import { cn } from "@/lib/utils";
import type { PipelineStatus, Severity } from "@/data/mockData";

const statusConfig: Record<PipelineStatus, { label: string; className: string }> = {
  success: { label: "Success", className: "bg-success/15 text-success border-success/30" },
  failed: { label: "Failed", className: "bg-critical/15 text-critical border-critical/30" },
  running: { label: "Running", className: "bg-info/15 text-info border-info/30" },
  cancelled: { label: "Cancelled", className: "bg-cancelled/15 text-cancelled border-cancelled/30" },
};

const severityConfig: Record<Severity, { label: string; className: string; dot: string }> = {
  critical: { label: "Critical", className: "bg-critical/15 text-critical border-critical/30", dot: "bg-critical" },
  warning: { label: "Warning", className: "bg-warning/15 text-warning border-warning/30", dot: "bg-warning" },
  info: { label: "Info", className: "bg-info/15 text-info border-info/30", dot: "bg-info" },
};

export function PipelineStatusBadge({ status }: { status: PipelineStatus }) {
  const config = statusConfig[status];
  return (
    <span className={cn("inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium", config.className)}>
      {status === "running" && (
        <span className="relative flex h-2 w-2">
          <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-info opacity-40" />
          <span className="relative inline-flex h-2 w-2 rounded-full bg-info" />
        </span>
      )}
      {status !== "running" && <span className={cn("h-1.5 w-1.5 rounded-full", 
        status === "success" ? "bg-success" : status === "failed" ? "bg-critical" : "bg-cancelled"
      )} />}
      {config.label}
    </span>
  );
}

export function SeverityBadge({ severity }: { severity: Severity }) {
  const config = severityConfig[severity];
  return (
    <span className={cn("inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium", config.className)}>
      <span className={cn("h-1.5 w-1.5 rounded-full", config.dot)} />
      {config.label}
    </span>
  );
}
