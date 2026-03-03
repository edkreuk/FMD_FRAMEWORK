import { cn } from "@/lib/utils";
import { CheckCircle2, AlertCircle, XCircle, Clock, PauseCircle } from "lucide-react";
import { Status } from "@/lib/mockData";

interface StatusBadgeProps {
  status: Status;
  className?: string;
  showIcon?: boolean;
}

const statusConfig = {
  success: {
    label: "Success",
    icon: CheckCircle2,
    color: "bg-emerald-50 text-emerald-700 border-emerald-200",
  },
  failed: {
    label: "Failed",
    icon: XCircle,
    color: "bg-rose-50 text-rose-700 border-rose-200",
  },
  running: {
    label: "Running",
    icon: Clock,
    color: "bg-blue-50 text-blue-700 border-blue-200 animate-pulse",
  },
  cancelled: {
    label: "Cancelled",
    icon: PauseCircle,
    color: "bg-slate-50 text-slate-700 border-slate-200",
  },
  warning: {
    label: "Warning",
    icon: AlertCircle,
    color: "bg-amber-50 text-amber-700 border-amber-200",
  },
};

export function StatusBadge({ status, className, showIcon = true }: StatusBadgeProps) {
  const config = statusConfig[status];
  const Icon = config.icon;

  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium border shadow-sm transition-colors",
        config.color,
        className
      )}
    >
      {showIcon && <Icon className="w-3.5 h-3.5" />}
      {config.label}
    </span>
  );
}
