import {
  AlertCircle,
  Ban,
  CheckCircle2,
  CircleHelp,
  Clock,
  Loader2,
  XCircle
} from "lucide-react";

// Single source of truth for how a job status renders. Icon + foreground
// color (used by StatusIcon) and pill background tone (used by StatusPill)
// are kept in lockstep so any new status added downstream only needs to
// land in this file.
export const STATUS_META = {
  successful: {
    Icon: CheckCircle2,
    iconClass: "text-status-successful",
    pillClass: "bg-status-successful text-white"
  },
  failed: {
    Icon: XCircle,
    iconClass: "text-status-failed",
    pillClass: "bg-status-failed text-white"
  },
  running: {
    Icon: Loader2,
    iconClass: "text-status-running",
    pillClass: "bg-status-running text-white",
    spin: true
  },
  accepted: {
    Icon: Clock,
    iconClass: "text-status-accepted",
    pillClass: "bg-status-accepted text-white"
  },
  dismissed: {
    Icon: Ban,
    iconClass: "text-status-dismissed",
    pillClass:
      "border border-status-dismissed text-slate-700 dark:text-slate-300"
  },
  lost: {
    Icon: CircleHelp,
    iconClass: "text-status-lost",
    pillClass: "border border-status-lost text-purple-700 dark:text-purple-300"
  }
};

export const FALLBACK_STATUS_META = {
  Icon: AlertCircle,
  iconClass: "text-muted-foreground",
  pillClass: "bg-muted text-muted-foreground"
};

export function getStatusMeta(status) {
  return STATUS_META[status] || FALLBACK_STATUS_META;
}
