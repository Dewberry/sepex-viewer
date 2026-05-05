import {
  AlertCircle,
  CheckCircle2,
  Clock,
  Loader2,
  XCircle
} from "lucide-react";

const ICON_MAP = {
  successful: CheckCircle2,
  failed: XCircle,
  running: Loader2,
  accepted: Clock,
  dismissed: AlertCircle,
  lost: AlertCircle
};

const COLOR_MAP = {
  successful: "text-status-successful",
  failed: "text-status-failed",
  running: "text-status-running",
  accepted: "text-status-accepted",
  dismissed: "text-status-dismissed",
  lost: "text-status-lost"
};

export default function StatusIcon({ status, className = "h-5 w-5" }) {
  const Icon = ICON_MAP[status] || AlertCircle;
  const color = COLOR_MAP[status] || "text-muted-foreground";
  const spin = status === "running" ? "animate-spin" : "";
  return <Icon className={`${className} ${color} ${spin}`.trim()} />;
}
