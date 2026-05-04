import { CheckCircle2, Clock, Loader2, XCircle } from "lucide-react";

export default function StatusIcon({ status, className = "h-4 w-4" }) {
  if (status === "successful")
    return <CheckCircle2 className={`${className} text-status-successful`} />;
  if (status === "running")
    return (
      <Loader2 className={`${className} animate-spin text-status-running`} />
    );
  if (status === "failed")
    return <XCircle className={`${className} text-status-failed`} />;
  return <Clock className={`${className} text-status-accepted`} />;
}
