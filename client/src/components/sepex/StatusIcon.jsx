import { getStatusMeta } from "@/lib/jobStatus";
import { cn } from "@/lib/utils";

export default function StatusIcon({ status, className = "h-4 w-4" }) {
  const { Icon, iconClass, spin } = getStatusMeta(status);
  return <Icon className={cn(className, iconClass, spin && "animate-spin")} />;
}
