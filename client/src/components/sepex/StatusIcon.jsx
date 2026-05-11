import { getStatusMeta } from "@/lib/jobStatus";
import { cn } from "@/lib/utils";

export default function StatusIcon({ status, className = "h-4 w-4", label }) {
  const { Icon, iconClass, spin } = getStatusMeta(status);
  const a11yProps = label
    ? { role: "img", "aria-label": label }
    : { "aria-hidden": "true" };
  return (
    <Icon
      className={cn(className, iconClass, spin && "animate-spin")}
      {...a11yProps}
    />
  );
}
