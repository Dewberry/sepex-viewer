import { getStatusMeta } from "@/lib/jobStatus";
import { cn } from "@/lib/utils";

// Bright status colors get black text for WCAG AA (white only hits 1.9–3.8:1
// on these greens / yellows / reds / blues). Outlined pills use a darker
// shade for text since the pure status color fails AA on light surfaces.
export default function StatusPill({ status }) {
  const { pillClass } = getStatusMeta(status);
  return (
    <span
      className={cn(
        "rounded-full px-2 py-0.5 text-[10px] font-medium",
        pillClass
      )}
    >
      {status}
    </span>
  );
}
