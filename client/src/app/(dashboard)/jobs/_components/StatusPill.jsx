// Bright status colors get black text for WCAG AA (white only hits 1.9–3.8:1
// on these greens / yellows / reds / blues). Outlined pills use a darker
// shade for text since the pure status color fails AA on light surfaces.
const STATUS_TONE = {
  successful: "bg-status-successful text-black",
  failed: "bg-status-failed text-black",
  running: "bg-status-running text-black",
  accepted: "bg-status-accepted text-black",
  dismissed:
    "border border-status-dismissed text-slate-700 dark:text-slate-300",
  lost: "border border-status-lost text-purple-700 dark:text-purple-300"
};

export default function StatusPill({ status }) {
  return (
    <span
      className={`rounded-full px-2 py-0.5 text-[10px] font-medium ${
        STATUS_TONE[status] || "bg-muted text-muted-foreground"
      }`}
    >
      {status}
    </span>
  );
}
