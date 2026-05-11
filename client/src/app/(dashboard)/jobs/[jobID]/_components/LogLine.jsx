const LEVEL_COLOR = {
  ERROR: "text-status-failed-fg",
  WARN: "text-status-running-fg",
  INFO: ""
};

// Go's zero-value time. Real Sepex emits this for log entries without a
// timestamp; formatting it as a clock value (e.g. "8:00:00 PM") is misleading.
const GO_ZERO_TIME = "0001-01-01T00:00:00Z";

export default function LogLine({ entry }) {
  const level = (entry?.level || "INFO").toUpperCase();
  const color = LEVEL_COLOR[level] ?? "";
  let timeLabel = "";
  if (entry?.time && entry.time !== GO_ZERO_TIME) {
    try {
      timeLabel = new Date(entry.time).toLocaleTimeString();
    } catch {
      timeLabel = entry.time;
    }
  }
  return (
    <div className="flex gap-2 px-2 py-0.5 text-muted-foreground hover:bg-background/60">
      <span className={`shrink-0 ${color}`.trim()}>[{level}]</span>
      <span className="shrink-0">{timeLabel}</span>
      <span className="flex-1 break-all">{entry?.msg || ""}</span>
    </div>
  );
}
