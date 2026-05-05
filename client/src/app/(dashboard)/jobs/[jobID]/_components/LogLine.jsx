const LEVEL_COLOR = {
  ERROR: "text-status-failed",
  WARN: "text-status-running",
  INFO: ""
};

export default function LogLine({ entry }) {
  const level = (entry?.level || "INFO").toUpperCase();
  const color = LEVEL_COLOR[level] ?? "";
  let timeLabel = "";
  if (entry?.time) {
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
