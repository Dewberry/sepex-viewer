const STATUS_TONE = {
  successful: "bg-status-successful text-white",
  failed: "bg-status-failed text-white",
  running: "bg-status-running text-white",
  accepted: "bg-status-accepted text-white",
  dismissed: "border border-status-dismissed text-status-dismissed",
  lost: "border border-status-lost text-status-lost"
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
