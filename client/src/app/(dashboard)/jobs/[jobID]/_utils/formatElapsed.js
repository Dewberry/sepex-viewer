// Render a duration as "Xs", "Xm Ys", or "Xh Ym".
// Used for the ELAPSED stat on the Run Summary card. Pass `created` and
// either `updated` (for terminal jobs) or `now` (for active jobs) — the
// caller picks which.
export function formatElapsed(start, end) {
  if (!start || !end) return "—";
  const startMs = new Date(start).getTime();
  const endMs = new Date(end).getTime();
  if (Number.isNaN(startMs) || Number.isNaN(endMs)) return "—";
  const diff = Math.max(0, Math.floor((endMs - startMs) / 1000));
  if (diff < 60) return `${diff}s`;
  const mins = Math.floor(diff / 60);
  const secs = diff % 60;
  if (mins < 60) return `${mins}m ${secs}s`;
  const hrs = Math.floor(mins / 60);
  const remMins = mins % 60;
  return `${hrs}h ${remMins}m`;
}
