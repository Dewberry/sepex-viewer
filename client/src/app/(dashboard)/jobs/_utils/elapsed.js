// Format a duration in seconds as a compact "Xh Ym Zs" / "Xm Ys" / "Xs" string.
export function formatDuration(totalSeconds) {
  if (!Number.isFinite(totalSeconds) || totalSeconds < 0) return "—";
  const sec = Math.floor(totalSeconds);
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

const ACTIVE = new Set(["accepted", "running"]);

// Compute elapsed duration for a job:
//   • terminal jobs → updated - created
//   • active jobs   → now - created (use the `now` arg so the caller can keep
//                     a stable reference for tick re-renders)
// Returns a formatted string or "—" when inputs are missing.
export function getElapsed(job, now = Date.now()) {
  if (!job?.created) return "—";
  const start = new Date(job.created).getTime();
  if (Number.isNaN(start)) return "—";
  const end = ACTIVE.has(job.status)
    ? now
    : new Date(job.updated || job.created).getTime();
  if (Number.isNaN(end)) return "—";
  return formatDuration(Math.max(0, (end - start) / 1000));
}
