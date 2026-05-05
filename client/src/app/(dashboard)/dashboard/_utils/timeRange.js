import { startOfDay, startOfHour } from "date-fns";

export const TIME_RANGES = ["24h", "7d", "30d"];

// Picks a `limit` for the single /jobs fetch that powers KPIs/charts/activity.
// Larger windows fetch more rows but the API has no `?updatedAfter` yet, so
// 7d/30d are still best-effort — see research/pm-meeting.md §A.
export function getJobsLimitForRange(range) {
  if (range === "30d") return 1000;
  if (range === "7d") return 500;
  return 200;
}

// Start of the windowed range, snapped to the first bucket boundary the chart
// uses (24h → 24 hourly buckets ending now; 7d/30d → daily buckets ending
// today). Keeping KPIs and the chart on the same window means a job either
// shows up in both or neither — no silent dropping at the edge.
export function getRangeStart(range, now = new Date()) {
  if (range === "30d") {
    const d = new Date(now);
    d.setDate(d.getDate() - 29);
    return startOfDay(d);
  }
  if (range === "7d") {
    const d = new Date(now);
    d.setDate(d.getDate() - 6);
    return startOfDay(d);
  }
  const d = new Date(now);
  d.setHours(d.getHours() - 23);
  return startOfHour(d);
}
