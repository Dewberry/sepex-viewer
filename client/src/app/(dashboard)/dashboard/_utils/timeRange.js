export const TIME_RANGES = ["24h", "7d", "30d"];

export const TIME_RANGE_LABELS = {
  "24h": "24h",
  "7d": "7d",
  "30d": "30d"
};

// Picks a `limit` for the single /jobs fetch that powers KPIs/charts/activity.
// Larger windows fetch more rows but the API has no `?updatedAfter` yet, so
// 7d/30d are still best-effort — see research/pm-meeting.md §A.
export function getJobsLimitForRange(range) {
  if (range === "30d") return 1000;
  if (range === "7d") return 500;
  return 200;
}

// Hours / days of lookback for client-side windowing.
export function getRangeStart(range, now = new Date()) {
  const start = new Date(now);
  if (range === "30d") {
    start.setDate(start.getDate() - 30);
  } else if (range === "7d") {
    start.setDate(start.getDate() - 7);
  } else {
    start.setHours(start.getHours() - 24);
  }
  return start;
}
