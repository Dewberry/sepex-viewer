import { startOfDay, startOfHour } from "date-fns";
import { PROPOSED_API_ENABLED } from "@/lib/featureFlags";

export const TIME_RANGES = ["24h", "7d", "30d"];

// Picks a `limit` for the single /jobs fetch that powers KPIs/charts/activity.
// Real Sepex `/jobs` silently clamps `limit` to 100 today, so with the flag
// off we ask for exactly that. With the flag on we ask for what the proposed
// stats endpoint would aggregate over — see research/proposed-next-steps.md A/C.
export function getJobsLimitForRange(range) {
  if (!PROPOSED_API_ENABLED) return 100;
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
