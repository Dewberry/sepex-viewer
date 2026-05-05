import {
  eachDayOfInterval,
  eachHourOfInterval,
  format,
  startOfDay,
  startOfHour
} from "date-fns";

// Build an array of evenly-spaced buckets between `start` and `now`, count
// jobs by status per bucket, and tag the last bucket as in-progress so the
// chart can render it at reduced opacity.
//
// Returns: [{ key, label, successful, failed, running, accepted, dismissed,
//             lost, total, inProgress }]
export default function bucketJobsOverTime(jobs, range, now = new Date()) {
  const useHourly = range === "24h";
  const start = new Date(now);
  if (useHourly) {
    start.setHours(start.getHours() - 23);
  } else if (range === "7d") {
    start.setDate(start.getDate() - 6);
  } else {
    start.setDate(start.getDate() - 29);
  }

  const startBucket = useHourly ? startOfHour(start) : startOfDay(start);
  const endBucket = useHourly ? startOfHour(now) : startOfDay(now);

  const slots = useHourly
    ? eachHourOfInterval({ start: startBucket, end: endBucket })
    : eachDayOfInterval({ start: startBucket, end: endBucket });

  const buckets = slots.map((slot, idx) => ({
    key: slot.toISOString(),
    label: useHourly
      ? format(slot, "ha").toLowerCase()
      : range === "7d"
        ? format(slot, "EEE")
        : format(slot, "M/dd"),
    successful: 0,
    failed: 0,
    running: 0,
    accepted: 0,
    dismissed: 0,
    lost: 0,
    total: 0,
    inProgress: idx === slots.length - 1
  }));

  if (buckets.length === 0) return buckets;

  for (const job of jobs) {
    if (!job.updated) continue;
    const t = new Date(job.updated).getTime();
    if (Number.isNaN(t)) continue;
    if (t < startBucket.getTime()) continue;

    const bucketStart = useHourly ? startOfHour(t) : startOfDay(t);
    const slot = buckets.find((b) => b.key === bucketStart.toISOString());
    if (!slot) continue;

    if (slot[job.status] !== undefined) {
      slot[job.status] += 1;
    }
    slot.total += 1;
  }

  return buckets;
}
