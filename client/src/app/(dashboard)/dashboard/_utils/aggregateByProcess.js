// Group jobs by processID, sort descending by count, and roll the long tail
// into an "Other" bucket so the donut chart stays readable.
export default function aggregateByProcess(jobs, topN = 6) {
  const counts = new Map();
  for (const job of jobs) {
    const key = job.processID || "(unknown)";
    counts.set(key, (counts.get(key) || 0) + 1);
  }

  const entries = [...counts.entries()]
    .map(([processID, count]) => ({ processID, count }))
    .sort((a, b) => b.count - a.count);

  if (entries.length <= topN) return entries;

  const head = entries.slice(0, topN);
  const tail = entries.slice(topN);
  const other = tail.reduce((sum, e) => sum + e.count, 0);
  return [...head, { processID: "Other", count: other }];
}
