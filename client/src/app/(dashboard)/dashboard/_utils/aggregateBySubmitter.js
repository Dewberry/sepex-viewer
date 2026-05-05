export default function aggregateBySubmitter(jobs, topN = 8) {
  const counts = new Map();
  for (const job of jobs) {
    const key = job.submitter || "(unknown)";
    counts.set(key, (counts.get(key) || 0) + 1);
  }

  return [...counts.entries()]
    .map(([submitter, count]) => ({ submitter, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, topN);
}
