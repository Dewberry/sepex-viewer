// Counts jobs by status and computes Successful / (Successful + Failed) as a
// percentage. Returns null for successRate when there are no terminal jobs to
// avoid showing 0% on an empty window.
export default function computeKpis(jobs) {
  let successful = 0;
  let failed = 0;
  let running = 0;
  for (const job of jobs) {
    if (job.status === "successful") successful += 1;
    else if (job.status === "failed") failed += 1;
    else if (job.status === "running") running += 1;
  }

  const terminal = successful + failed;
  const successRate =
    terminal === 0 ? null : Math.round((successful / terminal) * 100);

  return {
    total: jobs.length,
    successful,
    failed,
    running,
    successRate
  };
}
