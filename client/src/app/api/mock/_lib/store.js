import { buildSeed } from "./fixtures";

// globalThis survives Turbopack HMR reloads of route modules within a single
// dev process. Resets on `next dev` restart, which is fine for a screenshot
// mock — never use this in production.
function init() {
  const seed = buildSeed(Date.now());
  return {
    jobs: seed.jobs,
    logs: seed.logs,
    results: seed.results,
    metadata: seed.metadata,
    processes: seed.processes
  };
}

if (!globalThis.__sepexMockStore) {
  globalThis.__sepexMockStore = init();
}

export const store = globalThis.__sepexMockStore;

const ACTIVE_RUN_MS = 5_000; // accepted -> running
const TERMINAL_MS = 30_000; // running -> successful (for newly POST'd jobs)

// Jobs created by POST /processes/{id}/execution carry _submittedAt so we can
// progress their status over time. Seed jobs ignore this (already terminal or
// pinned to a specific status for screenshots).
export function liveStatus(job) {
  if (!job._isMockSubmission) return job.status;
  const elapsed = Date.now() - job._submittedAt;
  if (elapsed < ACTIVE_RUN_MS) return "accepted";
  if (elapsed < TERMINAL_MS) return "running";
  return job._terminalStatus || "successful";
}

// Returns a job projection suitable for response — replaces stored status with
// the live, time-derived status for newly-submitted jobs.
export function projectJob(job) {
  if (!job) return job;
  return { ...job, status: liveStatus(job) };
}

export function projectJobs(jobs) {
  return jobs.map(projectJob);
}

export function findJob(jobID) {
  return store.jobs.find((j) => j.jobID === jobID);
}

export function appendJob(job) {
  store.jobs.unshift(job);
  return job;
}

export function setJobStatus(jobID, status) {
  const job = findJob(jobID);
  if (!job) return null;
  job.status = status;
  job._isMockSubmission = false; // freeze status, no more progression
  job.updated = new Date().toISOString();
  return job;
}

export function appendLog(jobID, entry, stream = "process_logs") {
  if (!store.logs[jobID])
    store.logs[jobID] = { process_logs: [], server_logs: [] };
  store.logs[jobID][stream].push(entry);
}

// Seed dynamic logs for newly-submitted jobs based on elapsed time, so the
// Logs tab has tail content during the accepted -> running -> successful arc.
export function ensureProgressLogs(job) {
  if (!job._isMockSubmission) return;
  const elapsed = Date.now() - job._submittedAt;
  if (!store.logs[job.jobID])
    store.logs[job.jobID] = { process_logs: [], server_logs: [] };
  const pl = store.logs[job.jobID].process_logs;
  const sl = store.logs[job.jobID].server_logs;
  const at = (offset) => new Date(job._submittedAt + offset).toISOString();

  // Server logs are stable per-state; only push if not already there.
  const ensureServer = (msg, time) => {
    if (!sl.find((e) => e.msg === msg)) sl.push({ level: "INFO", msg, time });
  };
  ensureServer(`Job ${job.jobID} accepted by scheduler`, at(0));
  if (elapsed >= 1_000)
    ensureServer("Allocating 2 CPU, 4096MB on host docker-1", at(1_000));
  if (elapsed >= ACTIVE_RUN_MS)
    ensureServer("Container started", at(ACTIVE_RUN_MS));
  if (elapsed >= TERMINAL_MS)
    ensureServer("Container exited 0", at(TERMINAL_MS));

  // Process logs accumulate as time advances.
  const ensureProc = (level, msg, time) => {
    if (!pl.find((e) => e.msg === msg)) pl.push({ level, msg, time });
  };
  if (elapsed >= ACTIVE_RUN_MS) {
    ensureProc("INFO", `Initializing ${job.processID}`, at(ACTIVE_RUN_MS));
    ensureProc("INFO", "Loading configuration", at(ACTIVE_RUN_MS + 500));
  }
  if (elapsed >= 10_000)
    ensureProc("INFO", "Setup complete, preparing inputs", at(10_000));
  if (elapsed >= 15_000)
    ensureProc("INFO", "Simulation step 1 / 24", at(15_000));
  if (elapsed >= 22_000)
    ensureProc(
      "INFO",
      "Simulation step 12 / 24 — iteration converged",
      at(22_000)
    );
  if (elapsed >= 26_000)
    ensureProc("INFO", "Simulation step 24 / 24", at(26_000));
  if (elapsed >= TERMINAL_MS)
    ensureProc("INFO", "Writing outputs", at(TERMINAL_MS));
  if (elapsed >= TERMINAL_MS + 1000)
    ensureProc("INFO", "Job complete — exit 0", at(TERMINAL_MS + 1000));
}
