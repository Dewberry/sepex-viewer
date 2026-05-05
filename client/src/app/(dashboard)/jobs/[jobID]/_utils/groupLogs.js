// Heuristic phase grouping for process_logs[]. The Sepex API returns a flat
// list — these regexes are best-effort buckets that match the patterns
// emitted by typical compute plugins (HEC-RAS, ML training, ffmpeg). Lines
// that don't match any bucket fall into "Other" so nothing is hidden.
const PHASES = [
  {
    title: "Initialization",
    test: (msg) => /init|loading|configur|prepar|setup/i.test(msg)
  },
  {
    title: "Simulation",
    test: (msg) =>
      /timestep|simul|iteration|step\s*\d|epoch|train|process(?:ing)?/i.test(
        msg
      )
  },
  {
    title: "Output",
    test: (msg) => /writing|output|complete|finish|done|exit/i.test(msg)
  }
];

export function groupProcessLogs(logs) {
  if (!Array.isArray(logs) || logs.length === 0) return [];
  const buckets = PHASES.map((p) => ({ title: p.title, logs: [] }));
  const other = { title: "Other", logs: [] };
  for (const log of logs) {
    const msg = log?.msg || "";
    const idx = PHASES.findIndex((p) => p.test(msg));
    if (idx === -1) other.logs.push(log);
    else buckets[idx].logs.push(log);
  }
  const result = buckets.filter((b) => b.logs.length > 0);
  if (other.logs.length > 0) result.push(other);
  return result;
}
