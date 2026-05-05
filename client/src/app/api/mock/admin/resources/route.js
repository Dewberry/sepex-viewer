import {
  jsonResponse,
  maybeDelay,
  maybeFailRandomly
} from "@/app/api/mock/_lib/helpers";
import { projectJobs, store } from "@/app/api/mock/_lib/store";

const MAX_CPUS = 16;
const MAX_MEMORY_MB = 32_768;

export async function GET(request) {
  await maybeDelay(request);
  const fail = maybeFailRandomly("GET /admin/resources");
  if (fail) return fail;

  const projected = projectJobs(store.jobs);
  const running = projected.filter((j) => j.status === "running").length;
  const accepted = projected.filter((j) => j.status === "accepted").length;

  // 0.5 CPU + 1024 MB per running, similar weight queued for accepted.
  const usedCPUs = Math.min(running * 0.5 + 0.25, MAX_CPUS);
  const queuedCPUs = Math.min(accepted * 0.5, MAX_CPUS);
  const usedMemory = Math.min(running * 1024 + 256, MAX_MEMORY_MB);
  const queuedMemory = Math.min(accepted * 1024, MAX_MEMORY_MB);

  const round = (n) => Math.round(n * 10) / 10;

  return jsonResponse({
    resources: {
      usedCPUs: round(usedCPUs),
      usedMemory: Math.round(usedMemory),
      queuedCPUs: round(queuedCPUs),
      queuedMemory: Math.round(queuedMemory),
      maxCPUs: MAX_CPUS,
      maxMemory: MAX_MEMORY_MB,
      usedCPUsPct: round((usedCPUs / MAX_CPUS) * 100),
      queuedCPUsPct: round((queuedCPUs / MAX_CPUS) * 100),
      usedMemPct: round((usedMemory / MAX_MEMORY_MB) * 100),
      queuedMemPct: round((queuedMemory / MAX_MEMORY_MB) * 100)
    },
    links: [{ href: "/admin/resources", rel: "self", title: "this document" }]
  });
}
