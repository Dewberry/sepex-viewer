import { SCENARIO } from "./helpers";

const HOUR = 60 * 60 * 1000;
const DAY = 24 * HOUR;

export const PROCESSES = [
  {
    version: "1.4.2",
    id: "ras-2d-mesh",
    title: "RAS 2D Mesh Build",
    description:
      "Build a 2D computational mesh from terrain and geometry inputs.",
    jobControlOptions: ["sync-execute", "async-execute"],
    outputTransmission: ["reference", "value"],
    inputs: [
      {
        id: "store_name",
        title: "Storage Name",
        description: "S3-style storage prefix where outputs are written.",
        input: {
          literalDataDomain: {
            dataType: "string",
            valueDefinition: { anyValue: true }
          }
        },
        minOccurs: 1,
        maxOccurs: 1
      },
      {
        id: "watershed",
        title: "Watershed",
        description: "Watershed identifier for the run.",
        input: {
          literalDataDomain: {
            dataType: "string",
            valueDefinition: {
              possibleValues: ["trinity", "neches", "brazos", "colorado"]
            }
          }
        },
        minOccurs: 1,
        maxOccurs: 1
      },
      {
        id: "mesh_resolution",
        title: "Mesh resolution (m)",
        description: "Cell size in meters.",
        input: {
          literalDataDomain: {
            dataType: "integer",
            valueDefinition: { anyValue: true }
          }
        },
        minOccurs: 1,
        maxOccurs: 1
      },
      {
        id: "geometry",
        title: "Geometry overrides",
        description: "Optional JSON object with geometry edits.",
        input: {
          literalDataDomain: {
            dataType: "object",
            valueDefinition: { anyValue: true }
          }
        },
        minOccurs: 0,
        maxOccurs: 1
      }
    ]
  },
  {
    version: "0.9.1",
    id: "flood-sim",
    title: "Flood Simulation",
    description: "Run a 2D unsteady flood simulation against a prepared mesh.",
    jobControlOptions: ["async-execute"],
    outputTransmission: ["reference"],
    inputs: [
      {
        id: "mesh_path",
        title: "Mesh path",
        description: "S3 URI to mesh.",
        input: {
          literalDataDomain: {
            dataType: "string",
            valueDefinition: { anyValue: true }
          }
        },
        minOccurs: 1,
        maxOccurs: 1
      },
      {
        id: "duration_hours",
        title: "Duration (hours)",
        description: "Simulation duration in hours.",
        input: {
          literalDataDomain: {
            dataType: "integer",
            valueDefinition: { anyValue: true }
          }
        },
        minOccurs: 1,
        maxOccurs: 1
      },
      {
        id: "boundary_conditions",
        title: "Boundary conditions",
        description: "List of inflow points (JSON array).",
        input: {
          literalDataDomain: {
            dataType: "list",
            valueDefinition: { anyValue: true }
          }
        },
        minOccurs: 0,
        maxOccurs: 1
      }
    ]
  },
  {
    version: "2.0.0",
    id: "terrain-extract",
    title: "Terrain Extract",
    description: "Extract a clipped DEM from the regional terrain catalog.",
    jobControlOptions: ["sync-execute", "async-execute"],
    outputTransmission: ["reference"],
    inputs: [
      {
        id: "region",
        title: "Region",
        description: "Catalog region id.",
        input: {
          literalDataDomain: {
            dataType: "string",
            valueDefinition: {
              possibleValues: ["tx-east", "tx-central", "tx-west", "la-coastal"]
            }
          }
        },
        minOccurs: 1,
        maxOccurs: 1
      },
      {
        id: "bbox",
        title: "Bounding box",
        description: "[minX, minY, maxX, maxY] in WGS84.",
        input: {
          literalDataDomain: {
            dataType: "list",
            valueDefinition: { anyValue: true }
          }
        },
        minOccurs: 1,
        maxOccurs: 1
      }
    ]
  },
  {
    version: "1.0.0",
    id: "report-pdf",
    title: "Report PDF",
    description: "Render a final analysis PDF from job outputs.",
    jobControlOptions: ["async-execute"],
    outputTransmission: ["reference"],
    inputs: [
      {
        id: "title",
        title: "Report title",
        description: "Title that appears on the cover page.",
        input: {
          literalDataDomain: {
            dataType: "string",
            valueDefinition: { anyValue: true }
          }
        },
        minOccurs: 1,
        maxOccurs: 1
      },
      {
        id: "metadata",
        title: "Report metadata",
        description: "JSON object of cover-page metadata.",
        input: {
          literalDataDomain: {
            dataType: "object",
            valueDefinition: { anyValue: true }
          }
        },
        minOccurs: 0,
        maxOccurs: 1
      }
    ]
  }
];

const SUBMITTERS = [
  "dev@dewberry.local",
  "ana.rivera@example.com",
  "lee.pham@example.com",
  "j.okafor@example.com",
  "m.santos@example.com",
  "k.nguyen@example.com",
  "r.patel@example.com",
  "s.chen@example.com",
  "t.williams@example.com",
  "h.alvarez@example.com",
  "d.brown@example.com",
  "p.gupta@example.com"
];

const PROCESS_IDS = PROCESSES.map((p) => p.id);
const WATERSHEDS = ["trinity", "neches", "brazos", "colorado"];

function pick(arr, i) {
  return arr[i % arr.length];
}

function makeTags(i) {
  const watershed = pick(WATERSHEDS, i);
  const run = `run-${(i % 9) + 1}`;
  return i % 3 === 0
    ? [`${watershed}_500yr_mesh`, run]
    : i % 3 === 1
      ? [`${watershed}_100yr_mesh`, run, "qa"]
      : [`${watershed}_baseline`, run];
}

function makeInputs(processID, i) {
  switch (processID) {
    case "ras-2d-mesh":
      return {
        store_name: `mesh-${i}`,
        watershed: pick(WATERSHEDS, i),
        mesh_resolution: 50 + (i % 5) * 50,
        ...(i % 4 === 0
          ? { geometry: { walls: [`w-${i}`], breaches: [] } }
          : null)
      };
    case "flood-sim":
      return {
        mesh_path: `s3://sepex/meshes/${pick(WATERSHEDS, i)}/${i}.h5`,
        duration_hours: 24 + (i % 4) * 12,
        ...(i % 3 === 0
          ? { boundary_conditions: [{ id: "inflow-1", q: 1200 }] }
          : null)
      };
    case "terrain-extract":
      return {
        region: pick(["tx-east", "tx-central", "tx-west", "la-coastal"], i),
        bbox: [-95.5 - i * 0.1, 29.5, -94.5, 30.5]
      };
    case "report-pdf":
      return {
        title: `Run ${i} Analysis`,
        ...(i % 2 === 0
          ? { metadata: { author: pick(SUBMITTERS, i), revision: i } }
          : null)
      };
    default:
      return {};
  }
}

const FAIL_REASONS = [
  "Could not connect to data store: i/o timeout after 30s",
  "Mesh validation failed: 4 disconnected components",
  "Out of memory: requested 8GB, host has 4GB",
  "Invalid bbox: minY (29.5) exceeds maxY (29.0)",
  "Boundary condition reference not found: inflow-9",
  "License check failed: no available HEC-RAS seats",
  "Plugin exited with code 137 (SIGKILL)"
];

function processLogsFor(status, processID, i, finishedAt) {
  const t0 = new Date(finishedAt - 90_000);
  const tn = (offset) => new Date(t0.getTime() + offset).toISOString();
  const base = [
    { level: "INFO", msg: `Initializing ${processID} v1.0`, time: tn(0) },
    {
      level: "INFO",
      msg: "Loading configuration from environment",
      time: tn(500)
    },
    { level: "INFO", msg: "Setup complete, preparing inputs", time: tn(1500) }
  ];
  if (status === "accepted") return [];
  if (status === "running") {
    return [
      ...base,
      { level: "INFO", msg: "Simulation step 1 / 24", time: tn(5_000) },
      {
        level: "INFO",
        msg: "Simulation step 2 / 24 (timestep 0:30)",
        time: tn(20_000)
      },
      {
        level: "INFO",
        msg: "Simulation step 3 / 24 (timestep 1:00)",
        time: tn(40_000)
      }
    ];
  }
  if (status === "successful") {
    return [
      ...base,
      { level: "INFO", msg: "Simulation step 1 / 24", time: tn(5_000) },
      {
        level: "INFO",
        msg: "Simulation step 12 / 24 — iteration converged",
        time: tn(40_000)
      },
      {
        level: "WARN",
        msg: "Timestep clipped at 30s (cell 4821)",
        time: tn(50_000)
      },
      { level: "INFO", msg: "Simulation step 24 / 24", time: tn(70_000) },
      {
        level: "INFO",
        msg: "Writing outputs to s3://sepex/results/" + i,
        time: tn(80_000)
      },
      { level: "INFO", msg: "Job complete — exit 0", time: tn(85_000) }
    ];
  }
  if (status === "failed") {
    const reason = FAIL_REASONS[i % FAIL_REASONS.length];
    return [
      ...base,
      { level: "INFO", msg: "Simulation step 1 / 24", time: tn(5_000) },
      { level: "WARN", msg: "Memory pressure detected", time: tn(20_000) },
      { level: "ERROR", msg: reason, time: tn(25_000) }
    ];
  }
  if (status === "dismissed") {
    return [
      ...base,
      { level: "INFO", msg: "Simulation step 1 / 24", time: tn(5_000) },
      {
        level: "WARN",
        msg: "Dismiss request received from submitter",
        time: tn(10_000)
      },
      { level: "INFO", msg: "Process exiting cleanly", time: tn(11_000) }
    ];
  }
  // lost
  return [
    ...base,
    { level: "INFO", msg: "Simulation step 1 / 24", time: tn(5_000) },
    { level: "WARN", msg: "Heartbeat missing for 60s", time: tn(60_000) },
    {
      level: "ERROR",
      msg: "Worker host vanished without sending exit signal",
      time: tn(75_000)
    }
  ];
}

function serverLogsFor(status, jobID, finishedAt) {
  const t0 = new Date(finishedAt - 95_000);
  const tn = (offset) => new Date(t0.getTime() + offset).toISOString();
  const out = [
    { level: "INFO", msg: `Job ${jobID} accepted by scheduler`, time: tn(0) },
    {
      level: "INFO",
      msg: "Allocating 2 CPU, 4096MB on host docker-1",
      time: tn(800)
    },
    { level: "INFO", msg: "Container started: sha256:abc1234", time: tn(2200) }
  ];
  if (status === "successful")
    out.push({ level: "INFO", msg: "Container exited 0", time: tn(85_000) });
  if (status === "failed")
    out.push({ level: "ERROR", msg: "Container exited 1", time: tn(25_500) });
  if (status === "dismissed")
    out.push({
      level: "INFO",
      msg: "Container terminated via DELETE",
      time: tn(11_500)
    });
  if (status === "lost")
    out.push({
      level: "WARN",
      msg: "Marking job lost after timeout",
      time: tn(76_000)
    });
  return out;
}

function resultsFor(processID, i) {
  switch (processID) {
    case "ras-2d-mesh":
      return {
        mesh: {
          href: `s3://sepex/results/mesh-${i}.h5`,
          title: "2D Mesh",
          type: "application/x-hdf5"
        },
        report: { value: { cells: 248_321 + i * 17, edges: 502_117 + i * 33 } }
      };
    case "flood-sim":
      return {
        flood_extent: {
          href: `s3://sepex/results/flood-${i}.gpkg`,
          title: "Flood extent"
        },
        max_depth: { value: 4.2 + (i % 7) * 0.3 }
      };
    case "terrain-extract":
      return {
        dem: { href: `s3://sepex/results/dem-${i}.tif`, title: "Clipped DEM" }
      };
    case "report-pdf":
      return {
        report: { href: `s3://sepex/reports/report-${i}.pdf`, title: "Report" }
      };
    default:
      return {};
  }
}

function hexDigest(i) {
  // 64-char hex string seeded by i — deterministic image digest stand-in.
  // Uses Math.imul to keep multiplications in 32-bit space without precision
  // loss (plain `*` collapses bits when the product overflows 2^53).
  let s = "";
  let n = Math.imul(i + 1, 2654435761) >>> 0;
  for (let k = 0; k < 64; k += 1) {
    n = (Math.imul(n, 1664525) + 1013904223) >>> 0;
    s += ((n >>> ((k * 3) & 28)) & 0xf).toString(16);
  }
  return s;
}

function metadataFor(processID, i, finishedAt) {
  // OGC API – Processes job metadata shape. Matches the Sepex Go struct in
  // /Users/curtis/Documents/code/sepex/api/jobs/metadata.go so the design
  // branch reflects what the real API returns today.
  const proc = PROCESSES.find((p) => p.id === processID);
  const endedAt = new Date(finishedAt);
  const startedAt = new Date(finishedAt - 60_000 - (i % 120) * 1000);
  const version = proc?.version || "1.0.0";
  return {
    "@context": "https://schemas.opengis.net/ogcapi/processes/part1/1.0",
    apiJobId: `mock-${i}`,
    process: {
      processId: processID,
      processVersion: version
    },
    image: {
      imageURI: `ghcr.io/dewberry/${processID}:${version}`,
      imageDigest: `sha256:${hexDigest(i)}`
    },
    commands: [processID, "--config", "/etc/sepex/job.yml"],
    generatedAtTime: endedAt.toISOString(),
    startedAtTime: startedAt.toISOString(),
    endedAtTime: endedAt.toISOString()
  };
}

// Seeded LCG so screenshots are reproducible across `next dev` restarts.
function makeRng(seed) {
  let s = seed >>> 0;
  return () => {
    s = (s * 1664525 + 1013904223) >>> 0;
    return s / 0x100000000;
  };
}

// Status weights vary by how recent a job is. Recent jobs are more likely to
// still be running; old jobs have all terminated.
function pickStatus(rng, ageMs) {
  const r = rng();
  if (ageMs < HOUR) {
    if (r < 0.55) return "running";
    if (r < 0.85) return "accepted";
    if (r < 0.95) return "successful";
    return "failed";
  }
  if (ageMs < DAY) {
    if (r < 0.04) return "running";
    if (r < 0.06) return "accepted";
    if (r < 0.91) return "successful";
    if (r < 0.96) return "failed";
    if (r < 0.99) return "dismissed";
    return "lost";
  }
  if (ageMs < 7 * DAY) {
    if (r < 0.92) return "successful";
    if (r < 0.96) return "failed";
    if (r < 0.99) return "dismissed";
    return "lost";
  }
  if (r < 0.93) return "successful";
  if (r < 0.97) return "failed";
  if (r < 0.995) return "dismissed";
  return "lost";
}

// Returns a job's `updated` timestamp, biased so a meaningful number of jobs
// land in each Dashboard window (24h / 7d / 30d).
function nextTimestamp(now, rng) {
  // Buckets: ~1% in last 1h, ~4% in last 24h, ~15% in last 7d, rest in 30d.
  const r = rng();
  if (r < 0.01) return now - rng() * HOUR;
  if (r < 0.05) return now - HOUR - rng() * (DAY - HOUR);
  if (r < 0.2) return now - DAY - rng() * (7 * DAY - DAY);
  return now - 7 * DAY - rng() * (23 * DAY);
}

const TOTAL_SEED_JOBS = 5000;

function buildSeedJobs(now) {
  const rng = makeRng(0xc0ffee);
  const jobs = [];
  for (let i = 0; i < TOTAL_SEED_JOBS; i += 1) {
    const updated = nextTimestamp(now, rng);
    const ageMs = now - updated;
    const status = pickStatus(rng, ageMs);
    const processID = PROCESS_IDS[Math.floor(rng() * PROCESS_IDS.length)];
    const submitter = SUBMITTERS[Math.floor(rng() * SUBMITTERS.length)];
    const created = updated - 90_000;
    const jobID = `mock-${String(i).padStart(5, "0")}-${processID}`;
    const reasonIdx = Math.floor(rng() * FAIL_REASONS.length);
    jobs.push({
      type: "process",
      jobID,
      processID,
      status,
      submitter,
      created: new Date(created).toISOString(),
      updated: new Date(updated).toISOString(),
      tags: makeTags(i),
      host: i % 2 === 0 ? "docker" : "subprocess",
      hostJobID: `${i % 2 === 0 ? "container" : "pid"}-${10000 + i}`,
      mode: "async",
      lastErrorMessage:
        status === "failed" ? FAIL_REASONS[reasonIdx] : undefined,
      inputs: makeInputs(processID, i),
      _submittedAt: updated
    });
  }
  // Sort newest first so the in-memory store is already in display order.
  jobs.sort((a, b) => new Date(b.updated) - new Date(a.updated));
  return jobs;
}

function jobIndexFromID(jobID) {
  // jobID format: `mock-NNNNN-<processID>` — digits start at offset 5.
  const m = /^mock-(\d+)-/.exec(jobID);
  return m ? parseInt(m[1], 10) : 0;
}

// Only generate logs / results / metadata for the most recent N jobs.
// The PM demo only drills into a handful of jobs; the other thousands exist
// so the Dashboard charts and pagination look realistic. Keeping seed data
// O(1000s) instead of O(50k log entries) keeps boot time fast.
const EAGER_DETAIL_COUNT = 250;

function buildSeedLogs(jobs) {
  const logs = {};
  const slice = jobs.slice(0, EAGER_DETAIL_COUNT);
  for (const job of slice) {
    const finishedAt = new Date(job.updated).getTime();
    logs[job.jobID] = {
      process_logs: processLogsFor(
        job.status,
        job.processID,
        jobIndexFromID(job.jobID),
        finishedAt
      ),
      server_logs: serverLogsFor(job.status, job.jobID, finishedAt)
    };
  }
  return logs;
}

function buildSeedResults(jobs) {
  const results = {};
  const metadata = {};
  const slice = jobs.slice(0, EAGER_DETAIL_COUNT);
  for (const job of slice) {
    if (job.status !== "successful") continue;
    const i = jobIndexFromID(job.jobID);
    const finishedAt = new Date(job.updated).getTime();
    results[job.jobID] = resultsFor(job.processID, i);
    metadata[job.jobID] = metadataFor(job.processID, i, finishedAt);
  }
  return { results, metadata };
}

function applyScenario(jobs) {
  if (SCENARIO === "empty") return [];
  if (SCENARIO === "lots-running") {
    return jobs.map((j, i) =>
      i < 20 ? { ...j, status: i < 4 ? "accepted" : "running" } : j
    );
  }
  if (SCENARIO === "all-failed") {
    return jobs.map((j, i) => ({
      ...j,
      status: "failed",
      lastErrorMessage: FAIL_REASONS[i % FAIL_REASONS.length]
    }));
  }
  return jobs;
}

export function buildSeed(now = Date.now()) {
  const rawJobs = SCENARIO === "empty" ? [] : buildSeedJobs(now);
  const jobs = applyScenario(rawJobs);
  const logs = buildSeedLogs(jobs);
  const { results, metadata } = buildSeedResults(jobs);
  const processes = SCENARIO === "empty" ? [] : PROCESSES;
  return { jobs, logs, results, metadata, processes };
}

export { SCENARIO, FAIL_REASONS };
