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
  "m.santos@example.com"
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

function metadataFor(processID, i) {
  return {
    runtimeSeconds: 60 + i * 7,
    workerHost: i % 2 === 0 ? "docker-1" : "docker-2",
    pluginVersion: "1.4.2",
    inputs: makeInputs(processID, i)
  };
}

// Distribute statuses to populate Dashboard buckets meaningfully.
function statusForIndex(i) {
  // 0..2 within last hour: 2 running, 1 accepted
  if (i < 3) return ["running", "running", "accepted"][i];
  // 3..6 within last 6h: more accepted/running mixed with finished
  if (i < 7) return ["successful", "running", "accepted", "successful"][i - 3];
  // 7..14 within last 24h
  if (i < 15) {
    return [
      "successful",
      "successful",
      "failed",
      "successful",
      "dismissed",
      "successful",
      "failed",
      "successful"
    ][i - 7];
  }
  // 15..29 within last 7d
  if (i < 30) {
    return [
      "successful",
      "successful",
      "failed",
      "successful",
      "successful",
      "dismissed",
      "successful",
      "lost",
      "successful",
      "failed",
      "successful",
      "successful",
      "dismissed",
      "successful",
      "failed"
    ][i - 15];
  }
  // 30..49 within last 30d
  return [
    "successful",
    "failed",
    "successful",
    "successful",
    "lost",
    "successful",
    "successful",
    "dismissed",
    "successful",
    "failed",
    "successful",
    "successful",
    "successful",
    "failed",
    "dismissed",
    "successful",
    "successful",
    "lost",
    "successful",
    "failed"
  ][i - 30];
}

function timeForIndex(i, now) {
  // i=0..2 minutes ago, i=3..6 hours ago, etc.
  if (i < 3) return now - (i + 1) * 5 * 60 * 1000;
  if (i < 7) return now - (i - 2) * HOUR;
  if (i < 15) return now - (8 + (i - 7) * 2) * HOUR;
  if (i < 30)
    return now - (1 + Math.floor((i - 15) / 2)) * DAY - (i % 2) * 6 * HOUR;
  return now - (8 + (i - 30)) * DAY - (i % 3) * 4 * HOUR;
}

function buildSeedJobs(now) {
  const jobs = [];
  for (let i = 0; i < 50; i += 1) {
    const status = statusForIndex(i);
    const processID = pick(PROCESS_IDS, i);
    const updated = timeForIndex(i, now);
    // Each seed job ran for 90s before reaching its final updated state.
    // For active jobs, `created` is still earlier — `getElapsed` will compute
    // (now - created) on the client side.
    const created = updated - 90_000;
    const jobID = `mock-${String(i).padStart(4, "0")}-${processID}`;
    jobs.push({
      type: "process",
      jobID,
      processID,
      status,
      submitter: pick(SUBMITTERS, i),
      created: new Date(created).toISOString(),
      updated: new Date(updated).toISOString(),
      tags: makeTags(i),
      host: i % 2 === 0 ? "docker" : "subprocess",
      hostJobID: `${i % 2 === 0 ? "container" : "pid"}-${10000 + i}`,
      mode: "async",
      message:
        status === "failed" ? FAIL_REASONS[i % FAIL_REASONS.length] : undefined,
      inputs: makeInputs(processID, i),
      _submittedAt: updated // not exposed; used for time progression of new submissions
    });
  }
  return jobs;
}

function buildSeedLogs(jobs) {
  const logs = {};
  for (const job of jobs) {
    const finishedAt = new Date(job.updated).getTime();
    logs[job.jobID] = {
      process_logs: processLogsFor(
        job.status,
        job.processID,
        parseInt(job.jobID.slice(5, 9), 10) || 0,
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
  for (const job of jobs) {
    const i = parseInt(job.jobID.slice(5, 9), 10) || 0;
    if (job.status === "successful") {
      results[job.jobID] = resultsFor(job.processID, i);
      metadata[job.jobID] = metadataFor(job.processID, i);
    }
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
      message: FAIL_REASONS[i % FAIL_REASONS.length]
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
