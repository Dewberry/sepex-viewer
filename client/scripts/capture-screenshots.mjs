import { mkdir } from "node:fs/promises";
import path from "node:path";
import { chromium } from "@playwright/test";

const BASE_URL = process.env.BASE_URL || "http://localhost:3000";
const OUT_DIR = path.resolve(process.cwd(), "../research/screenshots/built");

const VIEWPORT_DESKTOP = { width: 1440, height: 900 };
const VIEWPORT_MOBILE = { width: 390, height: 844 }; // iPhone 14-class

async function pickSuccessfulJobID() {
  const res = await fetch(
    `${BASE_URL}/api/mock/jobs?status=successful&limit=1`
  );
  const body = await res.json();
  if (!body.jobs?.[0]?.jobID) {
    throw new Error("No successful jobs in the mock seed — bump fixtures?");
  }
  return body.jobs[0].jobID;
}

const JOB_ID = await pickSuccessfulJobID();

const PAGES = [
  { id: "landing", path: "/" },
  { id: "dashboard", path: "/dashboard" },
  { id: "builder", path: "/builder", selectProcess: true },
  { id: "jobs", path: "/jobs" },
  {
    id: "jobs-drawer-open",
    path: `/jobs?selected=${JOB_ID}`,
    onlyTheme: "dark",
    onlyDevice: "desktop"
  },
  { id: "job-detail", path: `/jobs/${JOB_ID}` }
];

const THEMES = ["light", "dark"];
const DEVICES = [
  { id: "desktop", viewport: VIEWPORT_DESKTOP, suffix: "" },
  { id: "mobile", viewport: VIEWPORT_MOBILE, suffix: "-mobile" }
];

function buildShots() {
  const shots = [];
  for (const p of PAGES) {
    for (const device of DEVICES) {
      if (p.onlyDevice && p.onlyDevice !== device.id) continue;
      for (const theme of THEMES) {
        if (p.onlyTheme && p.onlyTheme !== theme) continue;
        shots.push({
          name: `${p.id}${device.suffix}-${theme}`,
          path: p.path,
          theme,
          viewport: device.viewport,
          selectProcess: p.selectProcess
        });
      }
    }
  }
  return shots;
}

const SHOTS = buildShots();

async function applyTheme(page, theme) {
  await page.evaluate((t) => {
    localStorage.setItem("theme", t);
    document.documentElement.classList.remove("dark", "light");
    document.documentElement.classList.add(t);
  }, theme);
}

async function captureOne(browser, shot) {
  const context = await browser.newContext({ viewport: shot.viewport });
  const page = await context.newPage();

  await page.goto(BASE_URL, { waitUntil: "domcontentloaded" });
  await applyTheme(page, shot.theme);
  await page.goto(`${BASE_URL}${shot.path}`, {
    waitUntil: "domcontentloaded"
  });
  await page.waitForTimeout(2500);

  if (shot.selectProcess) {
    await page
      .getByRole("button", { name: /select a process/i })
      .click()
      .catch(() => {});
    await page.waitForTimeout(300);
    const firstOption = page
      .locator("[role='option'], button")
      .filter({ hasText: /RAS 2D Mesh Build|HEC-RAS|hms-runner|flood-sim/i })
      .first();
    await firstOption.click({ timeout: 2000 }).catch(() => {});
    await page.waitForTimeout(1500);
  }

  const target = path.join(OUT_DIR, `${shot.name}.png`);
  await page.screenshot({ path: target, fullPage: true });
  await context.close();
  return target;
}

async function main() {
  await mkdir(OUT_DIR, { recursive: true });
  const browser = await chromium.launch();
  const written = [];
  try {
    for (const shot of SHOTS) {
      const target = await captureOne(browser, shot);
      written.push(target);
      process.stdout.write(`✓ ${shot.name}\n`);
    }
  } finally {
    await browser.close();
  }
  console.log(`\nDone — wrote ${written.length} screenshots to ${OUT_DIR}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
