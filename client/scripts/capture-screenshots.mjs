import { mkdir } from "node:fs/promises";
import path from "node:path";
import { chromium } from "@playwright/test";

const BASE_URL = process.env.BASE_URL || "http://localhost:3000";
const OUT_DIR = path.resolve(process.cwd(), "../research/screenshots/built");

const VIEWPORT_DESKTOP = { width: 1440, height: 900 };

const JOB_ID = "mock-0008-ras-2d-mesh"; // a successful job in the mock seed

const SHOTS = [
  { name: "landing-light", path: "/", theme: "light" },
  { name: "landing-dark", path: "/", theme: "dark" },
  { name: "dashboard-light", path: "/dashboard", theme: "light" },
  { name: "dashboard-dark", path: "/dashboard", theme: "dark" },
  {
    name: "builder-light",
    path: "/builder",
    theme: "light",
    selectProcess: true
  },
  {
    name: "builder-dark",
    path: "/builder",
    theme: "dark",
    selectProcess: true
  },
  { name: "jobs-light", path: "/jobs", theme: "light" },
  { name: "jobs-dark", path: "/jobs", theme: "dark" },
  {
    name: "jobs-drawer-open-dark",
    path: `/jobs?selected=${JOB_ID}`,
    theme: "dark"
  },
  { name: "job-detail-light", path: `/jobs/${JOB_ID}`, theme: "light" },
  { name: "job-detail-dark", path: `/jobs/${JOB_ID}`, theme: "dark" }
];

async function applyTheme(page, theme) {
  await page.evaluate((t) => {
    localStorage.setItem("theme", t);
    document.documentElement.classList.remove("dark", "light");
    document.documentElement.classList.add(t);
  }, theme);
}

async function captureOne(browser, shot) {
  const context = await browser.newContext({ viewport: VIEWPORT_DESKTOP });
  const page = await context.newPage();

  // First load the app at root so localStorage is on the right origin, set theme, then nav.
  await page.goto(BASE_URL, { waitUntil: "domcontentloaded" });
  await applyTheme(page, shot.theme);
  await page.goto(`${BASE_URL}${shot.path}`, { waitUntil: "domcontentloaded" });
  // Polling means networkidle may never settle. Wait a fixed budget for data.
  await page.waitForTimeout(2500);

  if (shot.selectProcess) {
    // Click the process picker, pick the first option (RAS 2D Mesh Build).
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

  if (shot.drawer) {
    const firstJobIdButton = page
      .locator("table tbody tr")
      .first()
      .locator("button, a")
      .first();
    await firstJobIdButton.click({ trial: false }).catch(() => {});
    await page.waitForTimeout(1000);
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
