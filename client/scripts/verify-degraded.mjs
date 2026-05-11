// One-off helper: with the dev server running WITHOUT the proposed-API flag,
// captures /dashboard and /jobs so we can eyeball that:
//   - the time-range picker is hidden
//   - the dashboard footnote is shown
//   - FailedJobsAlert has no inline reasons
//   - the pagination footer shows "Page N" only (no "of M")
// Outputs to /tmp/sepex-verify/{dashboard,jobs}.png — not committed.

import { mkdir } from "node:fs/promises";
import path from "node:path";
import { chromium } from "@playwright/test";

const BASE_URL = "http://localhost:3000";
const OUT_DIR = "/tmp/sepex-verify";

async function main() {
  await mkdir(OUT_DIR, { recursive: true });
  const browser = await chromium.launch();
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 }
  });
  const page = await context.newPage();
  await page.goto(BASE_URL, { waitUntil: "domcontentloaded" });
  await page.evaluate(() => {
    localStorage.setItem("theme", "light");
    document.documentElement.classList.add("light");
  });

  for (const route of ["/dashboard", "/jobs"]) {
    await page.goto(`${BASE_URL}${route}`, { waitUntil: "domcontentloaded" });
    await page.waitForTimeout(3000);
    const name = route.slice(1) || "landing";
    await page.screenshot({
      path: path.join(OUT_DIR, `${name}.png`),
      fullPage: true
    });
    console.log(`✓ ${route}`);
  }
  await browser.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
