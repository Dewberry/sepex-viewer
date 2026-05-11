// Captures the 5 "proposed-state" screenshots for the PM walkthrough — what
// the Sepex Viewer redesign looks like once items A, C, D, H, I from
// research/proposed-next-steps.md ship on the Sepex Go API.
//
// Run with the dev server up *and* the feature flag enabled:
//   NEXT_PUBLIC_ENABLE_PROPOSED_API=true npm run dev      (terminal 1)
//   node scripts/capture-proposed-screenshots.mjs         (terminal 2)
//
// Outputs to research/screenshots/proposed/{2a,2c,2d,2h,2i}.png

import { mkdir } from "node:fs/promises";
import path from "node:path";
import { chromium } from "@playwright/test";

const BASE_URL = process.env.BASE_URL || "http://localhost:3000";
const OUT_DIR = path.resolve(process.cwd(), "../research/screenshots/proposed");

const VIEWPORT = { width: 1440, height: 900 };

async function applyTheme(page, theme) {
  await page.evaluate((t) => {
    localStorage.setItem("theme", t);
    document.documentElement.classList.remove("dark", "light");
    document.documentElement.classList.add(t);
  }, theme);
}

async function newPage(browser) {
  const context = await browser.newContext({ viewport: VIEWPORT });
  const page = await context.newPage();
  await page.goto(BASE_URL, { waitUntil: "domcontentloaded" });
  await applyTheme(page, "light");
  return { context, page };
}

async function shotDashboard24h(browser) {
  const { context, page } = await newPage(browser);
  await page.goto(`${BASE_URL}/dashboard`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(3500); // let charts paint
  await page.screenshot({
    path: path.join(OUT_DIR, "2a.png"),
    fullPage: true
  });
  await context.close();
}

async function shotDashboard30d(browser) {
  const { context, page } = await newPage(browser);
  await page.goto(`${BASE_URL}/dashboard`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(2000);
  // TimeRangePicker buttons use role="tab". Pick the "30d" tab.
  const thirtyD = page.getByRole("tab", { name: /^30d$/i });
  await thirtyD.click({ timeout: 4000 });
  await page.waitForTimeout(3500);
  await page.screenshot({
    path: path.join(OUT_DIR, "2c.png"),
    fullPage: true
  });
  await context.close();
}

async function shotJobsPagination(browser) {
  const { context, page } = await newPage(browser);
  await page.goto(`${BASE_URL}/jobs`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(2500);
  // Advance to page 3 so the footer shows "Page 3 of N". Scope to the
  // pagination footer container so we don't accidentally hit a "Next" elsewhere.
  const footer = page.locator("text=Per page").locator("..").locator("..");
  for (let i = 0; i < 2; i += 1) {
    await footer
      .getByRole("button", { name: /^next$/i })
      .click({ timeout: 4000 })
      .catch(() => {});
    await page.waitForTimeout(800);
  }
  await page.keyboard.press("Escape"); // dismiss any popover
  await page.locator("body").click({ position: { x: 10, y: 10 } });
  await page.waitForTimeout(400);
  await page.screenshot({
    path: path.join(OUT_DIR, "2d.png"),
    fullPage: true
  });
  await context.close();
}

async function shotFailedJobsAlert(browser) {
  const { context, page } = await newPage(browser);
  await page.goto(`${BASE_URL}/dashboard`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(3500);
  // FailedJobsAlert: h3 sits inside a flex wrapper inside the alert card.
  // Go up two levels to grab the whole card.
  const alert = page
    .locator("h3", { hasText: /Failed Jobs?$/ })
    .first()
    .locator("..")
    .locator("..");
  await alert.scrollIntoViewIfNeeded();
  await page.waitForTimeout(400);
  await alert.screenshot({ path: path.join(OUT_DIR, "2h.png") });
  await context.close();
}

async function shotCommandPalette(browser) {
  const { context, page } = await newPage(browser);
  await page.goto(`${BASE_URL}/dashboard`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(2500);
  await page.evaluate(() => {
    window.dispatchEvent(new Event("sepex:open-command-palette"));
  });
  await page.waitForTimeout(500);
  await page.keyboard.type("trini", { delay: 60 });
  await page.waitForTimeout(800);
  await page.screenshot({
    path: path.join(OUT_DIR, "2i.png"),
    fullPage: false
  });
  await context.close();
}

async function main() {
  await mkdir(OUT_DIR, { recursive: true });
  const browser = await chromium.launch();
  try {
    for (const [label, fn] of [
      ["2a — Dashboard (24h)", shotDashboard24h],
      ["2c — Dashboard (30d)", shotDashboard30d],
      ["2d — Jobs paginated", shotJobsPagination],
      ["2h — FailedJobsAlert", shotFailedJobsAlert],
      ["2i — Command palette", shotCommandPalette]
    ]) {
      await fn(browser);
      process.stdout.write(`✓ ${label}\n`);
    }
  } finally {
    await browser.close();
  }
  console.log(`\nDone — wrote 5 screenshots to ${OUT_DIR}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
