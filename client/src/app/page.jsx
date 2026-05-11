"use client";

import { useRouter } from "next/navigation";
import { Activity, PlayCircle, Terminal } from "lucide-react";
import ThemeSwitcher from "@/app/_components/ThemeSwitcher";
import { Button } from "@/components/ui/button";

const SPARK_BARS = [3, 4, 5, 7, 6, 8, 9, 7, 10, 11, 9, 12, 11, 13, 14, 15];
const SPARK_MAX = 15;

// "View API docs" only renders when an explicit URL is configured — the
// current Sepex servers (papi included) don't serve docs at a discoverable
// path, so we don't synthesize one from the base URL.
const apiDocsUrl = process.env.NEXT_PUBLIC_API_DOCS_URL || "";

function TopographicBackdrop() {
  return (
    <div
      aria-hidden
      className="pointer-events-none absolute inset-0 opacity-[0.04]"
      style={{
        backgroundImage: `
          repeating-linear-gradient(0deg, transparent, transparent 40px, currentColor 40px, currentColor 41px),
          repeating-linear-gradient(90deg, transparent, transparent 40px, currentColor 40px, currentColor 41px),
          repeating-radial-gradient(circle at 20% 30%, transparent 0, transparent 60px, currentColor 60px, currentColor 61px),
          repeating-radial-gradient(circle at 80% 70%, transparent 0, transparent 90px, currentColor 90px, currentColor 91px),
          repeating-radial-gradient(circle at 50% 50%, transparent 0, transparent 120px, currentColor 120px, currentColor 121px)
        `
      }}
    />
  );
}

function LifecycleCard() {
  return (
    <>
      <div className="mb-2 font-mono text-xs text-muted-foreground">
        image-resize-batch-042
      </div>
      <div className="flex flex-wrap items-center gap-2">
        <span className="rounded-full bg-status-accepted px-2 py-1 text-xs text-white">
          accepted
        </span>
        <span className="text-muted-foreground">→</span>
        <span className="flex items-center gap-1 rounded-full bg-status-running px-2 py-1 text-xs text-white">
          <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-white" />
          running
        </span>
        <span className="text-muted-foreground">→</span>
        <span className="rounded-full bg-status-successful px-2 py-1 text-xs text-white">
          successful
        </span>
      </div>
    </>
  );
}

function SparklineCard() {
  return (
    <>
      <div className="mb-3 flex items-start justify-between">
        <div className="text-xs text-muted-foreground">
          Jobs completed · 24h
        </div>
        <div className="text-xs text-status-successful">+12%</div>
      </div>
      <div
        role="img"
        aria-label={`Jobs completed over the last 24 hours, trending up 12 percent. Hourly counts: ${SPARK_BARS.join(", ")}.`}
        className="flex h-12 items-end gap-0.5"
      >
        {SPARK_BARS.map((value, i) => (
          <div
            key={i}
            aria-hidden="true"
            className="flex-1 rounded-sm bg-dewberry-teal"
            style={{ height: `${(value / SPARK_MAX) * 100}%` }}
          />
        ))}
      </div>
    </>
  );
}

function LogTailCard() {
  return (
    <div className="space-y-1 font-mono text-xs text-muted-foreground">
      <div>[INFO] 14:23:01 ndvi-tile-007 accepted</div>
      <div>[INFO] 14:23:02 ndvi-tile-007 → running</div>
      <div>[INFO] 14:23:18 ndvi-tile-007 → successful</div>
    </div>
  );
}

export default function LandingPage() {
  const router = useRouter();

  // Auth is intentionally off for v1 — Sign in just routes into the app.
  // See research/todo.md for the wire-up-real-auth follow-up.
  const handleSignIn = () => router.push("/dashboard");

  return (
    <div className="flex min-h-screen flex-col">
      <header className="border-b border-border bg-card px-6">
        <div className="mx-auto flex h-14 max-w-7xl items-center">
          <div className="flex items-center gap-2 font-semibold">
            <Activity className="h-5 w-5 text-primary" />
            <span>Sepex Viewer</span>
          </div>
        </div>
      </header>

      <main className="flex-1">
        <section className="relative flex min-h-[80vh] items-center overflow-hidden px-6 py-16 lg:py-24">
          <TopographicBackdrop />
          <div className="relative z-10 mx-auto w-full max-w-7xl">
            <div className="grid items-center gap-12 lg:grid-cols-2 lg:gap-16">
              <div className="space-y-8">
                <div className="space-y-6">
                  <div className="text-sm uppercase tracking-wider text-muted-foreground">
                    Open source · OGC API – Processes UI
                  </div>
                  <h1 className="text-5xl font-bold leading-tight">
                    Run, monitor, and inspect compute jobs.
                  </h1>
                  <p className="text-xl leading-relaxed text-muted-foreground">
                    A modern web UI for any OGC API – Processes server. Submit
                    jobs, watch them run with live logs, and inspect results —
                    works for hydrology models, geospatial analysis, ML
                    pipelines, image processing, or any other long-running
                    compute workload your team registers.
                  </p>
                </div>

                <div className="flex flex-col flex-wrap items-stretch gap-3 sm:flex-row sm:items-start">
                  <div className="flex flex-col gap-1">
                    <Button size="lg" onClick={handleSignIn}>
                      Sign in
                    </Button>
                    <span className="text-xs text-muted-foreground">
                      Auth is configurable — Keycloak by default, or any OIDC
                      provider.
                    </span>
                  </div>
                  {apiDocsUrl ? (
                    <Button size="lg" variant="outline" asChild>
                      <a
                        href={apiDocsUrl}
                        target="_blank"
                        rel="noopener noreferrer"
                      >
                        View API docs
                      </a>
                    </Button>
                  ) : null}
                </div>
              </div>

              <div className="relative hidden h-[400px] lg:block">
                <div
                  className="absolute right-0 top-8 w-72 rounded-lg border border-border bg-card p-4 shadow-xl"
                  style={{ transform: "rotate(2deg)" }}
                >
                  <LifecycleCard />
                </div>
                <div
                  className="absolute right-12 top-32 w-64 rounded-lg border border-border bg-card p-4 shadow-xl"
                  style={{ transform: "rotate(-1deg)" }}
                >
                  <SparklineCard />
                </div>
                <div
                  className="absolute right-4 top-64 w-80 rounded-lg border border-border bg-card p-4 shadow-xl"
                  style={{ transform: "rotate(1deg)" }}
                >
                  <LogTailCard />
                </div>
              </div>

              <div className="space-y-4 lg:hidden">
                <div className="rounded-lg border border-border bg-card p-4 shadow-lg">
                  <LifecycleCard />
                </div>
                <div className="rounded-lg border border-border bg-card p-4 shadow-lg">
                  <SparklineCard />
                </div>
                <div className="rounded-lg border border-border bg-card p-4 shadow-lg">
                  <LogTailCard />
                </div>
              </div>
            </div>
          </div>
        </section>

        <section className="bg-background px-6 py-16">
          <div className="mx-auto grid max-w-7xl gap-6 sm:grid-cols-1 md:grid-cols-3">
            <div className="rounded-lg border border-border bg-card p-6 transition-shadow hover:shadow-lg">
              <div className="mb-4 flex h-12 w-12 items-center justify-center rounded-full bg-status-successful/20">
                <PlayCircle className="h-6 w-6 text-status-successful" />
              </div>
              <h2 className="mb-2 text-lg font-semibold">Submit jobs</h2>
              <p className="text-sm text-muted-foreground">
                Pick any registered process, fill in inputs from the
                auto-generated form, execute sync or async.
              </p>
            </div>

            <div className="rounded-lg border border-border bg-card p-6 transition-shadow hover:shadow-lg">
              <div className="mb-4 flex h-12 w-12 items-center justify-center rounded-full bg-status-running/20">
                <Activity className="h-6 w-6 text-status-running" />
              </div>
              <h2 className="mb-2 text-lg font-semibold">Watch them run</h2>
              <p className="text-sm text-muted-foreground">
                Live status and streaming process logs from whatever execution
                backend you&rsquo;ve configured.
              </p>
            </div>

            <div className="rounded-lg border border-border bg-card p-6 transition-shadow hover:shadow-lg">
              <div className="mb-4 flex h-12 w-12 items-center justify-center rounded-full bg-dewberry-teal/20">
                <Terminal className="h-6 w-6 text-dewberry-teal" />
              </div>
              <h2 className="mb-2 text-lg font-semibold">Audit results</h2>
              <p className="text-sm text-muted-foreground">
                Logs, metadata, and download links to result files — every job
                addressable by URL.
              </p>
            </div>
          </div>
        </section>
      </main>

      <footer className="border-t border-border px-6 py-8">
        <div className="mx-auto flex max-w-7xl flex-col items-center justify-between gap-4 text-xs text-muted-foreground md:flex-row">
          <div className="flex items-center gap-3">
            <div>
              Built by Dewberry · MIT license · Free for anyone to deploy
            </div>
            <a
              href="https://github.com/Dewberry/sepex-viewer"
              target="_blank"
              rel="noopener noreferrer"
              className="transition-colors hover:text-foreground"
            >
              GitHub
            </a>
          </div>
          <ThemeSwitcher />
        </div>
      </footer>
    </div>
  );
}
