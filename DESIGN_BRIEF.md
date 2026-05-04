# Sepex Viewer — Design Brief

**For:** Claude Design / v0 / any UI generation tool
**Author:** Aaron Curtis
**Status:** Draft v1 — 2026-04-29
**Project root:** `~/Documents/code/sepex-viewer`

---

## 0. TL;DR for the designer

We're rebuilding a **Streamlit dashboard** as a modern **Next.js (App Router) + shadcn/ui + Tailwind** web app. It's a **front-end for the [Sepex API](https://github.com/Dewberry/sepex)** — an OGC API – Processes server (think: serverless functions for geospatial / scientific compute, defined as YAML-described "processes" backed by Docker containers, AWS Batch, or subprocesses).

The closest mental model is **Grafana for batch jobs**: dashboards of job activity at the top, drill-down into individual jobs and their logs/results/metadata at the bottom, plus tooling to author and submit new runs.

The target users are **Dewberry's water-resources engineers and data scientists** — technically literate, comfortable with JSON/YAML, who today launch jobs from CLI or Postman and have nowhere good to monitor them. They want one polished dashboard, not five tabs across MinIO, pgAdmin, Swagger, and a Streamlit prototype.

**Three primary routes**: `/` (overview dashboard), `/builder` (compose a payload for a process), `/jobs` (browse all jobs with detail panel). Plus a **landing page** at `/` for unauthenticated users.

**Inspiration**: Grafana, Vercel Dashboard, Linear, GitHub Actions runs view.

**Brand**: Dewberry — primary `#7A1341` (deep magenta), CTA teal `#336B87`, charcoal `#2C2924`. Open Sans. Dark-mode default, light-mode supported.

---

## 1. Recommended platform for design output

You asked whether Claude Design is the right tool. Quick take:

| Tool | Best for | Why for this project |
|------|----------|----------------------|
| **v0 (Vercel)** | Final, drop-in code | Outputs Next.js + shadcn + Tailwind directly. Same stack as the target. PR-ready. **Recommended for production output.** |
| **Claude Design (claude.ai canvas)** | Early visual exploration, design system thinking | Great at high-level layout iteration and copy. Use to *decide* the shape, then hand that to v0 to *build* it. |
| **Figma + Figma Make** | Pixel-perfect mocks before code | Use only if a stakeholder needs to sign off on visuals before implementation. Skip otherwise. |

**Suggested workflow:** Run this brief through **Claude Design** to get 4–6 alternative dashboard layouts and a landing-page direction. Pick a winner. Then feed that winner + this brief into **v0** screen-by-screen to produce shadcn/Next.js code.

---

## 2. Product context

### What sepex is
Sepex is a Go-based HTTP API conforming to the **OGC API – Processes** spec. It registers "processes" (compute jobs) defined by YAML files. Each process declares its inputs, outputs, container image, and resource needs. Clients submit jobs by POSTing JSON inputs. Jobs run sync or async, produce logs/results/metadata, and are stored in Postgres + MinIO/S3.

A *process* is a recipe (e.g., `hms-runner`, `twodimfim`, `pyecho`). A *job* is an instance of running that process with specific inputs (UUID, status, submitter, timestamps, tags).

### What the viewer is
A web UI in front of that API. The Streamlit prototype proved the workflow:

1. See what's happening across all jobs (charts, KPIs, recent failures).
2. Drill into a single job: status, logs, metadata, results.
3. Author and submit a new run (pick a process, fill in inputs, execute).
4. Manage jobs (dismiss running ones, batch-dismiss).
5. Browse files in the shared `/data` and `/mnt/sepex` mounts.

### Who uses it
- **Engineers** who run hydrology/hydraulics simulations (HMS, RAS, 2D FIM). They submit dozens of jobs per week and need to know which failed and why.
- **Data scientists** authoring new processes. They need to inspect inputs/outputs and test payloads.
- **Project managers** glancing at "is the queue moving?" and "did the overnight run succeed?"
- **Admins** monitoring resource pools (CPU/memory) and dismissing stuck jobs.

### Where it runs
Self-hosted via Docker Compose. Behind Keycloak auth in production (currently disabled in dev). Internal Dewberry tool — not public.

---

## 3. Stack constraints (non-negotiable)

These match Dewberry's other internal Next.js app, **reality-view** (`~/Documents/code/reality-view/client`). Keeping parity makes both apps easier to maintain and cross-train on.

- **Next.js 16** (App Router, RSC where possible)
- **React 19**
- **JavaScript** (no TypeScript) — match reality-view; team preference
- **Tailwind CSS v4** with `@theme inline` block
- **shadcn/ui** (New York style, RSC-enabled)
- **Radix UI** primitives
- **Lucide React** icons
- **next-auth v5-beta** with **Keycloak** provider (with a `NEXT_PUBLIC_DEV_BYPASS_AUTH` escape hatch)
- **Prisma** if we need a local DB (probably not — sepex API is the source of truth)
- **Open Sans** via Google Fonts
- Layout groups: `(dashboard)/` for authenticated app, root for landing/auth

For **data fetching**: prefer server components with `fetch()` to the sepex API for initial loads, plus a lightweight client-side polling layer for live job status (suggest **TanStack Query** v5 — it's the missing piece reality-view doesn't have but we need for poll-heavy job monitoring). This is the one allowed deviation from reality-view's stack.

For **forms**: react-hook-form + Zod. The payload builder needs validation per process schema.

---

## 4. Visual direction

### Primary inspiration: **Grafana**

What we want from Grafana:
- Dense information without feeling cluttered.
- Time-series charts as a first-class element on the overview page.
- Status pills with color semantics that the eye can scan in 200ms.
- A "things going wrong right now" panel that is always visible.
- Time-range picker in a global header (Last 24h / 7d / 30d / Custom).
- Auto-refresh toggle with selectable interval.

### Secondary inspiration
- **Vercel Dashboard** — for clean cards, deployment-list patterns, the run-detail drawer.
- **Linear** — for the keyboard-driven feel, command palette (`⌘K`), and tasteful empty states.
- **GitHub Actions** — for the job log viewer (collapsible groups, line numbers, ANSI color, search).

### What to avoid
- The "AI dashboard" cliché: pastel gradients, glassmorphism cards, generic illustration with floating dots.
- Excessive iconography. Use icons to *replace* text only when the icon is universally understood (search, settings, close, chevron). Pair icons with labels otherwise.
- Light, spacious "consumer" feel. This is a daily-driver tool used 4–8 hours a day. Density matters. Pack information.

### Brand palette (from `/Users/curtis/Documents/code/reality-view/client/src/app/styles/globals.css`)

```
Primary (Dewberry magenta):  #7A1341
CTA / Ring (teal):           #336B87
Charcoal (dark surface):     #2C2924
Error red:                   #8B2E40
```

Use Dewberry magenta sparingly — for primary CTAs, branded header accents, and active-nav indicators. CTA teal for focus rings and links. Charcoal for the app shell in dark mode. Dark mode is the default; light mode is fully supported.

### Status color semantics (locked)

| Status | Color | Visual treatment |
|--------|-------|------------------|
| `successful` | Green (e.g. `#22c55e`) | Solid pill, white text |
| `running` | Amber/gold (e.g. `#eab308`) | Pulsing dot + label |
| `accepted` | Sky/blue (e.g. `#0ea5e9`) | Solid pill |
| `failed` | Red (the brand error red `#8B2E40` or a brighter red like `#ef4444`) | Solid pill |
| `dismissed` | Slate/gray (`#64748b`) | Outline pill |
| `lost` | Purple (`#a855f7`) | Outline pill with warning icon |

These five+ states come straight from the API; do not invent new ones.

### Typography
- **Open Sans** for UI text (300, 400, 500, 700)
- **JetBrains Mono** or **Geist Mono** for log lines, JSON/YAML viewers, code editor, job IDs
- Job IDs (UUIDs) are long — render the **last 8 chars** in tables, full ID copyable on hover/click
- Timestamps in **UTC** with a tooltip showing local time

---

## 5. Information architecture

### Top-level routes

| Route | Purpose | Auth required |
|-------|---------|---------------|
| `/` | **Overview dashboard** — what's happening across the whole system. Charts, KPIs, recent failures, queue health. | Yes |
| `/builder` | **Payload Builder** — pick a process, fill in inputs, validate, save or submit. | Yes |
| `/jobs` | **Jobs** — browseable table of all jobs with filters and a job-detail panel. | Yes |
| `/jobs/[jobID]` | **Job detail** — deep-link to a single job (logs, results, metadata, dismiss action). Could also be a slide-over instead of a separate route — designer's call, but deep-linkability is required. | Yes |
| `/processes` | (Future) Catalog of all registered processes. List + detail. | Yes |
| `/processes/[id]` | (Future) Process detail including raw YAML, schema, recent runs. | Yes |
| `/admin` | (Future, role-gated) Resource pool dashboard, plugin reload, dismiss-all. | Yes (admin role) |
| `/files` | (Optional) Browse the shared `/data` mount. Less critical — could be a tab on `/jobs` instead. | Yes |
| `/login` | Keycloak handoff. | No |
| `/landing` or `/` (unauth) | **Landing page** — marketing page when not signed in. See §7. | No |

The current Streamlit prototype has experimented with: `home.py`, `pages/builder.py`, `pages/jobs.py`, `pages/manager.py` (submit + dismiss), `pages/monitor.py` (the jobs/details split), `pages/data_files.py` (browser). The Next.js redesign should consolidate `manager` into `/builder` (submit at the bottom of the builder flow) and `monitor` into `/jobs` (which is already the detail panel).

### Global navigation

- **Top app bar (h-14, charcoal)**: Sepex Viewer logo/wordmark on the left → primary nav (Dashboard, Builder, Jobs) → right side: time-range picker, auto-refresh toggle, theme switcher, user avatar/popover.
- **No persistent sidebar** for the main app — keeps screens wide enough for tables and charts. Use the right side for slide-over detail panels and the top bar for global controls.
- **Command palette** (`⌘K` / `Ctrl+K`) — jump to a job by ID, jump to a process, switch theme, sign out. shadcn/ui has a `Command` primitive built on cmdk.

### Mobile / narrow viewports
- App bar collapses; primary nav becomes a `Sheet` triggered by a hamburger.
- Tables collapse to stacked cards (one card per job).
- Charts shrink to single-column.
- Builder's split editor/preview becomes vertically stacked (editor on top, collapsible preview below).
- Job detail panel becomes a full-screen drawer instead of a side sheet.

Breakpoints: Tailwind defaults — `sm:640`, `md:768`, `lg:1024`, `xl:1280`, `2xl:1536`. Treat `lg` as the primary design target; design `sm` separately.

---

## 6. Decisions on Streamlit-provided affordances

Streamlit's hamburger menu in production gives users: System/Light/Dark, Clear Cache, Print, Record screen, Rerun, Settings, About. We need to make a per-item decision because none of these come for free in a custom Next.js app.

| Streamlit affordance | Decision | Reason |
|---|---|---|
| **System / Light / Dark theme** | **KEEP**. Theme switcher in the user popover, with "System" as default. Persist in localStorage; respect `prefers-color-scheme` for first visit. | Standard expectation. Dark is daily-driver for engineers; light for printing or bright environments. |
| **Clear Cache** | **REPLACE** with a per-page "Refresh" button (and TanStack Query's automatic revalidation). | Streamlit's cache is global and a footgun. With React Query, staleness is per-query and a single button can invalidate the visible queries. Don't expose a raw "nuke everything" button. |
| **Print** | **DROP**. The browser already prints. We won't design specifically for print stylesheets — engineers screenshot. | Effort/value not there. If a real print use case emerges, add a `print:` Tailwind variant pass to one or two pages. |
| **Record screen** | **DROP**. OS-level concern (Loom, QuickTime, OBS). | Not the app's job. |
| **Rerun** | **DROP**. Streamlit-specific concept that doesn't translate. | Replaced by the explicit "Refresh" button + auto-refresh toggle. |
| **Settings (wide mode etc.)** | **DROP**. Layout is fixed by the design. | n/a |
| **About** | **KEEP** as a small "About" item in the user popover. Links to GitHub, version, API status. | Useful for support. |

**New affordances to add that Streamlit doesn't have:**

- **Auto-refresh toggle** in the global header (off / 5s / 15s / 30s / 60s). Off by default. Visible indicator when on (subtle pulse on the refresh icon).
- **Time range picker** in the global header (Last 24h / 7d / 30d / Custom). Persisted per session.
- **Command palette** (`⌘K`). Jump to job by partial ID, jump to process, theme switch, sign out.
- **Toast notifications** (use `sonner`) for job submission, dismissal, errors.
- **Copy-to-clipboard** on every job ID, every code/JSON block.
- **Keyboard shortcuts**: `g d` → dashboard, `g b` → builder, `g j` → jobs, `?` → keyboard help dialog.
- **Real-time job status** for in-flight runs (polling at 2–5s while any visible job is `running` or `accepted`; pause when tab is hidden via `document.visibilitychange`).

---

## 7. Page-by-page brief

### 7.1 Landing page (unauthenticated `/`)

**Goal:** Communicate what Sepex Viewer is in 5 seconds. Push the user to sign in.

**Sections, top to bottom:**

1. **Hero**
   - Headline: short, punchy. e.g. *"Run, monitor, and inspect compute jobs."*
   - Subhead: *"The dashboard for Sepex — Dewberry's OGC-compliant process execution API."*
   - Primary CTA: **Sign in with Keycloak** → starts NextAuth flow.
   - Secondary CTA: **View the API docs** → links to the Swagger UI.
   - Visual: a tasteful, animated mock of a job running — not a literal screenshot, but an evocative composition (status pills cycling, a sparkline ticking, a log line appearing). Avoid stock illustrations.

2. **At-a-glance value props (3 cards)** — each ≤2 lines:
   - **Submit jobs** — Pick a registered process, fill in inputs, execute sync or async.
   - **Watch them run** — Live status, logs streaming, queue depth, resource utilization.
   - **Audit results** — Logs, metadata, results JSON, all addressable by URL.

3. **Live system status** (small, subtle — e.g. a single-line strip)
   - Pulls from `GET /admin/resources` (no auth required if `AUTH_LEVEL ≤ 1`): "API online · 3 jobs running · 47% CPU · v2025.11.0-beta". Green dot if healthy.

4. **Footer**
   - Dewberry · Resilience Solutions wordmark, "Open Source" tag, link to the GitHub repo.

**Tone**: Confident, technical. Not marketing-speak. This is for engineers signing into their own tool, not customers being persuaded.

**Responsive**: Single column on mobile, hero stays prominent.

---

### 7.2 Dashboard (`/`, authenticated)

**Goal:** Answer in one glance: *"Is anything broken? Is the queue moving? What ran recently?"*

**Layout (large screens, top to bottom):**

1. **Header strip (within the page, below global nav)**
   - Left: "Dashboard" h1.
   - Right: time-range picker (default Last 24h), auto-refresh toggle.

2. **KPI row — 5 stat cards in a single grid (`grid-cols-5` on `lg`, `grid-cols-2` on `sm`)**
   - **Total jobs** (in time range) — number + sparkline trend
   - **Successful** — number + % of total (green accent)
   - **Failed** — number + % of total (red accent, clickable → `/jobs?status=failed`)
   - **Running** — number, pulsing dot if > 0
   - **Success rate** — % with delta vs prior period

3. **Charts row — 2 columns on `lg`**
   - **Left (60% width): Stacked bar — jobs over time by status.** X = time bucket (auto-bucketed: 15min / 1h / 12h / 1d depending on range), Y = job count, color = status. Uses brand status colors. Hover tooltip with breakdown. This is the marquee chart and the closest analogue to a Grafana panel.
   - **Right (40% width): Distribution donut — jobs by process.** Top 10 + "other". Click a slice → filters all charts/tables to that process.

4. **Charts row 2 — 2 columns on `lg`**
   - **Left: Submitter bar chart** — top 10 submitters by job count.
   - **Right: Resource utilization gauges** — CPU and memory used vs queued vs free, from `/admin/resources`. Two simple horizontal stacked bars (used / queued / free) with percentage labels. Subtle warning treatment when used > 80%.

5. **Failed jobs alert**
   - If any failed jobs in the time range: red-toned card with count + a compact table of the 5 most recent (jobID short, processID, updated, "View" link to job detail). Collapsed by default if zero failures, otherwise expanded.

6. **Recent activity feed** (optional but nice)
   - Last 20 status transitions, vertically stacked, time-ago format ("3 min ago"). Linear-style.

**States:**
- **Loading**: Skeleton cards (no spinner) — match the final layout's heights so nothing jumps.
- **Empty** (no jobs in range): Friendly empty state with a CTA: "No jobs in the last 24 hours. [Submit your first job →]" linking to `/builder`.
- **Error** (API unreachable): Inline error banner at top with a Retry button. Don't blank out the whole page.

---

### 7.3 Builder (`/builder`)

**Goal:** Author a valid execution payload for a process, preview it, save it locally, and (optionally) submit it.

**Layout (large screens):**

1. **Process picker (top)**
   - A `Combobox` (shadcn) with the list from `GET /processes`. Searchable. Shows `title` + `id` + version. Selecting a process loads its detail.
   - Once selected, show a compact info card: title, version, description, `jobControlOptions` badges (sync / async), declared resource limits.

2. **Two-column main area (`grid-cols-2` on `lg`):**

   **Left column: Inputs editor**
   - **Tabs at top: "Form" | "JSON" | "YAML"**
   - **Form tab (the value-add over Streamlit):** auto-generated form from the process's `inputs` schema. For each input: label (`title`), description (small muted text), and the right control:
     - `dataType: value` → text input (or number if the description hints numeric)
     - `dataType: list` → repeatable group of inputs with add/remove buttons
     - `valueDefinition.possibleValues` present → `Select`
     - `dataType: boolean` → `Switch`
     - `dataType: object` → embedded JSON editor for that subtree
     - Honor `minOccurs` (required if ≥1) and `maxOccurs` (cap repeats).
   - **JSON tab:** Monaco editor (or CodeMirror 6 — lighter) with JSON schema validation. Highlights errors inline.
   - **YAML tab:** same editor, YAML mode. Two-way sync with JSON (round-trip).
   - Below the editor: **Validation status** ("Valid · 3 inputs filled" or "Invalid: missing required field `inputs[0].name`"). Inline, calm. No red-screen-of-death.

   **Right column: Preview + actions**
   - **Preview** of the final payload as JSON (collapsed sections for nested objects). Read-only, monospace.
   - **Submitter field** — text input (default to signed-in user's email).
   - **Tags field** — `MultiSelect` / chip input — optional tags to attach to the job (filterable later).
   - **Execution mode** — only shown if process supports both sync + async. Radio: `Async (recommended)` | `Sync`.
   - **Action buttons** (sticky at bottom of right column):
     - **Submit** (primary, Dewberry magenta) — disabled if invalid.
     - **Save payload** (dropdown: as JSON / as YAML) — downloads to local disk.
     - **Load payload** (file picker, accepts JSON/YAML) — populates the editor.

3. **Submission feedback**
   - On submit, show a `Sheet` slide-over from the right with the API response. If sync: results JSON. If async: jobID with two CTAs — "View job →" (to `/jobs/[jobID]`) and "Submit another" (clears form).
   - Toast on success/failure.

**States:**
- **No process selected** → editor is disabled with a muted message: "Choose a process to start building a payload."
- **Process has no declared inputs** → editor shows "This process accepts no inputs. Press Submit to run it as-is."
- **Submission in flight** → button shows spinner + "Submitting…", disable form.
- **Submission failed** → toast + inline error in the slide-over with the full response body (formatted JSON, copyable).

---

### 7.4 Jobs (`/jobs`)

**Goal:** Find a job. Inspect it. Manage it (dismiss).

**Layout (large screens):**

```
┌─────────────────────────────────────────────────────────────┐
│ Filters bar (sticky)                                        │
├─────────────────────────────┬───────────────────────────────┤
│ Jobs table (60% width)      │ Job detail panel (40% width)  │
│                             │                               │
│ [paginated list of jobs]    │ [tabs: Logs/Results/Metadata] │
│                             │                               │
└─────────────────────────────┴───────────────────────────────┘
```

1. **Filters bar (sticky top of page)**
   - **Search** input — searches by full or partial jobID, by submitter email.
   - **Process** combobox — single select from `/processes`, plus "All".
   - **Status** multi-select — all six states, default all.
   - **Submitter** combobox — populated from current job set.
   - **Time range** — inherits global picker; can be overridden here.
   - **Tags** — chip input (filters by `?tags=` API param).
   - **Clear filters** link, right-aligned.
   - Filters serialize to URL query params (`?status=failed,running&processID=hms-runner&...`) for shareable links.

2. **Jobs table (left column, 60% width)**
   - Columns: **Status pill** | **Job ID** (last 8 chars, monospace, click to copy) | **Process** | **Submitter** | **Updated** (relative time + UTC tooltip) | **Tags** (chips, max 2 visible + "+N").
   - Selecting a row highlights it and loads details in the right panel (no navigation).
   - Row also has a deep-link icon → opens `/jobs/[jobID]` in a new tab.
   - **Pagination** at bottom: "Showing 1–20 of 247" + Prev / Next + page-size selector (20 / 50 / 100). Server-side via `limit` + `offset`.
   - **Bulk actions** (when rows are checked): Dismiss selected (with confirmation dialog). Only available for non-terminal statuses.
   - **Empty state**: clear message ("No jobs match your filters") + "Clear filters" button.

3. **Job detail panel (right column, 40% width)**
   - Header: **Status pill**, full job ID (copyable), process title, submitter, "Dismiss" button (only enabled for `accepted` / `running`).
   - Tabs: **Logs** | **Results** | **Metadata** | **Inputs** (echo of what was submitted, useful for "what did I run?")
   - **Logs tab** — Github-Actions style:
     - Toggle: "Process logs" / "Server logs" (sub-tabs).
     - Each log line: monospace, level pill (INFO/WARN/ERROR colored), timestamp, message.
     - Search within logs (highlight matches).
     - "Tail" toggle — auto-scroll to bottom on new lines (when polling and job is running).
     - Download as text.
   - **Results tab** — if results are JSON, render as collapsible JSON tree (e.g., `react-json-view-lite`). If results have file references (transmission=reference), render as a list of file links with download buttons. Empty state: "Job has no results yet" or "Job did not produce results".
   - **Metadata tab** — pretty-printed JSON with key-value sections for the well-known fields (version, runtime, inputArguments, recoveryNotice).
   - **Inputs tab** — the original payload, JSON tree.

**Mobile / narrow:** Filters collapse into a single "Filters" button → opens a `Sheet`. Table becomes stacked cards. Selecting a card opens the detail in a full-screen drawer.

**States:**
- Loading rows → skeleton rows.
- Polling refresh: visual tick on the refresh icon, no spinner over the whole table.
- Selected job is in-flight (`running`/`accepted`): tail logs poll every 2s; status pill polls.
- Detail panel empty (no job selected) → friendly message: "Select a job to view its details" with an arrow pointing to the table.

---

### 7.5 Job detail (`/jobs/[jobID]`)

Same content as the right-side detail panel from `/jobs`, but as a full page (deep-link target). Top breadcrumb: `Jobs › {jobID-short}`. Add a "Back to jobs" link.

This route can be a server component that fetches the job by ID, then hydrates the tabs as client components for log polling.

---

### 7.6 Auxiliary surfaces

- **Toaster** (`sonner`, top-right) — global, used for submission/dismissal/error feedback.
- **Confirmation dialogs** — required for: dismiss single job, dismiss bulk jobs, sign out (no — too noisy), delete process (admin only, future).
- **Keyboard help** (`?` key) — `Sheet` listing all shortcuts.
- **Command palette** (`⌘K`) — see §6.

---

## 8. Component inventory

The designer should produce mocks for these reusable pieces (in addition to whole-page comps):

- `StatusPill` — six variants matching the status enum.
- `JobIdBadge` — short UUID + copy-on-click + tooltip with full ID.
- `KpiCard` — number, label, optional sparkline, optional delta vs prior period.
- `StackedBarChart` — for jobs-over-time by status. (Use Recharts or Tremor; Recharts matches reality-view closer.)
- `DonutChart` — for process distribution.
- `ResourceGauge` — horizontal stacked bar for CPU/memory.
- `LogViewer` — virtualized list of log lines, search, level filter, tail mode, download.
- `JsonTree` — collapsible JSON tree with copy-on-node.
- `CodeEditor` — Monaco or CodeMirror, JSON/YAML modes, validation gutter.
- `ProcessPicker` — Combobox with description preview.
- `JobsTable` — sortable, paginated, selectable rows.
- `FiltersBar` — chip-style applied filters with x-to-remove.
- `TimeRangePicker` — preset + custom date range.
- `AutoRefreshToggle` — off / 5s / 15s / 30s / 60s.
- `EmptyState` — icon + headline + body + CTA. Reuse pattern site-wide.
- `ErrorState` — same pattern but for "something went wrong, here's why".
- `LoadingSkeleton` — per major layout (KPI row, chart, table).
- `CommandMenu` — wraps shadcn `Command` + cmdk; global ⌘K.
- `ThemeSwitcher` — dropdown in user popover (System/Light/Dark).
- `UserPopover` — avatar → email, role, sign out, theme switcher, About.

---

## 9. States to design (always ask "what does this look like when…")

For every page, produce mocks for at least:
- **Loading** (first paint) — skeletons, never spinners on the whole page.
- **Loading** (refresh in place) — subtle pulse on a refresh icon, content stays visible.
- **Empty** (zero data, but no error) — friendly explanation + next action.
- **Error** (API down, 500, network) — actionable, with Retry.
- **Unauthorized** (401/403) — explanation + sign-in CTA.
- **Stale** (auto-refresh paused because tab is hidden) — small indicator: "Paused (tab inactive)".
- **Optimistic** (action submitted, awaiting confirmation) — e.g. job submitted, sliding into the table greyed-out until the next poll confirms.
- **Mobile** version of the layout.

---

## 10. Accessibility

- Keyboard navigable end-to-end. All interactive elements reachable by Tab; visible focus rings (CTA teal `#336B87`).
- Status pills must not rely on color alone — pair with text label.
- Charts must have a sensible alt-text summary and a "View as table" toggle.
- Color contrast: AA minimum, AAA for body text where possible.
- Respect `prefers-reduced-motion` for the running-pulse and auto-refresh animations.

---

## 11. Out of scope (for v1)

So the designer doesn't try to mock these:

- Process *creation* in the UI (admin uploads YAML on the server today; future enhancement).
- Plugin marketplace / browse.
- Multi-tenant / org switching.
- Notifications (email, Slack) — future.
- A "workflow" abstraction across multiple jobs — future, even though the underlying API supports tags that could underpin this.
- File upload during payload building (not supported by API yet).

---

## 12. Reference materials to feed alongside this brief

When prompting Claude Design / v0, also pass:

1. **This document** in full.
2. The **API endpoint table from §11** of the API survey (for context on what data renders where).
3. **Screenshots of Grafana** (a job-list view + a panel view).
4. The **reality-view Header + global styles** (`/Users/curtis/Documents/code/reality-view/client/src/app/_components/Header.jsx` and `src/app/styles/globals.css`) — so the visual identity matches.
5. The **example process YAML** at `examples/register-processes/pyecho.yaml` plus the more complex `cc/hms-runner/hms-runner.yaml` (off the `rnd/cc` branch) — so the designer understands how varied real process schemas can get.

---

## 13. Deliverables (what to ask the designer for)

In priority order:

1. **Dashboard (`/`) — full mock**, light + dark, desktop + mobile.
2. **Jobs (`/jobs`) — full mock**, with detail panel populated, both desktop and mobile (full-screen drawer variant).
3. **Job detail (`/jobs/[jobID]`)** — Logs tab populated with realistic log lines, Results tab with a sample JSON.
4. **Builder (`/builder`)** — Form tab + JSON tab variants, with submission slide-over.
5. **Landing page** — desktop + mobile.
6. **Component snapshots** — StatusPill (all variants), KpiCard, LogViewer, FiltersBar.
7. **Empty / error / loading states** for the dashboard and jobs page.
8. **Color tokens + type scale** as a one-pager (for the implementer to drop into Tailwind config and `globals.css`).

Optional extras:
- A short Loom walking through the navigation flow.
- A click-through prototype.

---

## Appendix A — Current Streamlit surface (what we're replacing)

**Branches surveyed:**
- `main` / `docs/expand-readme` — single page (`app/home.py`): KPIs, stacked bar, donut, submitter bar, three-column layout (Processes / Jobs table / Details).
- `twodimfim` — adds `pages/builder.py` (process picker + JSON editor + JSON/YAML preview + downloads) and `pages/jobs.py` (file upload → submit).
- `rnd/cc` — most evolved: `home.py` (dashboard with time-range filters, search, auto-refresh, export to CSV, KPI row, failed-jobs alert), `pages/builder.py`, `pages/manager.py` (Submit / Dismiss / Dismiss All tabs with confirmations), `pages/monitor.py` (paginated jobs table + details panel with log tailing), `pages/data_files.py` (file browser with pagination + JSON preview).

The new design should consolidate these into the three target routes (`/`, `/builder`, `/jobs`) and absorb the most useful innovations from each branch:
- From `home.py` on `rnd/cc`: time-range filter, auto-refresh, failed-jobs alert, KPI row, export.
- From `monitor.py`: pagination, tail logs, deselect-on-filter-change.
- From `manager.py`: dismiss flow with confirmation, bulk dismiss with progress.
- From `builder.py`: process picker, JSON ↔ YAML preview, download.
- From `data_files.py`: file browser (relocate to a tab or future `/files` route).

## Appendix B — API endpoints the UI consumes

Brief recap (full detail lives in the API survey output kept with this brief):

| Endpoint | Used by |
|---|---|
| `GET /processes` | Dashboard (process map), Builder (picker), Jobs (filter) |
| `GET /processes/{id}` | Builder (input schema), Jobs detail (process metadata) |
| `POST /processes/{id}/execution` | Builder (submit), with optional `Prefer: respond-async` and `X-SEPEX-User-Email` header |
| `GET /jobs?limit&offset&processID&status&submitter&tags` | Dashboard (chart/KPI data), Jobs (table) |
| `GET /jobs/{id}` | Jobs detail (status), polled while running |
| `GET /jobs/{id}/logs` | Jobs detail → Logs tab, polled while running |
| `GET /jobs/{id}/results` | Jobs detail → Results tab |
| `GET /jobs/{id}/metadata` | Jobs detail → Metadata tab |
| `DELETE /jobs/{id}` | Jobs detail (Dismiss action), Bulk dismiss |
| `GET /admin/resources` | Dashboard (resource gauges), Landing (system status strip) |

Auth: Keycloak JWT in `Authorization: Bearer …`. `X-SEPEX-User-Email` header on submission. Roles: `admin`, `service_account`, plus per-process roles. `AUTH_LEVEL` 0/1/2 controls scope.

Job statuses (locked): `accepted`, `running`, `successful`, `failed`, `dismissed`, `lost`.
