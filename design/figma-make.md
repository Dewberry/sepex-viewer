# Sepex Viewer — Figma Make Workflow

Starter prompts and design tokens for generating UI mocks in Figma Make.

The full design brief lives at [`../DESIGN_BRIEF.md`](../DESIGN_BRIEF.md). This file extracts what Figma Make needs to know in its (short) context window, plus per-screen prompts you can paste in one at a time.

**Working file (CSI-General team):** https://www.figma.com/design/HldgOxyL3NwoENXsGg3Tkf
File key: `HldgOxyL3NwoENXsGg3Tkf` — share this back with me along with a frame's node ID and I can read any frame via the Figma MCP.

---

## How to use this

1. Open Figma Make (or Figma's AI panel) in a new file.
2. **Paste §1 (System context) once at the start of the chat.** This anchors the visual identity.
3. **Paste one screen prompt at a time** (§3.1–§3.5). Iterate on that screen with follow-ups before moving on.
4. **Generate desktop first** (1440 × 900). Once you like it, ask Figma Make to "produce a mobile variant at 390 × 844".
5. **Generate dark mode first** (it's our default). Then "produce a light-mode variant".
6. When a screen is good enough, share the Figma URL back to me in chat. I have the official Figma MCP loaded, so I'll read the file directly via `get_design_context` (returns screenshot + reference React/Tailwind code + any tokens you define), then translate to the repo's actual conventions (shadcn components, Dewberry tokens already in `reality-view`, route structure).

---

## 1. System context (paste at the top of every Figma Make chat)

```
We're designing Sepex Viewer — an internal Dewberry web app for engineers
who run compute jobs (geospatial / hydrology simulations) on an OGC API –
Processes server. Think "Grafana for batch jobs": a dashboard for
submitting jobs, watching them run, and inspecting their logs and results.

Audience: water-resources engineers and data scientists. Technically
literate, will use this 4–8 hours a day. Density matters more than
spaciousness. This is a daily-driver internal tool, not a marketing site.

Visual references (in priority order):
  1. Grafana — dense panels, time-range pickers, status pills, sparklines
  2. Vercel Dashboard — clean cards, deployment-list patterns, side drawers
  3. Linear — keyboard-first feel, command palette, tasteful empty states
  4. GitHub Actions runs view — log viewer with collapsible groups

Avoid: pastel gradients, glassmorphism, generic AI-dashboard aesthetic,
floating-dot illustrations, oversized hero icons, consumer-app whitespace.

Brand (Dewberry):
  Primary (deep magenta):    #7A1341  — primary CTAs, brand accents only
  CTA / focus ring (teal):   #336B87  — links, focus rings, secondary CTAs
  Charcoal (dark surface):   #2C2924  — app shell, header bg in dark mode
  Error red:                 #8B2E40

Type: Open Sans (UI) + JetBrains Mono or Geist Mono (logs, JSON, IDs).

Status pill colors (locked — these come from the API enum):
  successful  → green   (#22c55e)  solid pill, white text
  running     → amber   (#eab308)  solid pill + pulsing dot
  accepted    → sky     (#0ea5e9)  solid pill
  failed      → red     (#ef4444)  solid pill
  dismissed   → slate   (#64748b)  outline pill
  lost        → purple  (#a855f7)  outline pill + warning icon

Stack the design will be implemented in: Next.js 16 App Router, React 19,
Tailwind v4, shadcn/ui (New York style). So: prefer flat, rectangular
cards with subtle borders; rounded corners default to 0.5rem (8px);
shadows minimal. Use Radix-style components (Dialog, Sheet, Popover,
Tabs, Combobox, Command palette).

Default theme: dark mode. Light mode is fully supported.
Target viewport: 1440×900 desktop primary, 390×844 mobile responsive.
```

---

## 2. Quick token reference (paste alongside the system context if Figma Make needs more specifics)

### Colors

| Role | Dark mode | Light mode |
|---|---|---|
| App background | `#1C1917` (stone-900) | `#FAFAF9` (stone-50) |
| Card / surface | `#2C2924` (charcoal) | `#FFFFFF` |
| Card border | `#3F3B36` | `#E7E5E4` (stone-200) |
| Primary text | `#FAFAF9` (stone-50) | `#1C1917` (stone-900) |
| Muted text | `#A8A29E` (stone-400) | `#78716C` (stone-500) |
| Brand primary (CTA) | `#7A1341` (Dewberry) | `#7A1341` |
| Accent / focus ring | `#336B87` (CTA teal) | `#336B87` |
| Error / destructive | `#EF4444` | `#8B2E40` (brand error) |
| Success | `#22C55E` | `#16A34A` |
| Warning | `#EAB308` | `#CA8A04` |

### Spacing scale

Tailwind defaults: 4px increments. Common values: `gap-2` (8px), `gap-4` (16px), `gap-6` (24px), `gap-8` (32px). Page padding: `p-6` desktop, `p-4` mobile.

### Type scale

| Token | Size / line-height | Use |
|---|---|---|
| `text-xs` | 12 / 16 | Captions, table cells |
| `text-sm` | 14 / 20 | Body, secondary nav, form labels |
| `text-base` | 16 / 24 | Primary body |
| `text-lg` | 18 / 28 | Card titles |
| `text-xl` | 20 / 28 | Section headings |
| `text-2xl` | 24 / 32 | Page H1 |
| `text-3xl` | 30 / 36 | Landing hero subhead |
| `text-5xl` | 48 / 56 | Landing hero headline |

Font weights: 400 (body), 500 (emphasis, table headers), 600 (headings, buttons), 700 (hero).

### Radius + borders

- Default radius: `0.5rem` (8px)
- Pills / badges: `9999px`
- Cards: `0.5rem`, 1px border
- Buttons: `0.5rem`

### Iconography

Lucide icons (matches shadcn). Common set: `Activity`, `BarChart3`, `Box`, `CheckCircle2`, `ChevronRight`, `CircleAlert`, `Copy`, `Download`, `Filter`, `Search`, `Settings`, `Terminal`, `XCircle`, `Loader2`, `RefreshCw`, `User`, `LogOut`, `Sun`, `Moon`, `Monitor`, `Command`, `Clock`, `PlayCircle`, `Pause`, `Trash2`.

---

## 3. Per-screen prompts

Paste these one at a time, after the system context. Iterate before moving on.

### 3.1 Landing page (`/` unauthenticated)

```
Design the landing page for Sepex Viewer. Single page, scrollable, dark mode.

Sections (top to bottom):

1. Top bar — Sepex Viewer wordmark on the left, a "Sign in" button (teal CTA)
   on the right. h-14, charcoal background, subtle bottom border.

2. Hero — full-width, centered, ~80vh:
   - Eyebrow text: "Internal · Dewberry"
   - Headline (text-5xl, weight 700): "Run, monitor, and inspect compute jobs."
   - Subhead (text-xl, muted): "The dashboard for Sepex — Dewberry's
     OGC-compliant process execution API."
   - Two buttons side-by-side: "Sign in with Keycloak" (primary, Dewberry
     magenta) and "View API docs" (ghost outline)
   - To the right of the text block on desktop, an evocative composition —
     NOT a literal product screenshot, NOT stock illustration. Suggested:
     three small floating cards showing (a) a status pill animating
     accepted→running→successful, (b) a sparkline ticking up, (c) a code-
     style log line appearing. Minimal, geometric, Linear-style.

3. Three value-prop cards (grid, equal columns on lg, stacked on sm):
   - "Submit jobs" — Pick a registered process, fill in inputs, execute
     sync or async. Icon: PlayCircle.
   - "Watch them run" — Live status, streaming logs, queue depth, resource
     utilization. Icon: Activity.
   - "Audit results" — Logs, metadata, results JSON, all addressable by URL.
     Icon: Terminal.
   Cards should be flat charcoal with 1px border, subtle hover lift.

4. Live system status strip — a single line of text-sm, centered, with a
   small green dot: "API online · 3 jobs running · 47% CPU · v2025.11.0-beta".
   Above the footer.

5. Footer — Dewberry wordmark, "Internal use only" notice, link to GitHub.
   text-xs muted.

Tone: confident and technical, not marketing-speak. No exclamation marks.
Mobile: stack everything to a single column; hero illustration moves below
the text or shrinks dramatically.
```

### 3.2 Dashboard (`/`, authenticated)

```
Design the authenticated Dashboard page for Sepex Viewer. Dark mode default.
Reference: Grafana home dashboard layout density.

Global app bar (h-14, charcoal, persistent across all authenticated routes):
- Left: Sepex Viewer wordmark
- Center-left: nav links — Dashboard (active), Builder, Jobs
- Right cluster: Time-range picker dropdown ("Last 24h"), Auto-refresh
  toggle ("Off" / "5s" / "15s" / "30s" / "60s"), Refresh icon button,
  Theme switcher (Sun/Moon icon), User avatar circle (opens popover)

Page content (within main viewport, p-6):

1. Page header — "Dashboard" h1 on left, small "Last updated 12s ago"
   timestamp on right.

2. KPI row — 5 stat cards in a single grid (grid-cols-5 on lg, grid-cols-2
   on sm). Each card: muted label on top, large number (text-3xl), small
   sparkline or delta indicator below.
   - Total jobs (no accent)
   - Successful (green accent on the number)
   - Failed (red accent — entire card is clickable, links to /jobs?status=failed)
   - Running (amber accent + small pulsing dot if > 0)
   - Success rate ("96.2%" with a green up-arrow "+2.1pp vs prior period")

3. Charts row 1 — two columns, 60/40 split:
   - LEFT (60%): Stacked bar chart "Jobs over time by status".
     X-axis: time buckets. Y-axis: job count. Color = status (use the locked
     status colors). Hover tooltip showing breakdown by status. Bucket
     auto-adjusts to the time range. Legend below the chart.
   - RIGHT (40%): Donut chart "Jobs by process". Top 10 processes plus
     "other". Center of donut shows total count. Legend on the side.

4. Charts row 2 — two columns, 50/50:
   - LEFT: Horizontal bar chart "Top submitters". 10 bars max, sorted desc.
   - RIGHT: "Resource utilization" — two horizontal stacked bars
     (CPU, Memory), each showing used / queued / free segments with
     percentages. Used color: amber if >80%, green otherwise.

5. "Failed jobs" alert card — only visible if count > 0. Red-toned card
   (subtle red left border, dark background): heading "3 failed jobs in
   the last 24 hours" + a compact 5-row table (job ID short, process,
   updated relative time, "View" link). "View all failures →" link on the
   right.

6. Activity feed — vertical list, last 20 status transitions, Linear-style:
   small status pill + "{processID} #{jobIDshort}" + relative time
   ("3 min ago"). text-sm. Subtle dividers between rows.

Density: aim for 1.5 screens of scroll content. Don't space everything out.
Cards have minimal padding (p-4), tight gaps (gap-4 or gap-6).

Mobile (390): KPI cards become 2-column grid, charts stack to single column
at full width, "failed jobs" alert collapses by default.
```

### 3.3 Builder (`/builder`)

```
Design the Builder page — where users author and submit a job payload.

Global app bar same as Dashboard. "Builder" is the active nav link.

Page content (p-6):

1. Page header — "Builder" h1 on left, "Recent payloads" dropdown on right
   (lists locally-saved payloads, future feature, can be a disabled pill
   for now).

2. Process picker section — full-width Combobox/select at the top:
   "Select a process…" placeholder. Once a process is picked, expand into
   a compact info card showing:
   - Process title (text-lg, weight 600), version pill ("v8.15.2023"),
     description (text-sm muted, max 2 lines)
   - Right-aligned: jobControlOptions badges ("sync-execute", "async-
     execute") + resource hint ("0.5 CPU · 1024 MB")

3. Two-column main editor area (grid-cols-2 on lg, stacked on md):

   LEFT COLUMN — Inputs editor:
   - Tabs at top: "Form" | "JSON" | "YAML" (default to Form)
   - Form tab: auto-generated form from the process's input schema. For
     each input show: label (the input title), description (small muted
     text), and an appropriate control (text input / number / select /
     switch / repeatable group with add/remove). Required inputs marked
     with a small red asterisk. Inputs in a single column, generous
     vertical rhythm.
   - JSON tab: code editor look — monospace, line numbers, dark editor
     surface even in light mode. Syntax-highlighted JSON. Inline
     validation errors with a small red gutter mark.
   - YAML tab: same editor style, YAML syntax highlight.
   - Below the editor, a thin status row: green check + "Valid" or red x
     + "Invalid: missing required field 'inputs[0].name'".

   RIGHT COLUMN — Preview + actions:
   - "Preview" label + a read-only monospace JSON tree showing the final
     payload (indented, collapsible nodes).
   - Below the preview, a form area:
     - Submitter (text input, prefilled with user's email)
     - Tags (chip input — type a tag, press enter, chip appears)
     - Execution mode (radio group: "Async (recommended)" | "Sync") —
       only visible if the process supports both
   - Sticky action bar at the bottom of the right column:
     - Primary "Submit" button (Dewberry magenta, full-width or large)
     - Secondary "Save payload" dropdown (split-button: Save as JSON /
       Save as YAML)
     - Tertiary "Load payload" file-picker link

4. After submission — show a Sheet sliding in from the right with the
   API response. Header: "Job submitted". Show jobID (monospace, copyable),
   status pill, two CTAs: "View job →" and "Submit another". Show the
   raw response JSON below.

Mobile: tabs and stacked sections. The two-column editor stacks (Inputs
on top, Preview + actions below).
```

### 3.4 Jobs (`/jobs`)

```
Design the Jobs page — table + side detail panel.

Global app bar same as Dashboard. "Jobs" is the active nav link.

Page content (p-6):

1. Page header — "Jobs" h1 on left, "Showing 1–20 of 247" small text on right.

2. Filters bar — sticky, full-width row of controls. From left to right:
   - Search input with magnifying glass icon ("Search by job ID or submitter…")
   - "Process" combobox (single select, default "All processes")
   - "Status" multi-select (checkboxes — accepted / running / successful /
     failed / dismissed / lost — default all)
   - "Submitter" combobox
   - "Time range" dropdown ("Last 24h", inheriting global picker)
   - "Tags" chip input
   - On the right: applied-filters chip strip (each chip has an x to
     remove), "Clear all" link
   Filters serialize to URL query params for shareable links.

3. Two-column main area (grid-cols-[1.5fr_1fr] on lg):

   LEFT — Jobs table (60% width):
   - Sticky header row, stone-700 background.
   - Columns: Status (pill), Job ID (last 8 chars in mono, copy icon on
     hover), Process (truncate with tooltip), Submitter (email truncated),
     Updated (relative time + UTC tooltip), Tags (max 2 chips visible +
     "+N").
   - Selectable rows (single-select highlights with subtle teal left
     border). Checkbox column for bulk-select.
   - Compact density (h-10 rows). Zebra striping or just bottom borders —
     designer's call, lean toward bottom borders only.
   - Bulk action bar appears at top of table when rows are checked:
     "3 selected · Dismiss selected" button (only enabled for non-terminal
     statuses).
   - Pagination footer: "Showing 1–20 of 247" + Prev/Next buttons +
     page-size selector (20 / 50 / 100).

   RIGHT — Job detail panel (40% width, sticky to the right):
   - Header: status pill, full job ID (monospace, copy icon), process
     title (text-lg), submitter (text-sm muted). "Dismiss job" button
     top-right (only enabled for accepted/running, with confirmation
     dialog).
   - Tabs: "Logs" | "Results" | "Metadata" | "Inputs"
   - Logs tab: sub-toggle "Process logs / Server logs". Each log line:
     small level pill (INFO/WARN/ERROR colored) + monospace timestamp +
     monospace message. Search within logs (input above the log list,
     highlighted matches). "Tail" toggle (auto-scrolls to bottom).
     "Download" icon button.
   - Results tab: collapsible JSON tree, monospace. Empty state if no
     results yet.
   - Metadata tab: pretty key-value cards for known fields (version,
     runtime, inputArguments, recoveryNotice).
   - Inputs tab: original payload as JSON tree.

4. Empty states:
   - No jobs match filters: centered message + "Clear filters" button
   - No job selected (right panel): centered icon + "Select a job to view
     details" + arrow pointing left

Mobile: filters collapse into a single "Filters" button → opens a Sheet.
Table becomes stacked cards (one per job, showing status pill, ID short,
process, updated, tags). Tapping a card opens the detail in a full-screen
drawer instead of the side panel.
```

### 3.5 Job detail (`/jobs/[jobID]`)

```
Design the dedicated Job Detail page (deep-link target — same content as
the right-panel from /jobs but laid out as a full page).

Global app bar same as Dashboard.

Page content (p-6):

1. Breadcrumb — "Jobs › a1b2c3d4" (clickable Jobs link, monospace ID short).

2. Page header — full-width row:
   - Left: Status pill, full job ID (monospace, large text, copy icon),
     process title, submitter.
   - Right: "Dismiss job" button (destructive style, only enabled for
     accepted/running) and "← Back to jobs" link.
   Below the header, a thin metadata row: "Submitted 2026-04-29 14:23 UTC ·
   Started 14:23 · {ended/running for 3m 12s}".

3. Tabs — "Logs" | "Results" | "Metadata" | "Inputs" — same content as
   the panel version but full-width (much more horizontal space, so the
   log viewer should use it: wider lines, no truncation).

4. Logs tab as the default view:
   - Toolbar: sub-tab toggle "Process logs / Server logs", search input,
     level filter (multi-select pills), "Tail" toggle, "Download" button.
   - Virtualized log list: small line-number gutter, level pill, timestamp,
     message. Group consecutive same-level lines with subtle visual
     grouping (Github Actions style).
   - At the bottom, a subtle status line: "Showing 1,247 lines · Polling
     every 2s · Tail on".

Mobile: breadcrumb stays, header stacks vertically, tabs scroll horizontally
if they overflow.
```

---

## 4. Iteration tips for Figma Make

- **Refine, don't restart.** "Tighten the KPI row spacing — currently too airy" beats "redo the dashboard".
- **Be specific about densities.** "Reduce row height to 36px" or "remove every other border".
- **Reference the brand explicitly when it drifts.** "The accent color should be #336B87 teal, not a generic blue."
- **Ask for variants explicitly.** "Now produce the same screen in light mode" / "Now produce a 390px-wide mobile variant".
- **Save versions.** Duplicate the frame before each major iteration so you can roll back.

## 5. Sharing back to Claude Code

Once a screen is in good shape:

1. In Figma, **right-click the frame → "Copy link to selection"**. This gives me the file key + the exact node ID.
2. Paste the link in chat with a note like "here's the dashboard mock, please scaffold it".
3. I'll call `mcp__claude_ai_Figma__get_design_context` on it, which returns the screenshot + reference React/Tailwind code + any design tokens you defined as Figma variables. I translate that to the repo's actual stack (shadcn components from `~/Documents/code/reality-view/client/src/components/ui/`, Dewberry tokens from reality-view's `globals.css`, our route structure).

If you set up Figma Variables for the brand colors, I'll pick them up automatically via `get_variable_defs` and map them to Tailwind tokens.

## 6. What NOT to design (out of scope for v1)

So Figma Make doesn't waste effort:
- Process creation in the UI (admin uploads YAML on the server)
- Plugin marketplace
- Multi-tenant / org switching
- Email or Slack notification UI
- File upload during payload building
- A `/processes` catalog page (deferred — process picker in Builder is enough for v1)
