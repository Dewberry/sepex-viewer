# Sepex Viewer — Implementation Handoff

**Status:** Design phase complete. Ready to scaffold the Next.js client.
**Branch:** `design` (created from `main`, has DESIGN_BRIEF + design/ committed).
**Read first:**
1. [`../DESIGN_BRIEF.md`](../DESIGN_BRIEF.md) — full design spec (IA, routes, components, states, brand)
2. [`./figma-make.md`](./figma-make.md) — designer's Figma Make playbook (informational; doesn't block dev)
3. This file — what to build, in what order

---

## Quick orientation

- **Repo:** `/Users/curtis/Documents/code/sepex-viewer`
- **Goal:** Replace the Streamlit prototype (`app/*.py`) with a Next.js + shadcn web app.
- **Reference repo:** `/Users/curtis/Documents/code/reality-view/client` — same firm, same stack, mirror its conventions.
- **Backend it talks to:** Sepex API (Go OGC API – Processes server) at `http://localhost:5050` in dev. Started by the existing `docker-compose.yml`.

## Current state

- ✅ `DESIGN_BRIEF.md` — full design brief (committed on `design`)
- ✅ `design/figma-make.md` — Figma Make starter prompts (committed)
- ✅ `design/scaffold-plan.md` — this file
- ⏳ `client/` — **not yet created** (this is the next context's job)
- 🟡 Figma file: https://www.figma.com/design/ukqqMd0SruRdcPMAQUr90W (CSI-General → "Sepex Viewer" project, file_key `ukqqMd0SruRdcPMAQUr90W`)
  - Contains a renamed default page and an empty landing page from MCP debugging — needs cleanup or fresh start. Not a blocker.

## Stack (locked — match reality-view exactly except where noted)

- **Next.js 16** App Router
- **React 19**
- **JavaScript** (no TypeScript) — match reality-view
- **Tailwind CSS v4** with `@theme inline` block
- **shadcn/ui** New York style, RSC-enabled
- **Lucide React** icons
- **next-auth v5-beta** + **Keycloak** provider, with `NEXT_PUBLIC_DEV_BYPASS_AUTH=true` escape hatch
- **Open Sans** for UI; **JetBrains Mono** for logs/IDs/JSON
- **TanStack Query v5** — *the one allowed deviation from reality-view*; needed for poll-heavy job-status flows
- **react-hook-form + zod** for the Builder form
- **sonner** for toasts

## Routes (final spec — see DESIGN_BRIEF §5)

| Route | Layout | Purpose |
|---|---|---|
| `/` | root | Landing (unauthenticated) |
| `/(dashboard)/dashboard` | dashboard | Overview (KPIs, charts, alerts) |
| `/(dashboard)/builder` | dashboard | Compose + submit a job payload |
| `/(dashboard)/jobs` | dashboard | Jobs table + side detail panel |
| `/(dashboard)/jobs/[jobID]` | dashboard | Job detail (deep-link) |

Two layout groups, mirroring reality-view: root layout for unauth/landing, `(dashboard)/layout.jsx` wraps everything authed (mounts global app bar, providers, session).

---

## Step-by-step

### 1. Scaffold Next.js into `client/`

```bash
cd /Users/curtis/Documents/code/sepex-viewer
npx create-next-app@latest client \
  --js --tailwind --app --src-dir \
  --import-alias "@/*" --use-npm --eslint --no-turbopack
```

After it finishes, verify `client/package.json` shows Next 16 + React 19. If create-next-app gave an older Next, manually bump:

```bash
cd client
npm install next@latest react@latest react-dom@latest
```

### 2. Install additional dependencies

```bash
cd client
npm install \
  @tanstack/react-query@latest \
  next-auth@beta \
  lucide-react \
  react-hook-form zod @hookform/resolvers \
  sonner \
  recharts \
  date-fns
```

### 3. Init shadcn/ui

```bash
cd client
npx shadcn@latest init
# Choose: Default style? → New York
# Base color? → Stone (we override colors anyway)
# CSS variables? → Yes
```

Then add the baseline component set:

```bash
npx shadcn@latest add button card dialog sheet tabs command table select tooltip popover dropdown-menu sonner skeleton separator badge input textarea checkbox switch combobox scroll-area
```

(Some of those — `combobox` — may need to be manually composed. Check shadcn docs.)

### 4. Port Dewberry design system

Copy these files from reality-view as starting points, then strip anything Reality-View-specific:

- `~/Documents/code/reality-view/client/src/app/styles/globals.css` → `client/src/app/globals.css`
  - Keep brand colors, Tailwind v4 theme block, dark-mode variables.
  - Brand palette to preserve: primary `#7A1341` (Dewberry magenta), accent `#336B87` (CTA teal), `#2C2924` (charcoal), error `#8B2E40`.
  - Add status pill colors as CSS variables: `--status-successful: #22c55e`, `--status-running: #eab308`, `--status-accepted: #0ea5e9`, `--status-failed: #ef4444`, `--status-dismissed: #64748b`, `--status-lost: #a855f7`.
- `~/Documents/code/reality-view/client/src/app/styles/fonts.js` → `client/src/app/fonts.js`
  - Open Sans (300/400/500/700) + JetBrains Mono (400/500).
  - Wire both into `client/src/app/layout.jsx`.

### 5. Stub the routes

Create these files. Each renders a centered placeholder so the structure is navigable:

```
client/src/app/
  layout.jsx                             # root layout, mounts <Providers>, fonts, global Toaster
  page.jsx                               # Landing page (unauth)
  providers.jsx                          # <QueryClientProvider> + <SessionProvider> + theme
  (dashboard)/
    layout.jsx                           # global app bar (Dashboard / Builder / Jobs nav, time range, refresh, theme switcher, user popover)
    dashboard/
      page.jsx                           # Dashboard placeholder
    builder/
      page.jsx                           # Builder placeholder
    jobs/
      page.jsx                           # Jobs placeholder
      [jobID]/
        page.jsx                         # Job detail placeholder
```

For now each placeholder is just `<div>{routeName}</div>` — design phase will fill them in.

### 6. Sepex API client

Create `client/src/lib/sepex.js` — a thin wrapper around the OGC endpoints. Default base URL from `process.env.NEXT_PUBLIC_SEPEX_BASE_URL` (default `http://localhost:5050`).

Endpoints to wrap (full table in DESIGN_BRIEF Appendix B):

```
GET  /processes
GET  /processes/{id}
POST /processes/{id}/execution        // body: { inputs: {...}, tags?: [...] }
                                       // headers: X-SEPEX-User-Email, optional Prefer: respond-async
GET  /jobs?limit&offset&processID&status&submitter&tags
GET  /jobs/{id}
GET  /jobs/{id}/logs
GET  /jobs/{id}/results
GET  /jobs/{id}/metadata
DELETE /jobs/{id}                      // dismiss
GET  /admin/resources
```

**Header gotcha:** the Streamlit `manager.py` sends `X-ProcessAPI-User-Email`; the API survey says `X-SEPEX-User-Email`. **Verify against the actual sepex Go code at `~/Documents/code/sepex/api/`** before locking it in. If both are accepted, prefer `X-SEPEX-User-Email`.

**Job statuses (locked):** `accepted`, `running`, `successful`, `failed`, `dismissed`, `lost`.

### 7. Providers wiring

`client/src/app/providers.jsx`:

```jsx
'use client';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { SessionProvider } from 'next-auth/react';
import { ThemeProvider } from 'next-themes';     // or your own
import { Toaster } from '@/components/ui/sonner';
// QueryClient: defaults to 30s staleTime, retry once, refetchOnWindowFocus false
```

Polling pattern: per-query `refetchInterval` of 2s when data shows `running`/`accepted`, off otherwise. Pause polling when `document.visibilityState === 'hidden'` (TanStack handles this if you set `refetchIntervalInBackground: false`).

### 8. NextAuth + Keycloak skeleton

Mirror reality-view's pattern at `~/Documents/code/reality-view/client/src/app/_utils/auth/`:

- `client/src/app/_utils/auth/auth.js` — `NextAuth({ basePath: "/sepex-viewer/api/auth", providers: [KeycloakProvider({ ... })] })`
- `client/src/app/_utils/auth/auth-helpers.js` — `getAuthUser()`, `requireAuthUser()`
- `client/src/app/api/auth/[...nextauth]/route.js` — handler with basePath fix
- Honor `NEXT_PUBLIC_DEV_BYPASS_AUTH=true` for local dev (return a stub session).

Keycloak env vars (mirror `.env.example` from reality-view):
```
KEYCLOAK_CLIENT_ID=...
KEYCLOAK_CLIENT_SECRET=...
KEYCLOAK_ISSUER=...
NEXTAUTH_SECRET=...
NEXTAUTH_URL=http://localhost:3000
NEXT_PUBLIC_DEV_BYPASS_AUTH=true       # dev only
NEXT_PUBLIC_SEPEX_BASE_URL=http://localhost:5050
```

Add `client/.env.example` documenting these.

### 9. Verify it boots

```bash
cd client
npm run dev
```

Should serve at http://localhost:3000. Visit `/`, `/dashboard`, `/builder`, `/jobs`, `/jobs/test-id` — all five should render their placeholder pages without 404.

### 10. Commit

```bash
git add client/
git commit -m "scaffold Next.js client with Dewberry tokens and route stubs"
```

Don't push yet — the user will review locally first.

---

## Out-of-scope reminders (from DESIGN_BRIEF §11)

Don't build any of these in v1:
- Process *creation* in the UI
- Plugin marketplace
- Multi-tenant / org switching
- Email/Slack notification UI
- File upload during payload building (API doesn't support it)
- A standalone `/processes` catalog route (process picker in Builder is sufficient)

## Pending decisions / open questions

- **`/admin` route + role-gating** — deferred. The brief mentions it; not part of v1 scaffold.
- **`/files` route** — optional, deferred. The Streamlit prototype's file browser was useful but not critical.
- **Submitter header naming** — confirm `X-SEPEX-User-Email` vs `X-ProcessAPI-User-Email` against `~/Documents/code/sepex/api/` source before shipping.

## Known artifacts to clean up later

- **Figma file** `ukqqMd0SruRdcPMAQUr90W` has a renamed default page (encoded debug status in its name) and an empty "01 — Landing" page from a failed MCP build. Either:
  - Delete the file and recreate via MCP (now that the font-style bug is fixed; see commit history of this conversation if needed), or
  - Use it as the Figma Make canvas and let Make populate it
- **In-conversation memory** of the API endpoint survey is preserved in `DESIGN_BRIEF.md` Appendix B. Source-of-truth: `~/Documents/code/sepex/api/` (Go).

## How to pick this up in a new context

1. `cd /Users/curtis/Documents/code/sepex-viewer && git checkout design`
2. Read `DESIGN_BRIEF.md` (skim §0–6, deep on §3 stack and §5 routes)
3. Read this file
4. Execute steps 1–10 above
5. The work is mostly mechanical; deviate from the spec only if reality-view does something different that you want to mirror

Estimated effort: 1–2 hours for a clean scaffold + 30 min for verification.
