# Sepex Viewer

Dashboard for [Sepex](https://github.com/Dewberry/sepex)

![Sepex Viewer landing page](preview.png)

## Quick Start

### 1. Environment variables

```bash
cp .env.example .env
```

The defaults work for local development. Set `NEXT_PUBLIC_SEPEX_BASE_URL` to point the viewer at whichever Sepex API instance you want (local docker, papi, etc.).

### 2. Create the shared docker network

The compose file expects an external network named `process_api_net`. Create it once per machine:

```bash
docker network create process_api_net
```

### 3. Start the stack

```bash
docker compose up -d
```

The viewer is at http://localhost:3000. The sepex / postgres / minio services in the compose file are commented out by default — uncomment them if you want a fully local backend instead of pointing at a remote API.

---

## Docker Commands

**Stop the application:**
```bash
docker compose down
```

**Start the application:**
```bash
docker compose up -d
```

**Rebuild the client image:**
```bash
docker compose build client
```

**Rebuild and restart:**
```bash
docker compose up -d --build
```

**View logs:**
```bash
docker compose logs -f client
```

**Clear all data (postgres, minio, api state):**
```bash
docker compose down && rm -rf ./.data
```

**Prune volumes:**
```bash
docker system prune -a --volumes
```

---

## Services

| Service | Image | Purpose |
|---------|-------|---------|
| **client** (`client/`) | Next.js, built locally | Viewer UI for sepex |
| **sepex** | `ghcr.io/dewberry/sepex/api` | Process execution API (commented out — uncomment for local backend) |
| **postgres** | `postgres:17.2-alpine3.20` | Job and process metadata (commented out) |
| **minio** | `quay.io/minio/minio` | S3-compatible object storage (commented out) |

The client hits the sepex API over HTTP (`NEXT_PUBLIC_SEPEX_BASE_URL`). When the local sepex service is enabled, the API uses postgres for job state and minio for storage. To run against AWS S3 instead, set `STORAGE_SERVICE=aws-s3` in `.env`.

The client container mounts the repo at `/app`, so changes under `client/` hot-reload without a rebuild.

## Access Points

- Viewer: http://localhost:3000
- sepex API (when running locally): http://localhost:5050
- MinIO console (when running locally): http://localhost:9001 (user / password from `.env`)
- Postgres (when running locally): `localhost:5432`

Configuration toggles (db, storage, auth) are documented inline in [`.env.example`](.env.example).

## Registering Processes

The sepex API registers processes from yaml files at startup. Point `PLUGINS_LOAD_DIR` in `.env` at a directory of yaml configs and restart the sepex container — every `.yaml` / `.yml` in there is read and registered.

[`examples/register-processes/pyecho.yaml`](examples/register-processes/pyecho.yaml) is a sample config — a Python plugin that echoes its input. Drop it (and any others) into your plugins directory, restart, and the processes show up in the **Builder** page of the viewer.
