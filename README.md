# Sepex Viewer

Prototype Streamlit dashboard for [Sepex](https://github.com/Dewberry/sepex)

| <img src="./preview.png"> |
|:--:|
| *Jobs overview with per-job details* |

## Quick Start

### 1. Environment variables

```bash
copy .env.example .env
```

The defaults work for local development (postgres + minio + auth disabled).

### 2. Create the shared docker network

The compose file expects an external network named `process_api_net`. Create it once per machine:

```bash
docker network create process_api_net
```

### 3. Start the stack

```bash
docker compose up -d
```

The viewer is at http://localhost:8501. First boot pulls the sepex API image and initializes postgres + minio.

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

**Rebuild the viewer image:**
```bash
docker compose build viewer
```

**Rebuild and restart:**
```bash
docker compose up -d --build
```

**View logs:**
```bash
docker compose logs -f viewer
```

**Clear all data (postgres, minio, api state):**
```bash
docker compose down
rmdir /s /q .data
```

**Prune volumes:**
```bash
docker system prune -a --volumes
```

---

## Services

| Service | Image | Purpose |
|---------|-------|---------|
| **viewer** (`app/`) | Streamlit, built locally | Dashboard UI for sepex |
| **sepex** | `ghcr.io/dewberry/sepex/api` | Process execution API |
| **postgres** | `postgres:17.2-alpine3.20` | Job and process metadata |
| **minio** | `quay.io/minio/minio` | S3-compatible object storage |

The viewer hits the sepex API over HTTP (`SEPEX_BASE_URL`). The API uses postgres for job state and minio for storage. To run against AWS S3 instead, set `STORAGE_SERVICE=aws-s3` in `.env`.

The viewer container mounts the repo at `/app`, so changes under `app/` hot-reload without a rebuild.

## Access Points

- Viewer: http://localhost:8501
- sepex API: http://localhost:5050
- sepex API docs (Swagger): http://localhost:5050/swagger/index.html
- MinIO console: http://localhost:9001 (user / password from `.env`)
- Postgres: `localhost:5432`

Configuration toggles (db, storage, auth) are documented inline in [`.env.example`](.env.example).

## Registering Processes

The sepex API registers processes from yaml files at startup. Point `PLUGINS_LOAD_DIR` in `.env` at a directory of yaml configs and restart the sepex container — every `.yaml` / `.yml` in there is read and registered.

[`examples/register-processes/pyecho.yaml`](examples/register-processes/pyecho.yaml) is a sample config — a Python plugin that echoes its input. Drop it (and any others) into your plugins directory, restart, and the processes show up in the **Processes** column of the viewer.