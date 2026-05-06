# Sepex Viewer
Dashboard for [Sepex](https://github.com/Dewberry/sepex)

![Sepex Viewer landing page](preview.png)

## Docker Commands

**Stop the application:**
```bash
docker compose down
```

**Start the application:**
```bash
docker compose up -d
```

**Rebuild images:**
```bash
docker compose build
```

**Clear all data (database, minio, api):**
```bash
docker compose down && rm -rf ./.data
```

**Prune volumes:**
```bash
docker system prune -a --volumes
```
