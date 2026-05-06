#!/bin/sh
set -e

# Wait for the Sepex API to accept TCP connections.
# Hostname comes from the docker-compose service name; override with
# SEPEX_HOST / SEPEX_PORT if running this container elsewhere.
# SEPEX_HOST="${SEPEX_HOST:-sepex}"
# SEPEX_PORT="${SEPEX_PORT:-5050}"

# echo "Waiting for Sepex API at ${SEPEX_HOST}:${SEPEX_PORT}..."
# attempts=0
# until nc -z "${SEPEX_HOST}" "${SEPEX_PORT}" 2>/dev/null; do
#   attempts=$((attempts + 1))
#   if [ "${attempts}" -ge 60 ]; then
#     echo "Sepex API not reachable after 60s; starting dev server anyway."
#     break
#   fi
#   sleep 1
# done

# if [ "${attempts}" -lt 60 ]; then
#   echo "Sepex API ready"
# fi

echo "Starting Next.js dev server on :${PORT:-3000}..."
exec npm run dev -- --hostname 0.0.0.0
