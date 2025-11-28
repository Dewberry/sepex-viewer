#!/bin/sh
set -e

HOST=localhost
# Source environment variables from .env
set -a
. ./.env
set +a
export PGPASSWORD="$POSTGRES_PASSWORD"
# Wait for Postgres to be ready
until pg_isready -h "$HOST" -U "$POSTGRES_USER"; do
  echo "Waiting for postgres..."
  sleep 2
done
# Run the SQL script to set up job status logging
PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f ./init-job-status-log.sql
