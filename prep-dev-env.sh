#!/usr/bin/env bash
set -euo pipefail

declare -a sepex_directories=("postgres" "minio" "api/plugins/cc" "logs" "tmp/job_logs" "plugins")

declare -a data_directories=("sepex" "sepex-data" )

declare -a plugins=("seed-generator" "fragility-curve" "hms-mutator") # "hms-runner" "ressim-runner")

# Copy .env.example to .env if .env doesn't exist
if [ ! -f ./.env ]; then
  cp ./.env.example ./.env
  # Set WORKSPACE_PATH to current directory (macOS-compatible sed)
  sed -i '' "s|WORKSPACE_PATH=''|WORKSPACE_PATH='$(pwd)'|" ./.env
  echo "Created .env from .env.example with WORKSPACE_PATH=$(pwd)"
fi

# Load SEPEX_DIR and DATA_DIR from .env (remove quotes and comments)
SEPEX_DIR=$(grep "^SEPEX_DIR=" ./.env | cut -d'=' -f2 | cut -d'#' -f1 | tr -d "'\" " | head -1)
DATA_DIR=$(grep "^DATA_DIR=" ./.env | cut -d'=' -f2 | cut -d'#' -f1 | tr -d "'\" " | head -1)

# Verify variables are set
if [ -z "$SEPEX_DIR" ] || [ -z "$DATA_DIR" ]; then
  echo "Error: SEPEX_DIR or DATA_DIR not found in .env file"
  exit 1
fi

# Substitute ${SEPEX_DIR} and ${DATA_DIR} placeholders in .env with actual values (with ./ prefix)
sed -i '' "s|\${SEPEX_DIR}|./$SEPEX_DIR|g" ./.env
sed -i '' "s|\${DATA_DIR}|./$DATA_DIR|g" ./.env

# Create sepex directories for containers
echo "Setting up sepex directories in $SEPEX_DIR/"
for dir in "${sepex_directories[@]}"; do
  mkdir -p "$SEPEX_DIR/$dir"
done
echo "Sepex directories ready in $SEPEX_DIR/"

# Create data directories for containers
echo "Setting up data directories in $DATA_DIR/"
for dir in "${data_directories[@]}"; do
  mkdir -p "$DATA_DIR/$dir"
done
echo "Data directories ready in $DATA_DIR/"

# Build main adapter
docker build ./cc/cc-adapter -t cc-sepex-adapter:local

# Build plugins and copy YAML configs
for plugin in "${plugins[@]}"; do
  echo "Building plugin: $plugin"
  docker build "./cc/$plugin" -t "${plugin}-plugin:sepex"

  # Copy YAML configs - find any .yaml files in the plugin directory
  for yaml_file in "./cc/$plugin"/*.yaml; do
    if [ -f "$yaml_file" ]; then
      filename=$(basename "$yaml_file")
      cp "$yaml_file" "$SEPEX_DIR/api/plugins/cc/$filename"
      echo "Copied $filename to plugins directory"
    fi
  done
done