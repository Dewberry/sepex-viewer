#!/usr/bin/env bash
set -euo pipefail

# Copy .env.example to .env if .env doesn't exist
if [ ! -f ./.env ]; then
  cp ./.env.example ./.env
  # Set WORKSPACE_PATH to current directory
  sed -i "s|WORKSPACE_PATH=''|WORKSPACE_PATH='$(pwd)'|" ./.env
  echo "Created .env from .env.example with WORKSPACE_PATH=$(pwd)"
fi

# Create data directories for containers
echo "Setting up data directories..."
mkdir -p ./.data/postgres
mkdir -p ./.data/minio
mkdir -p ./.data/api/plugins/cc
echo "Data directories ready"

# Build main adapter
docker build ./cc/cc-adapter -t cc-sepex-adapter:local

# Define plugins to build
declare -a plugins=("seed-generator" "fragility-curve" "hms-mutator" "hms-runner" "ressim-runner")

# Build plugins and copy YAML configs
for plugin in "${plugins[@]}"; do
  echo "Building plugin: $plugin"
  docker build "./cc/$plugin" -t "${plugin}-plugin:sepex"

  # Copy YAML configs - find any .yaml files in the plugin directory
  for yaml_file in "./cc/$plugin"/*.yaml; do
    if [ -f "$yaml_file" ]; then
      filename=$(basename "$yaml_file")
      cp "$yaml_file" "./.data/api/plugins/cc/$filename"
      echo "Copied $filename to plugins directory"
    fi
  done
done

# Create /mnt/sepex directory for local data storage
mkdir -p /mnt/sepex 2>/dev/null || sudo mkdir -p /mnt/sepex
echo "Created /mnt/sepex directory for local data storage"

mkdir -p /mnt/sepex-data 2>/dev/null || sudo mkdir -p /mnt/sepex-data
echo "Created /mnt/sepex-data directory for local data storage"