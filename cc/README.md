# SEPEX Setup for USACE cloud-compute

This directory contains Docker-based computational components for SEPEX workflows, including the adapter and various files for using USACE cloud compute plugins.

## Directory Structure

- **cc-adapter**: Main adapter service that integrates CC plugins with SEPEX
- **fragility-curve**: Fragility curve computation plugin
- **hms-mutator**: HMS (Hydrologic Modeling System) mutation plugin with two variants
- **hms-runner**: HMS execution plugin
- **ressim-runner**: Resource simulation plugin
- **seed-generator**: Seed/block generation plugins
- **example-payloads**: Sample job submission payloads for testing

## Using `prep-cc-dev-env.sh`

The `prep-cc-dev-env.sh` script automates the setup of the CC development environment. Run it from the root directory:

```bash
./prep-cc-dev-env.sh
```

### What the Script Does

1. **Environment Configuration**
   - Creates `.env` from `.env.example` if it doesn't exist
   - Configures `WORKSPACE_PATH` to the current directory
   - Resolves `SEPEX_DIR` and `DATA_DIR` paths from `.env`

2. **Directory Structure**
   - Creates required SEPEX directories: `postgres`, `minio`, `api/plugins/cc`, `logs`, `tmp/job_logs`, `plugins`
   - Creates data directories for each plugin: `seed-generator-seeds`, `seed-generator-blocks`, `fragility-curve`, `hms-mutator-fishnets`, `hms-mutator-full-sim-sst`, `hms-runner`, `ressim-runner`

3. **Docker Image Building**
   - Builds the CC adapter: `cc-sepex-adapter:local`
   - Builds plugin images with `:sepex` tag:
     - `seed-generator-plugin:sepex`
     - `fragility-curve-plugin:sepex`
     - `hms-mutator-plugin:sepex`

4. **Configuration Deployment**
   - Copies YAML plugin definitions from `cc/<plugin>/*.yaml` to `SEPEX_DIR/api/plugins/cc/`
   - Updates data mount paths in YAML files to use absolute paths

### Prerequisites

- Docker installed and running
- `.env.example` file present in the root directory
- Bash shell
- Write permissions in the project directory

## Troubleshooting

### Script Fails with "Error: SEPEX_DIR or DATA_DIR not found"

**Problem**: The `.env` file is missing required variables.

**Solution**:
1. Ensure `.env.example` exists in the root directory
2. Check that `SEPEX_DIR` and `DATA_DIR` are defined:
   ```bash
   grep "^SEPEX_DIR=" .env
   grep "^DATA_DIR=" .env
   ```
3. If missing, manually add them to `.env`:
   ```
   SEPEX_DIR=local
   DATA_DIR=data
   ```

### Docker Build Fails

**Problem**: One or more Docker images fail to build.

**Solution**:
1. Check Docker is running: `docker ps`
2. Review the build logs for specific plugin errors
3. Ensure all `Dockerfile`s in `cc/` subdirectories are valid
4. Try building a single plugin manually:
   ```bash
   docker build ./cc/seed-generator -t seed-generator-plugin:sepex
   ```

### Permission Denied on Directories

**Problem**: Script fails to create directories.

**Solution**:
1. Ensure you have write permissions: `ls -ld local/` and `ls -ld data/`
2. If directories exist with wrong permissions, fix them:
   ```bash
   chmod 755 local data
   ```

### YAML Files Not Copied to Plugins Directory

**Problem**: YAML configurations aren't appearing in `SEPEX_DIR/api/plugins/cc/`

**Solution**:
1. Verify YAML files exist in plugin directories:
   ```bash
   ls -la cc/*/\*.yaml
   ```
2. Check that directories were created:
   ```bash
   ls -la local/api/plugins/cc/
   ```
3. Manually verify a YAML file has correct data paths:
   ```bash
   grep "^  " cc/seed-generator/seed-generator-seeds.yaml | head -5
   ```

### Data Mount Paths Are Incorrect

**Problem**: Containers can't access data files due to wrong mount paths.

**Solution**:
1. Verify the absolute paths in copied YAML files:
   ```bash
   grep "/data" local/api/plugins/cc/\*.yaml
   ```
2. Check that the data directories exist:
   ```bash
   ls -la data/
   ```
3. If paths are wrong, re-run the script or manually update YAML files with correct paths

## Re-running the Script

To reset and rebuild the environment:

```bash
# Full reset (removes local directories and rebuilds)
rm -rf local/
./prep-cc-dev-env.sh
```

## Plugin-Specific Information

### Seed Generator
- Generates initial blocks or seeds for simulations
- Inputs: Configuration files
- Outputs: `blocks.json` or `seeds.json`

### Fragility Curve
- Computes structural fragility curves
- Inputs: System response data, failure elevations
- Outputs: Fragility curve JSON

### HMS Mutator
- Mutates HMS (Hydrologic Modeling System) parameters
- Two variants: fishnets and full-sim-sst
- Outputs: Modified HMS configurations

### HMS Runner
- Executes HMS simulations
- Requires pre-configured HMS setup
- Outputs: Simulation results

### Ressim Runner
- Executes resource simulations
- Outputs: Resource allocation results

## Next Steps

After running `prep-cc-dev-env.sh`:

1. Verify all Docker images built successfully:
   ```bash
   docker images | grep -E "sepex|plugin"
   ```

2. Start the main application with Docker Compose:
   ```bash
   docker-compose up -d
   ```

3. Submit test jobs using payloads in `cc/example-payloads/`

4. Monitor job logs in `local/api/logs/`

## For More Help

- Check container logs: `docker logs <container_id>`
- Verify mounted volumes: `docker inspect <container_id>`
- Review `.env` configuration
- Check SEPEX API documentation
