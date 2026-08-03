#!/usr/bin/env bash
#SBATCH --job-name=primate_manager_cycle
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=04:00:00

set -euo pipefail
CONFIG_FILE="${1:?Usage: sbatch run_manager_cycle.slurm CONFIG}"
[[ "$CONFIG_FILE" = /* ]] || CONFIG_FILE="${SLURM_SUBMIT_DIR:-$PWD}/$CONFIG_FILE"
# Slurm may execute a spool copy, so the repository must come from the config.
# shellcheck disable=SC1090
source "$CONFIG_FILE"
REPOSITORY_ROOT="${MANAGER_ROOT:?MANAGER_ROOT must be configured}"
[[ -x "$REPOSITORY_ROOT/bin/manager_cycle.sh" ]] || {
    printf 'ERROR: Manager script is not executable: %s\n' "$REPOSITORY_ROOT/bin/manager_cycle.sh" >&2
    exit 1
}
mkdir -p "${RUNTIME_LOG_DIR:-${MANAGER_RUNTIME_ROOT:-${RUNTIME_ROOT:-$MANAGER_ROOT}}/logs}"
"$REPOSITORY_ROOT/bin/manager_cycle.sh" "$CONFIG_FILE"
