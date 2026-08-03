#!/usr/bin/env bash
#SBATCH --job-name=primate_import
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=24:00:00
#SBATCH --output=logs/import_existing_%j.out
#SBATCH --error=logs/import_existing_%j.err

set -euo pipefail
CONFIG_FILE="${1:?Usage: sbatch run_import_existing.slurm CONFIG}"
REPOSITORY_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ "$CONFIG_FILE" = /* ]] || CONFIG_FILE="${SLURM_SUBMIT_DIR:-$PWD}/$CONFIG_FILE"
# shellcheck disable=SC1090
source "$CONFIG_FILE"
mkdir -p "${RUNTIME_LOG_DIR:-${MANAGER_RUNTIME_ROOT:-${RUNTIME_ROOT:-$MANAGER_ROOT}}/logs}"
if command -v module >/dev/null 2>&1 && [[ -n "${SAMTOOLS_MODULE:-}" ]]; then
    module load "$SAMTOOLS_MODULE"
fi
"${REPOSITORY_ROOT}/bin/initialize_samples.sh" "$CONFIG_FILE"
"${REPOSITORY_ROOT}/bin/import_existing_results.sh" "$CONFIG_FILE"
"${REPOSITORY_ROOT}/bin/report_incomplete_samples.sh" "$CONFIG_FILE"
"${REPOSITORY_ROOT}/bin/show_status.sh" "$CONFIG_FILE"
