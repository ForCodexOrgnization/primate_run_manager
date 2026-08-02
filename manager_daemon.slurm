#!/usr/bin/env bash
#SBATCH --job-name=primate_mgr
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=23:30:00
#SBATCH --output=logs/manager_%j.out
#SBATCH --error=logs/manager_%j.err

set -euo pipefail
CONFIG_FILE="${RUN_MANAGER_CONFIG:?Set RUN_MANAGER_CONFIG to an absolute config path}"
MANAGER_ROOT="$(cd "$(dirname "$0")" && pwd)"
source "$CONFIG_FILE"
mkdir -p "${MANAGER_ROOT}/logs"
while true; do
    "${MANAGER_ROOT}/bin/manager_cycle.sh" "$CONFIG_FILE" || true
    sleep "${MANAGER_POLL_SECONDS:-1800}"
done
