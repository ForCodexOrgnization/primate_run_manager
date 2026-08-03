#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONFIG_FILE="${1:-}"
command -v sbatch >/dev/null 2>&1 || { printf 'ERROR: sbatch not found; manager cycle must be submitted through Slurm.\n' >&2; exit 1; }
[[ -n "$CONFIG_FILE" && -f "$CONFIG_FILE" ]] || { printf 'ERROR: Config does not exist: %s\n' "${CONFIG_FILE:-<unset>}" >&2; exit 1; }
CONFIG_FILE="$(cd "$(dirname "$CONFIG_FILE")" && pwd)/$(basename "$CONFIG_FILE")"
# shellcheck disable=SC1090
source "$CONFIG_FILE"
runtime_log_dir="${RUNTIME_LOG_DIR:-${MANAGER_RUNTIME_ROOT:-${RUNTIME_ROOT:-$MANAGER_ROOT}}/logs}"
mkdir -p "$runtime_log_dir"
submission=$(sbatch --parsable \
    --output="${runtime_log_dir}/manager_cycle_%j.out" \
    --error="${runtime_log_dir}/manager_cycle_%j.err" \
    "${REPOSITORY_ROOT}/run_manager_cycle.slurm" "$CONFIG_FILE")
job_id=${submission%%;*}
printf 'Submitted manager cycle Slurm job %s\n' "$job_id"
