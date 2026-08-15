#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIG_FILE="${1:-}"
command -v sbatch >/dev/null 2>&1 || { printf 'ERROR: sbatch not found; the daemon must run through Slurm.\n' >&2; exit 1; }
command -v squeue >/dev/null 2>&1 || { printf 'ERROR: squeue not found.\n' >&2; exit 1; }
[[ -n "$CONFIG_FILE" && -s "$CONFIG_FILE" ]] || { printf 'ERROR: Config missing or empty: %s\n' "${CONFIG_FILE:-<unset>}" >&2; exit 1; }
CONFIG_FILE="$(cd "$(dirname "$CONFIG_FILE")" && pwd)/$(basename "$CONFIG_FILE")"
bash -n "$CONFIG_FILE"
# shellcheck disable=SC1090
source "$CONFIG_FILE"
: "${MANAGER_ROOT:?MANAGER_ROOT must be configured}"
: "${MANAGER_DAEMON_TIME:=23:30:00}"
poll_seconds="${MANAGER_POLL_SECONDS:-1800}"
[[ "$poll_seconds" =~ ^[1-9][0-9]*$ ]] || { printf 'ERROR: MANAGER_POLL_SECONDS must be a positive integer.\n' >&2; exit 1; }
[[ -x "$MANAGER_ROOT/bin/manager_cycle.sh" ]] || { printf 'ERROR: Manager cycle is not executable under MANAGER_ROOT: %s\n' "$MANAGER_ROOT" >&2; exit 1; }

existing=$(squeue --noheader --user="$USER" --name=primate_manager_daemon --states=RUNNING,PENDING --format='%A %T' | sed '/^[[:space:]]*$/d')
if [[ -n "$existing" ]]; then
    printf 'ERROR: A manager daemon is already RUNNING or PENDING:\n%s\n' "$existing" >&2
    exit 1
fi
runtime_log_dir="${RUNTIME_LOG_DIR:-${MANAGER_RUNTIME_ROOT:-${RUNTIME_ROOT:-$MANAGER_ROOT}}/logs}"
mkdir -p "$runtime_log_dir"
submission=$(sbatch --parsable \
    --time="$MANAGER_DAEMON_TIME" \
    --output="$runtime_log_dir/manager_daemon_%j.out" \
    --error="$runtime_log_dir/manager_daemon_%j.err" \
    "$REPOSITORY_ROOT/run_manager_daemon.slurm" "$CONFIG_FILE")
job_id=${submission%%;*}
printf 'Submitted manager daemon Slurm job %s\n' "$job_id"
