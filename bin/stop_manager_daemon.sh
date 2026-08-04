#!/usr/bin/env bash
set -euo pipefail
CONFIG_FILE="${1:-}"
command -v squeue >/dev/null 2>&1 || { printf 'ERROR: squeue not found.\n' >&2; exit 1; }
command -v scancel >/dev/null 2>&1 || { printf 'ERROR: scancel not found.\n' >&2; exit 1; }
[[ -n "$CONFIG_FILE" && -s "$CONFIG_FILE" ]] || { printf 'ERROR: Config missing or empty: %s\n' "${CONFIG_FILE:-<unset>}" >&2; exit 1; }
bash -n "$CONFIG_FILE"
# Validate that this is a manager config without modifying manager state.
# shellcheck disable=SC1090
source "$CONFIG_FILE"
: "${MANAGER_ROOT:?MANAGER_ROOT must be configured}"

mapfile -t jobs < <(squeue --noheader --user="$USER" --name=primate_manager_daemon --states=RUNNING,PENDING --format='%A|%T|%j' | sed '/^[[:space:]]*$/d')
if (( ${#jobs[@]} == 0 )); then
    printf 'No RUNNING or PENDING primate_manager_daemon job found for user %s.\n' "$USER"
    exit 0
fi
for job in "${jobs[@]}"; do
    IFS='|' read -r job_id job_state job_name <<< "$job"
    [[ "$job_name" == primate_manager_daemon ]] || continue
    printf 'Cancelling manager daemon job %s (%s, %s).\n' "$job_id" "$job_state" "$job_name"
    scancel --signal=TERM "$job_id"
done
