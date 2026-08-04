#!/usr/bin/env bash
#SBATCH --job-name=primate_manager_daemon
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=5-00:00:00
#SBATCH --signal=B:USR1@300

set -uo pipefail

CONFIG_FILE="${1:?Usage: sbatch run_manager_daemon.slurm CONFIG}"
[[ "$CONFIG_FILE" = /* ]] || CONFIG_FILE="${SLURM_SUBMIT_DIR:-$PWD}/$CONFIG_FILE"
[[ -s "$CONFIG_FILE" ]] || { printf 'ERROR: Config missing or empty: %s\n' "$CONFIG_FILE" >&2; exit 1; }
# shellcheck disable=SC1090
source "$CONFIG_FILE"
REPOSITORY_ROOT="${MANAGER_ROOT:?MANAGER_ROOT must be configured}"
MANAGER_POLL_SECONDS="${MANAGER_POLL_SECONDS:-1800}"
[[ "$MANAGER_POLL_SECONDS" =~ ^[1-9][0-9]*$ ]] || {
    printf 'ERROR: MANAGER_POLL_SECONDS must be a positive integer\n' >&2
    exit 1
}
[[ -x "$REPOSITORY_ROOT/bin/manager_cycle.sh" ]] || {
    printf 'ERROR: Manager script is not executable: %s\n' "$REPOSITORY_ROOT/bin/manager_cycle.sh" >&2
    exit 1
}

runtime_log_dir="${RUNTIME_LOG_DIR:-${MANAGER_RUNTIME_ROOT:-${RUNTIME_ROOT:-$MANAGER_ROOT}}/logs}"
mkdir -p "$runtime_log_dir" "$MANAGER_ROOT/state/locks"
exec 8>"$MANAGER_ROOT/state/locks/manager_daemon.lock"
if ! flock -n 8; then
    printf '[%s] Another manager daemon owns %s; exiting.\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$MANAGER_ROOT/state/locks/manager_daemon.lock" >&2
    exit 0
fi

stop_requested=0
resubmit_requested=0
replacement_submitted=0
sleep_pid=""
on_stop() {
    stop_requested=1
    [[ -z "$sleep_pid" ]] || kill "$sleep_pid" 2>/dev/null || true
    printf '[%s] Stop requested; allowing the active manager cycle to finish.\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" >&2
}
on_walltime() {
    resubmit_requested=1
    stop_requested=1
    [[ -z "$sleep_pid" ]] || kill "$sleep_pid" 2>/dev/null || true
    printf '[%s] Wall-time warning received; finishing the active cycle before exit.\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" >&2
}
trap on_stop TERM INT
trap on_walltime USR1

submit_replacement() {
    (( resubmit_requested == 1 && replacement_submitted == 0 )) || return 0
    local submission job_id
    if submission=$(sbatch --parsable --dependency="afterany:${SLURM_JOB_ID:?}" \
        --output="$runtime_log_dir/manager_daemon_%j.out" \
        --error="$runtime_log_dir/manager_daemon_%j.err" \
        "$REPOSITORY_ROOT/run_manager_daemon.slurm" "$CONFIG_FILE"); then
        job_id=${submission%%;*}
        replacement_submitted=1
        printf '[%s] Submitted replacement daemon job %s after job %s.\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$job_id" "$SLURM_JOB_ID"
    else
        printf '[%s] ERROR: Could not submit replacement daemon; resubmit manually.\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" >&2
    fi
}

cycle=0
consecutive_failures=0
while (( stop_requested == 0 )); do
    cycle=$((cycle + 1))
    started=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
    printf '[%s] Starting manager cycle %d.\n' "$started" "$cycle"

    "$REPOSITORY_ROOT/bin/manager_cycle.sh" "$CONFIG_FILE" &
    cycle_pid=$!
    while true; do
        wait "$cycle_pid" 2>/dev/null && cycle_rc=0 || cycle_rc=$?
        if ! kill -0 "$cycle_pid" 2>/dev/null; then
            break
        fi
    done

    if (( cycle_rc == 0 )); then
        consecutive_failures=0
        delay=$MANAGER_POLL_SECONDS
    else
        consecutive_failures=$((consecutive_failures + 1))
        case "$consecutive_failures" in
            1) delay=300 ;;
            2) delay=900 ;;
            *) delay=1800 ;;
        esac
        printf '[%s] Manager cycle %d failed (exit code %d; consecutive failures %d).\n' \
            "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$cycle" "$cycle_rc" "$consecutive_failures" >&2
    fi

    finished=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
    next_epoch=$(( $(date +%s) + delay ))
    next_check=$(date -u -d "@$next_epoch" +'%Y-%m-%dT%H:%M:%SZ')
    printf '[%s] Completed manager cycle %d; exit code %d; next check %s (delay %ss).\n' \
        "$finished" "$cycle" "$cycle_rc" "$next_check" "$delay"

    (( stop_requested == 0 )) || break
    sleep "$delay" &
    sleep_pid=$!
    wait "$sleep_pid" 2>/dev/null || true
    sleep_pid=""
done

submit_replacement
printf '[%s] Manager daemon exiting cleanly.\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
