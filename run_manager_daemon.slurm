#!/usr/bin/env bash
#SBATCH --job-name=primate_manager_daemon
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --signal=B:USR1@300

set -uo pipefail

CONFIG_FILE="${1:?Usage: sbatch run_manager_daemon.slurm CONFIG}"
[[ "$CONFIG_FILE" = /* ]] || CONFIG_FILE="${SLURM_SUBMIT_DIR:-$PWD}/$CONFIG_FILE"
[[ -s "$CONFIG_FILE" ]] || { printf 'ERROR: Config missing or empty: %s\n' "$CONFIG_FILE" >&2; exit 1; }
# shellcheck disable=SC1090
source "$CONFIG_FILE"
REPOSITORY_ROOT="${MANAGER_ROOT:?MANAGER_ROOT must be configured}"
MANAGER_DAEMON_TIME="${MANAGER_DAEMON_TIME:-23:30:00}"
MANAGER_POLL_SECONDS="${MANAGER_POLL_SECONDS:-1800}"
DISK_PRESSURE_POLL_SECONDS="${DISK_PRESSURE_POLL_SECONDS:-60}"
[[ "$MANAGER_POLL_SECONDS" =~ ^[1-9][0-9]*$ ]] || {
    printf 'ERROR: MANAGER_POLL_SECONDS must be a positive integer\n' >&2
    exit 1
}
[[ "$DISK_PRESSURE_POLL_SECONDS" =~ ^[1-9][0-9]*$ ]] || {
    printf 'ERROR: DISK_PRESSURE_POLL_SECONDS must be a positive integer\n' >&2
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
daemon_code_sha=$(sha256sum "$REPOSITORY_ROOT/run_manager_daemon.slurm" | awk '{print $1}')
repository_sha=$(git -C "$REPOSITORY_ROOT" rev-parse HEAD 2>/dev/null || printf unknown)
printf 'slurm_job_id\tstarted_at\tdaemon_code_sha\trepository_sha\n%s\t%s\t%s\t%s\n' \
  "${SLURM_JOB_ID:-interactive}" "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$daemon_code_sha" "$repository_sha" \
  > "$MANAGER_ROOT/state/manager_daemon_status.tsv"

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
    printf '[%s] Wall-time warning received; queueing successor before waiting for the active cycle.\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" >&2
    submit_replacement
}
trap on_stop TERM INT

submit_replacement() {
    (( resubmit_requested == 1 && replacement_submitted == 0 )) || return 0
    local submission job_id
    if submission=$(sbatch --parsable --dependency="afterany:${SLURM_JOB_ID:?}" \
        --time="$MANAGER_DAEMON_TIME" \
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
trap on_walltime USR1

# Normal cadence is split into lightweight disk probes plus exact Slurm
# admission reconciliation. Full manager state scans remain on the slow cadence.
normal_pressure_sleep() {
    local remaining=$MANAGER_POLL_SECONDS slice used
    while (( remaining > 0 && stop_requested == 0 )); do
        slice=$DISK_PRESSURE_POLL_SECONDS
        (( slice > remaining )) && slice=$remaining
        sleep "$slice" & sleep_pid=$!
        wait "$sleep_pid" 2>/dev/null || true
        sleep_pid=""
        (( stop_requested == 0 )) || return 1
        remaining=$((remaining - slice))
        used=$(df -P "${WORK_DISK_CHECK_PATH:-$PIPELINE_WORK_ROOT}" | awk 'NR==2{gsub(/%/,"",$5); print $5}')
        [[ "$used" =~ ^[0-9]+$ ]] || { printf '[%s] WARNING: work disk pressure probe returned an invalid value.\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" >&2; continue; }
        # Reconcile global concurrency and resume priority on every lightweight
        # cadence, even when disk use is healthy. This does not run a state scan.
        WORK_DISK_USED_PERCENT_OVERRIDE="$used" "$REPOSITORY_ROOT/bin/control_streaming_array_admission.sh" "$CONFIG_FILE"
        if (( used >= WORK_CRITICAL_PERCENT )); then
            printf '[%s] Critical work disk pressure detected by lightweight probe (%s%%); pending array admission is closed.\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$used"
            return 2
        fi
    done
    return 0
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
        hold_file="$MANAGER_ROOT/state/streaming_array_disk_holds.tsv"
        phase_file="$MANAGER_ROOT/state/manager_phase.tsv"
        if { [[ -s "$hold_file" ]] && (( $(awk 'END{print NR-1}' "$hold_file") > 0 )); } ||
           { [[ -s "$phase_file" ]] && awk -F '\t' 'NR==2&&$1=="PAUSED_DISK_PRESSURE"{found=1}END{exit !found}' "$phase_file"; }; then
            delay=$DISK_PRESSURE_POLL_SECONDS
            printf '[%s] Disk pressure active; using accelerated %ss admission/cleanup check.\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$delay"
        fi
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
    if (( cycle_rc == 0 && delay == MANAGER_POLL_SECONDS )); then
        normal_pressure_sleep && sleep_rc=0 || sleep_rc=$?
        # rc=2 means admission control has already run and the loop should
        # immediately start its expensive cleanup/reconciliation cycle.
        (( sleep_rc != 1 )) || break
    else
        sleep "$delay" &
        sleep_pid=$!
        wait "$sleep_pid" 2>/dev/null || true
        sleep_pid=""
    fi
done

submit_replacement
printf '[%s] Manager daemon exiting cleanly.\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
