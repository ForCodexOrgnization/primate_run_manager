#!/usr/bin/env bash
# Disk-pressure admission control for streaming arrays.  Only exact PENDING
# array elements are held; RUNNING work is never suspended or cancelled.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ "$PIPELINE_MODE" == streaming_per_sample ]] || exit 0

state_file="${MANAGER_ROOT}/state/streaming_array_disk_holds.tsv"
[[ -e "$state_file" ]] || printf 'array_job_id\tarray_task_id\theld_at\thold_reason\n' > "$state_file"
used="${WORK_DISK_USED_PERCENT_OVERRIDE:-$(work_disk_used_percent)}"

# %A/%a is job id/account on the Slurm version deployed at McCleary.  %F/%K
# are the authoritative expanded ArrayJobID/ArrayTaskID fields.
pending_elements() {
    local job="$1" out
    out=$(squeue --noheader --array --jobs="$job" --states=PENDING --format='%F|%K|%T|%r') || return 1
    awk -F '|' '$1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ && $3=="PENDING" {print}' <<< "$out"
}
is_recorded() { awk -F '\t' -v j="$1" -v t="$2" 'NR>1&&$1==j&&$2==t{f=1}END{exit !f}' "$state_file"; }

# Drop ownership records for elements that completed, started, or whose hold
# was replaced. On query failure retain the record rather than risk releasing
# or losing ownership information.
pruned=$(mktemp); head -n 1 "$state_file" > "$pruned"
while IFS=$'\t' read -r job task held_at reason; do
    [[ "$job" == array_job_id ]] && continue
    if rows=$(pending_elements "$job" 2>/dev/null); then
        current=$(awk -F '|' -v t="$task" '$2==t{print $4;exit}' <<< "$rows")
        [[ "$current" == JobHeldUser ]] && printf '%s\t%s\t%s\t%s\n' "$job" "$task" "$held_at" "$reason" >> "$pruned"
    else printf '%s\t%s\t%s\t%s\n' "$job" "$task" "$held_at" "$reason" >> "$pruned"; fi
done < "$state_file"
mv "$pruned" "$state_file"

if (( used >= WORK_CRITICAL_PERCENT )); then
    command -v squeue >/dev/null && command -v scontrol >/dev/null || { log "ERROR: critical disk pressure: admission tools unavailable"; exit 1; }
    pending_candidates=0; already_manager_held=0; newly_held=0; hold_failures=0
    jobs=$(awk -F '\t' 'NR>1&&$9~/^(CREATED|SUBMITTED|RUNNING)$/&&$4~/^[0-9]+$/{print $4}' "$WAVE_STATUS_FILE" | sort -u)
    for job in $jobs; do
        rows=$(pending_elements "$job") || { log "ERROR: squeue failed while discovering pending elements for $job"; exit 1; }
        while IFS='|' read -r array task state reason; do
            [[ -n "$array" ]] || continue; pending_candidates=$((pending_candidates+1))
            if is_recorded "$array" "$task"; then already_manager_held=$((already_manager_held+1)); continue; fi
            # Admin and unrelated user holds are deliberately not claimed by us.
            [[ "$reason" != JobHeldUser && "$reason" != JobHeldAdmin ]] || continue
            if scontrol hold "${array}_${task}"; then
                printf '%s\t%s\t%s\tcritical work filesystem %s%%\n' "$array" "$task" "$(now_iso)" "$used" >> "$state_file"
                newly_held=$((newly_held+1))
            else hold_failures=$((hold_failures+1)); fi
        done <<< "$rows"
    done
    # Re-query: a record is protected only while the exact element has a user
    # hold. A task that raced to RUNNING is no longer pending and is safe.
    unprotected_pending=0; verified=$(mktemp); trap 'rm -f "$verified"' EXIT
    head -n 1 "$state_file" > "$verified"
    for job in $jobs; do
        rows=$(pending_elements "$job") || { log "ERROR: squeue verification failed for $job"; exit 1; }
        while IFS='|' read -r array task state reason; do
            [[ -n "$array" ]] || continue
            if is_recorded "$array" "$task" && [[ "$reason" == JobHeldUser ]]; then
                awk -F '\t' -v j="$array" -v t="$task" '$1==j&&$2==t{print;exit}' "$state_file" >> "$verified"
            elif [[ "$reason" != JobHeldAdmin && "$reason" != JobHeldUser ]]; then
                unprotected_pending=$((unprotected_pending+1))
            fi
        done <<< "$rows"
    done
    mv "$verified" "$state_file"; trap - EXIT
    admission=HELD; (( unprotected_pending == 0 )) || admission=DEGRADED
    log "Disk pressure array admission: $admission; pending_candidates=$pending_candidates; already_manager_held=$already_manager_held; newly_held=$newly_held; hold_failures=$hold_failures; unprotected_pending=$unprotected_pending; release_threshold=${WORK_ARRAY_RELEASE_PERCENT}%"
    (( unprotected_pending == 0 )) || exit 1
elif (( used <= WORK_ARRAY_RELEASE_PERCENT )); then
    released=0; retained=$(mktemp); trap 'rm -f "$retained"' EXIT; head -n 1 "$state_file" > "$retained"
    while IFS=$'\t' read -r job task held_at reason; do
        [[ "$job" == array_job_id ]] && continue
        rows=$(pending_elements "$job") || { printf '%s\t%s\t%s\t%s\n' "$job" "$task" "$held_at" "$reason" >> "$retained"; continue; }
        current=$(awk -F '|' -v t="$task" '$2==t{print $4;exit}' <<< "$rows")
        if [[ "$current" == JobHeldUser ]]; then
            scontrol release "${job}_${task}" && released=$((released+1)) || printf '%s\t%s\t%s\t%s\n' "$job" "$task" "$held_at" "$reason" >> "$retained"
        fi
    done < "$state_file"
    mv "$retained" "$state_file"; trap - EXIT
    log "Disk pressure array admission: ACTIVE; released_manager_held=$released; release_threshold=${WORK_ARRAY_RELEASE_PERCENT}%"
else
    count=$(awk 'END{print NR?NR-1:0}' "$state_file")
    log "Disk pressure array admission: $([[ $count -gt 0 ]] && echo HELD || echo ACTIVE); hysteresis; manager_held=$count"
fi
