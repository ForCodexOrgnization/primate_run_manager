#!/usr/bin/env bash
# Hold only pending elements of active streaming arrays.  `scontrol hold` sets a
# pending job's priority to zero; unlike suspend, it does not stop running work.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ "$PIPELINE_MODE" == streaming_per_sample ]] || exit 0

state_file="${MANAGER_ROOT}/state/streaming_array_disk_holds.tsv"
[[ -e "$state_file" ]] || printf 'array_job_id\tarray_task_id\theld_at\thold_reason\n' > "$state_file"
used="${WORK_DISK_USED_PERCENT_OVERRIDE:-$(work_disk_used_percent)}"

pending_elements() {
    local job="$1"
    squeue --noheader --array --jobs="$job" --states=PENDING --format='%A|%a|%T|%r' 2>/dev/null |
      awk -F '|' '$1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ && $3=="PENDING" {print}'
}

# The record is the authority that distinguishes our holds from unrelated user
# holds.  Forget records as soon as the exact element is no longer pending with
# a user hold; in particular, never release an administrator's replacement hold.
prune_stale_records() {
    [[ -s "$state_file" ]] || return 0
    command -v squeue >/dev/null 2>&1 || return 0
    local retained job task held_at reason current
    retained=$(mktemp)
    head -n 1 "$state_file" > "$retained"
    while IFS=$'\t' read -r job task held_at reason; do
        [[ "$job" == array_job_id ]] && continue
        current=$(pending_elements "$job" | awk -F '|' -v t="$task" '$2==t{print $4;exit}')
        [[ "$current" == JobHeldUser ]] && printf '%s\t%s\t%s\t%s\n' "$job" "$task" "$held_at" "$reason" >> "$retained"
    done < "$state_file"
    mv "$retained" "$state_file"
}

prune_stale_records

if (( used >= WORK_CRITICAL_PERCENT )); then
    command -v squeue >/dev/null 2>&1 && command -v scontrol >/dev/null 2>&1 || {
        log "ERROR: critical disk pressure but squeue/scontrol is unavailable; array admission cannot be held"; exit 1;
    }
    held=0
    while IFS=$'\t' read -r job; do
        [[ "$job" =~ ^[0-9]+$ ]] || continue
        while IFS='|' read -r array task state reason; do
            [[ "$reason" != JobHeldUser && "$reason" != JobHeldAdmin ]] || continue
            awk -F '\t' -v j="$array" -v t="$task" 'NR>1&&$1==j&&$2==t{found=1}END{exit !found}' "$state_file" && continue
            # Slurm rejects hold if this element won the scheduling race and is
            # no longer pending.  It is never suspended or cancelled here.
            if scontrol hold "${array}_${task}"; then
                printf '%s\t%s\t%s\tcritical work filesystem %s%%\n' "$array" "$task" "$(now_iso)" "$used" >> "$state_file"
                held=$((held + 1))
            fi
        done < <(pending_elements "$job")
    done < <(awk -F '\t' 'NR>1&&$9~/^(CREATED|SUBMITTED|RUNNING)$/&&$4~/^[0-9]+$/{print $4}' "$WAVE_STATUS_FILE" | sort -u)
    log "Disk pressure array admission: HELD; reason: critical work filesystem ${used}%; newly held pending elements: $held; release threshold: ${WORK_ARRAY_RELEASE_PERCENT}%"
elif (( used <= WORK_ARRAY_RELEASE_PERCENT )); then
    released=0; retained=$(mktemp); trap 'rm -f "$retained"' EXIT
    head -n 1 "$state_file" > "$retained"
    while IFS=$'\t' read -r job task held_at reason; do
        [[ "$job" == array_job_id ]] && continue
        current=$(pending_elements "$job" | awk -F '|' -v t="$task" '$2==t{print $4;exit}')
        if [[ "$current" == JobHeldUser ]]; then
            if scontrol release "${job}_${task}"; then released=$((released + 1)); else printf '%s\t%s\t%s\t%s\n' "$job" "$task" "$held_at" "$reason" >> "$retained"; fi
        fi
        # Completed/cancelled elements, and holds changed by an administrator,
        # need no manager release and are intentionally forgotten.
    done < "$state_file"
    mv "$retained" "$state_file"; trap - EXIT
    log "Disk pressure array admission: ACTIVE; released manager-held elements: $released; release threshold: ${WORK_ARRAY_RELEASE_PERCENT}%"
else
    count=$(awk 'END{print (NR > 0 ? NR - 1 : 0)}' "$state_file")
    log "Disk pressure array admission: $([[ $count -gt 0 ]] && echo HELD || echo ACTIVE); hysteresis band; release threshold: ${WORK_ARRAY_RELEASE_PERCENT}%"
fi
