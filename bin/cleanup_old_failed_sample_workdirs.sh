#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ "$PIPELINE_MODE" == streaming_per_sample ]] || exit 0
used_now() { if [[ -n "${WORK_DISK_USED_PERCENT_OVERRIDE:-}" ]]; then printf '%s\n' "$WORK_DISK_USED_PERCENT_OVERRIDE"; else work_disk_used_percent; fi; }
validated_complete() { awk -F '\t' -v s="$1" 'NR>1&&$1==s&&$8==1{ok=1}END{exit !ok}' "$VALIDATION_FILE"; }
sample_status() { awk -F '\t' -v s="$1" 'NR>1&&$1==s{print $4;exit}' "$STATUS_FILE"; }

used=$(used_now); (( used >= FAILED_CACHE_CLEAN_TRIGGER_PERCENT )) || exit 0
candidates=$(mktemp); unsorted=$(mktemp); trap 'rm -f "$candidates" "$unsorted"' EXIT
failed_count=0; cancelled_count=0
while IFS=$'\t' read -r sample status wave; do
    safe_sample_id "$sample" || { log "Skipping unsafe sample ID: $sample"; continue; }
    marker="${PIPELINE_WORK_ROOT}/.sample_state/${sample}.failure.tsv"
    if [[ -s "$marker" ]]; then
        epoch=$(marker_field "$marker" first_failure_epoch)
        if [[ ! "$epoch" =~ ^[0-9]+$ ]]; then epoch=$(stat -c %Y "$marker"); log "$sample uses legacy failure marker timestamp"; fi
        printf '%s\t%s\t%s\tFAILED\n' "$epoch" "$sample" "$status"
        failed_count=$((failed_count + 1))
    elif [[ "$status" == PIPELINE_DEFERRED_RETRY ]] && ! validated_complete "$sample" && [[ "$(sample_array_state "$sample")" == CANCELLED ]]; then
        # Markerless cancellation is terminal but can predate the worker's
        # failure trap.  Its lock and Slurm state are checked again below.
        printf '0\t%s\t%s\tCANCELLED_NO_MARKER\n' "$sample" "$status"
        cancelled_count=$((cancelled_count + 1))
    fi
done < <(awk -F '\t' 'NR>1&&$4~/^(PIPELINE_DEFERRED_FAILED|PIPELINE_DEFERRED_RETRY)$/{print $1"\t"$4"\t"$6}' "$STATUS_FILE") > "$unsorted"
sort -t $'\t' -k1,1n -k2,2 "$unsorted" > "$candidates"
log "Eligible failed-cache candidates: $failed_count; eligible markerless-cancelled candidates: $cancelled_count"

processed=0; target_reached=0
while IFS=$'\t' read -r epoch sample status kind; do
    used=$(used_now); if (( used < FAILED_CACHE_CLEAN_TARGET_PERCENT )); then target_reached=1; break; fi
    work=$(sample_work_root "$sample"); root=$(realpath -m "$PIPELINE_WORK_ROOT"); target=$(realpath -m "$work")
    [[ "$root" != / && "$target" == "$root/$sample" && "$target" != "$root" && -d "$target" ]] || continue
    validated_complete "$sample" && continue
    state=$(sample_array_state "$sample")
    if [[ "$kind" == CANCELLED_NO_MARKER ]]; then
        [[ "$status" == PIPELINE_DEFERRED_RETRY && "$state" == CANCELLED ]] || continue
    else
        [[ -s "${PIPELINE_WORK_ROOT}/.sample_state/${sample}.failure.tsv" ]] || continue
        [[ -f "${MANAGER_ROOT}/state/failure_diagnostics/samples/${sample}/ARCHIVE_COMPLETE" ]] || continue
        slurm_state_is_active "$state" && continue
        failure_worker_state=$(marker_field "${PIPELINE_WORK_ROOT}/.sample_state/${sample}.failure.tsv" worker_state)
        running_worker_state=$(marker_field "${PIPELINE_WORK_ROOT}/.sample_state/${sample}.running.tsv" worker_state 2>/dev/null || true)
        [[ "$failure_worker_state" != IMMEDIATE_RETRY && "$running_worker_state" != IMMEDIATE_RETRY ]] || continue
    fi
    # The lock file is persistent metadata; only acquisition determines activity.
    exec {lock_fd}>"${PIPELINE_WORK_ROOT}/.locks/${sample}.lock"; flock -n "$lock_fd" || { exec {lock_fd}>&-; continue; }
    state2=$(sample_array_state "$sample")
    current_status=$(sample_status "$sample")
    safe=1
    [[ -d "$target" ]] || safe=0
    validated_complete "$sample" && safe=0
    if [[ "$kind" == CANCELLED_NO_MARKER ]]; then
        [[ "$current_status" == PIPELINE_DEFERRED_RETRY && "$state2" == CANCELLED ]] || safe=0
    elif slurm_state_is_active "$state2"; then safe=0
    fi
    if (( ! safe )); then flock -u "$lock_fd"; exec {lock_fd}>&-; continue; fi
    bytes=$(du -sb "$target" 2>/dev/null | awk '{print $1+0}'); deleted=$(now_iso)
    if [[ "$DRY_RUN" == 0 ]]; then rm -rf --one-file-system -- "$target"; fi
    reason=archived_failed_sample
    note="failed sample work cache deleted under disk pressure; future deferred retry will run fresh"
    [[ "$kind" != CANCELLED_NO_MARKER ]] || { reason=terminal_cancelled_without_failure_marker; note="cancelled work cache deleted under disk pressure; deferred retry must run fresh"; }
    receipt="${MANAGER_ROOT}/state/receipts/failed_sample_work_cleanup/${sample}.$(date -u +%Y%m%dT%H%M%SZ).tsv"
    { printf 'sample_id\tprevious_manager_status\tslurm_state\tcache_status\tcache_deleted_at\tcache_bytes_released\tretry_mode\twork_root\tcleanup_reason\tfirst_failure_epoch\n'; printf '%s\t%s\t%s\tDELETED\t%s\t%s\tfresh\t%s\t%s\t%s\n' "$sample" "$current_status" "$state2" "$deleted" "${bytes:-0}" "$target" "$reason" "$epoch"; } > "$receipt"
    with_state_lock update_sample_fields "$sample" "notes=$note"
    flock -u "$lock_fd"; exec {lock_fd}>&-
    processed=$((processed + 1))
    log "Deleted $reason cache $sample (${bytes:-0} bytes); filesystem was ${used}%"
done < "$candidates"

if (( target_reached )); then
    log "Failed-cache cleanup stopped: target reached (below ${FAILED_CACHE_CLEAN_TARGET_PERCENT}%)"
else
    log "Failed-cache cleanup stopped: eligible candidates exhausted (deleted $processed)"
fi
