#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ "$PIPELINE_MODE" == streaming_per_sample ]] || exit 0
used_now() { if [[ -n "${WORK_DISK_USED_PERCENT_OVERRIDE:-}" ]]; then printf '%s\n' "$WORK_DISK_USED_PERCENT_OVERRIDE"; else work_disk_used_percent; fi; }
used=$(used_now); (( used >= FAILED_CACHE_CLEAN_TRIGGER_PERCENT )) || exit 0
candidates=$(mktemp); trap 'rm -f "$candidates"' EXIT
while IFS=$'\t' read -r sample status wave; do
 safe_sample_id "$sample" || { log "Skipping unsafe sample ID: $sample"; continue; }
 marker="${PIPELINE_WORK_ROOT}/.sample_state/${sample}.failure.tsv"; [[ -s "$marker" ]] || continue
 epoch=$(marker_field "$marker" first_failure_epoch); if [[ ! "$epoch" =~ ^[0-9]+$ ]]; then epoch=$(stat -c %Y "$marker"); log "$sample uses legacy failure marker timestamp"; fi
 printf '%s\t%s\t%s\t%s\n' "$epoch" "$sample" "$status" "$wave"
done < <(awk -F '\t' 'NR>1&&$4~/^(PIPELINE_DEFERRED_FAILED|PIPELINE_DEFERRED_RETRY)$/{print $1"\t"$4"\t"$6}' "$STATUS_FILE") | sort -t $'\t' -k1,1n -k2,2 > "$candidates"
while IFS=$'\t' read -r epoch sample status wave; do
 used=$(used_now); (( used >= FAILED_CACHE_CLEAN_TARGET_PERCENT )) || break
 work=$(sample_work_root "$sample"); root=$(realpath -m "$PIPELINE_WORK_ROOT"); target=$(realpath -m "$work")
 [[ "$root" != / && "$target" == "$root/$sample" && "$target" != "$root" && -d "$target" ]] || continue
 [[ -f "${MANAGER_ROOT}/state/failure_diagnostics/samples/${sample}/ARCHIVE_COMPLETE" ]] || continue
 # Output validation wins over a stale failure marker.
 awk -F '\t' -v s="$sample" 'NR>1&&$1==s&&$8==1{ok=1}END{exit !ok}' "$VALIDATION_FILE" && continue
 state=$(sample_array_state "$sample"); slurm_state_is_active "$state" && continue
 [[ ! -s "${PIPELINE_WORK_ROOT}/.sample_state/${sample}.running.tsv" ]] || continue
 [[ ! -s "${PIPELINE_WORK_ROOT}/.sample_state/${sample}.requeue.tsv" ]] || continue
 [[ "$(marker_field "${PIPELINE_WORK_ROOT}/.sample_state/${sample}.failure.tsv" worker_state)" != IMMEDIATE_RETRY ]] || continue
 # The lock file is persistent metadata; only acquisition determines activity.
 exec {lock_fd}>"${PIPELINE_WORK_ROOT}/.locks/${sample}.lock"; flock -n "$lock_fd" || { exec {lock_fd}>&-; continue; }
 state2=$(sample_array_state "$sample"); if slurm_state_is_active "$state2"; then flock -u "$lock_fd"; exec {lock_fd}>&-; continue; fi
 bytes=$(du -sb "$target" 2>/dev/null | awk '{print $1+0}'); deleted=$(now_iso)
 if [[ "$DRY_RUN" == 0 ]]; then rm -rf --one-file-system -- "$target"; fi
 receipt="${MANAGER_ROOT}/state/receipts/failed_sample_work_cleanup/${sample}.$(date -u +%Y%m%dT%H%M%SZ).tsv"
 { printf 'sample_id\tfirst_failure_epoch\tcache_status\tcache_deleted_at\tcache_bytes_released\tretry_mode\twork_root\n'; printf '%s\t%s\tDELETED\t%s\t%s\tfresh\t%s\n' "$sample" "$epoch" "$deleted" "${bytes:-0}" "$target"; } > "$receipt"
 with_state_lock update_sample_fields "$sample" "notes=failed sample work cache deleted under disk pressure; future deferred retry will run fresh"
 flock -u "$lock_fd"; exec {lock_fd}>&-
 log "Deleted failed sample cache $sample (${bytes:-0} bytes); filesystem was ${used}%"
done < "$candidates"
