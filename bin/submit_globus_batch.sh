#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"
ensure_state_files
[[ "$ENABLE_TRANSFER" == 1 ]] || { log "ENABLE_TRANSFER=0"; exit 0; }
command -v globus >/dev/null || die "globus CLI not found"

used=$(disk_used_percent)
mapfile -t samples < <(get_samples_by_status '^READY_TO_TRANSFER$' | head -n "$TRANSFER_BATCH_SIZE")
((${#samples[@]})) || { log "No READY_TO_TRANSFER samples"; exit 0; }
if (( ${#samples[@]} < TRANSFER_BATCH_SIZE && used < FORCE_TRANSFER_PERCENT )); then
    log "Only ${#samples[@]} ready and disk ${used}% < force threshold ${FORCE_TRANSFER_PERCENT}%; waiting"
    exit 0
fi

batch_id="transfer_$(date -u +%Y%m%dT%H%M%SZ)_${HPC_NAME}"
batch_file="${MANAGER_ROOT}/manifests/transfer_batches/${batch_id}.batch"
sample_file="${MANAGER_ROOT}/manifests/transfer_batches/${batch_id}.samples"
: > "$batch_file"; : > "$sample_file"
for sample in "${samples[@]}"; do
    printf '%s/ %s/ --recursive\n' "$sample" "$sample" >> "$batch_file"
    printf '%s\n' "$sample" >> "$sample_file"
done

task_id=$(globus transfer \
    "${SOURCE_COLLECTION}:${SOURCE_ROOT}" \
    "${DEST_COLLECTION}:${DEST_ROOT}" \
    --batch "$batch_file" \
    --sync-level "$GLOBUS_SYNC_LEVEL" \
    --label "$batch_id" \
    --format=UNIX --jmespath task_id)
[[ -n "$task_id" ]] || die "Globus did not return task ID"
printf '%s\t%s\tACTIVE\t%s\t%s\t%s\tsubmitted\n' "$batch_id" "$task_id" "$sample_file" "$(now_iso)" "$(now_iso)" >> "$TRANSFER_TASK_FILE"
for sample in "${samples[@]}"; do
    with_state_lock update_sample_row "$sample" TRANSFERRING "" "" "$task_id" "${DEST_ROOT%/}/${sample}/" "full sample directory transfer submitted"
done
log "Submitted Globus batch $batch_id task $task_id (${#samples[@]} samples)"
