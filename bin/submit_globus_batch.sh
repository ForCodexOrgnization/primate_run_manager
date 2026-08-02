#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ "$ENABLE_TRANSFER" == 1 ]] || { log "ENABLE_TRANSFER=0"; exit 0; }
[[ "$DRY_RUN" == 1 ]] || command -v globus >/dev/null || die "globus CLI not found"
used=$(disk_used_percent); mapfile -t samples < <(get_samples_by_status '^READY_TO_TRANSFER$' | head -n "$TRANSFER_BATCH_SIZE")
((${#samples[@]})) || { log "No READY_TO_TRANSFER samples"; exit 0; }
if ((${#samples[@]}<TRANSFER_BATCH_SIZE && used<FORCE_TRANSFER_PERCENT)); then log "Waiting for transfer batch or disk threshold"; exit 0; fi
batch_id="transfer_$(date -u +%Y%m%dT%H%M%SZ)_${HPC_NAME}_$$"; manifest="${MANAGER_ROOT}/manifests/transfer_batches/${batch_id}.batch"; sample_file="${manifest%.batch}.samples"
: > "$manifest"; printf '%s\n' "${samples[@]}" > "$sample_file"; for s in "${samples[@]}"; do printf '%s/ %s/ --recursive\n' "$s" "$s" >> "$manifest"; done
cmd=(globus transfer "${SOURCE_COLLECTION}:${SOURCE_ROOT}" "${DEST_COLLECTION}:${DEST_ROOT}" --batch "$manifest" --sync-level checksum --label "$batch_id" --format=UNIX --jmespath task_id)
if [[ "$DRY_RUN" == 1 ]]; then printf 'DRY RUN: '; printf '%q ' "${cmd[@]}"; printf '\n'; exit 0; fi
task_id=$("${cmd[@]}"); [[ -n "$task_id" ]] || die "Globus did not return task ID"
record_transfer() { local tmp="${TRANSFER_TASK_FILE}.tmp.$$"; cat "$TRANSFER_TASK_FILE" > "$tmp"; printf '%s\t%s\tACTIVE\t%s\t%s\t%s\tsubmitted\n' "$batch_id" "$task_id" "$sample_file" "$(now_iso)" "$(now_iso)" >> "$tmp"; mv "$tmp" "$TRANSFER_TASK_FILE"; for s in "${samples[@]}"; do update_sample_fields "$s" "status=TRANSFERRING" "globus_task_id=$task_id" "workspace_path=${DEST_ROOT%/}/$s/" "transfer_status=ACTIVE" "notes=full sample directory transfer submitted"; done; }
with_state_lock record_transfer
