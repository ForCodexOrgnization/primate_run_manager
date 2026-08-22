#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ "$ENABLE_TRANSFER" == 1 ]] || { log "ENABLE_TRANSFER=0"; exit 0; }
path_marker="$MANAGER_ROOT/state/path_check.passed"
if [[ "${PATH_CHECK_REQUIRED:-1}" == 1 ]]; then
    [[ -f "$path_marker" ]] || die "Transfer blocked: required path check marker is absent; run bin/check_paths.sh"
    marker_local=$(awk -F= '$1=="LOCAL_RESULTS"{print substr($0,index($0,"=")+1); exit}' "$path_marker")
    marker_source=$(awk -F= '$1=="SOURCE_ROOT_LOCAL_VIEW"{print substr($0,index($0,"=")+1); exit}' "$path_marker")
    [[ -n "$marker_local" && -n "$marker_source" ]] || die "Transfer blocked: path check marker is stale or incomplete; run bin/check_paths.sh"
    [[ "$marker_local" == "$LOCAL_RESULTS" && "$marker_source" == "$SOURCE_ROOT_LOCAL_VIEW" ]] || die "Transfer blocked: path check marker does not match the current path configuration; run bin/check_paths.sh"
fi
active_tasks=$(awk -F '\t' 'NR>1&&$3=="ACTIVE"{n++}END{print n+0}' "$TRANSFER_TASK_FILE")
(( active_tasks < MAX_ACTIVE_TRANSFER_TASKS )) || { log "Maximum active Globus tasks reached ($active_tasks/$MAX_ACTIVE_TRANSFER_TASKS)"; exit 0; }
used=$(disk_used_percent); mapfile -t samples < <(get_samples_by_status_limit '^READY_TO_TRANSFER$' "$TRANSFER_BATCH_SIZE")
((${#samples[@]})) || { log "No READY_TO_TRANSFER samples"; exit 0; }
if ((${#samples[@]}<TRANSFER_BATCH_SIZE && used<FORCE_TRANSFER_PERCENT)); then log "Waiting for transfer batch or disk threshold"; exit 0; fi
for s in "${samples[@]}"; do
    [[ -d "$SOURCE_ROOT_LOCAL_VIEW/$s" ]] || die "Transfer blocked: sample is absent from SOURCE_ROOT_LOCAL_VIEW: $s"
done
batch_id="transfer_$(date -u +%Y%m%dT%H%M%SZ)_${HPC_NAME}_$$"; manifest="${MANAGER_ROOT}/manifests/transfer_batches/${batch_id}.batch"; sample_file="${manifest%.batch}.samples"
: > "$manifest"; : > "$sample_file"
for s in "${samples[@]}"; do
    printf '%s\n' "$s" >> "$sample_file"
    printf '%s/ %s/ --recursive\n' "$s" "$s" >> "$manifest"
done
cmd=(globus transfer "${SOURCE_COLLECTION}:${SOURCE_ROOT}" "${DEST_COLLECTION}:${DEST_ROOT}" --batch "$manifest" --sync-level "$GLOBUS_SYNC_LEVEL" --label "$batch_id" --format=UNIX --jmespath task_id)
if [[ "$DRY_RUN" == 1 ]]; then printf 'DRY RUN: '; printf '%q ' "${cmd[@]}"; printf '\n'; exit 0; fi
module_err=$(mktemp); globus_err=$(mktemp); trap 'rm -f "$module_err" "$globus_err"' EXIT
if ! (load_globus_module) 2>"$module_err"; then
  detail=$(cat "$module_err"); record_globus_health UNKNOWN transfer_submit 127 "$detail"; log "Globus health UNKNOWN: $detail"; exit 1
fi
set +e; task_id=$("${cmd[@]}" 2>"$globus_err"); rc=$?; set -e
if (( rc != 0 )) || [[ -z "$task_id" ]]; then
  health=UNKNOWN; (( rc == 4 )) && health=AUTH_REQUIRED
  detail=$(cat "$globus_err"); [[ -n "$task_id" ]] || detail="${detail}${detail:+; }no task ID returned"
  record_globus_health "$health" transfer_submit "$rc" "$detail"
  log "Globus transfer submission $health (rc=$rc): $detail"
  exit 1
fi
record_globus_health HEALTHY transfer_submit 0 "submitted task $task_id"
record_transfer() { local tmp="${TRANSFER_TASK_FILE}.tmp.$$"; cat "$TRANSFER_TASK_FILE" > "$tmp"; printf '%s\t%s\tACTIVE\t%s\t%s\t%s\tsubmitted\n' "$batch_id" "$task_id" "$sample_file" "$(now_iso)" "$(now_iso)" >> "$tmp"; mv "$tmp" "$TRANSFER_TASK_FILE"; for s in "${samples[@]}"; do update_sample_fields "$s" "status=TRANSFERRING" "globus_task_id=$task_id" "workspace_path=${DEST_ROOT%/}/$s/" "transfer_status=ACTIVE" "notes=full sample directory transfer submitted"; done; }
with_state_lock record_transfer
