#!/usr/bin/env bash
# Backward-compatible name: this submits one manager wave, not one pipeline batch.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files; validate_config
[[ "$ENABLE_PIPELINE_SUBMIT" == 1 ]] || { log "ENABLE_PIPELINE_SUBMIT=0"; exit 0; }
(( $(active_wave_count) < MAX_ACTIVE_PIPELINE_WAVES )) || { log "Maximum active manager waves reached"; exit 0; }
used=$(disk_used_percent); dirs=$(local_sample_dir_count)
(( used < STOP_SUBMIT_PERCENT )) || { log "Disk ${used}% >= ${STOP_SUBMIT_PERCENT}%"; exit 0; }
(( dirs < MAX_LOCAL_SAMPLE_DIRS )) || { log "Local sample dirs ${dirs} >= ${MAX_LOCAL_SAMPLE_DIRS}"; exit 0; }
mapfile -t samples < <(awk -F '\t' -v max="$MAX_PIPELINE_RETRIES" 'NR>1 && ($4=="PENDING" || ($4=="PIPELINE_RETRY_READY" && $7<max)) {print $1}' "$STATUS_FILE" | head -n "$PIPELINE_WAVE_SIZE")
((${#samples[@]})) || { log "No eligible samples"; exit 0; }
seq_file="${MANAGER_ROOT}/state/wave_sequence"; exec 7>"${MANAGER_ROOT}/state/locks/wave_id.lock"; flock -x 7
seq=0; [[ -s "$seq_file" ]] && read -r seq < "$seq_file"; seq=$((seq+1)); printf '%s\n' "$seq" > "${seq_file}.tmp.$$"; mv "${seq_file}.tmp.$$" "$seq_file"; flock -u 7
wave_id=$(printf 'wave_%s_%s_%06d' "$(date -u +%Y%m%dT%H%M%SZ)" "$HPC_NAME" "$seq")
wave_file="${MANAGER_ROOT}/manifests/pipeline_waves/${wave_id}.samples.tsv"; submit_log="${MANAGER_ROOT}/logs/${wave_id}.submit.log"
for sample in "${samples[@]}"; do printf '%s\t%s\n' "$sample" "$(sample_species "$sample")"; done > "$wave_file"
work="${PIPELINE_WORK_ROOT}/${wave_id}"; batch_lists="${work}/batch_lists"; mkdir -p "$batch_lists"
command=(env "FULL_SAMPLE_LIST=$wave_file" "PRE_OUTPUT_DIR=$LOCAL_RESULTS" "ROUND_OUTPUT_DIR=$LOCAL_RESULTS" "ROUND1_OUTDIR=$LOCAL_RESULTS" "NF_BASE_WORK_DIR=$work" "BATCH_LIST_DIR=$batch_lists" "BATCH_SIZE=$PIPELINE_BATCH_SIZE" "CHAIN_CONCURRENT_BATCHES=$CHAIN_CONCURRENT_BATCHES" "NUMT_CONCURRENT=$NUMT_CONCURRENT" "CLEAN_ON_SUCCESS=$CLEAN_ON_SUCCESS" "ENABLE_CHUNKED_ALIGNMENT=$ENABLE_CHUNKED_ALIGNMENT" "NF_CONFIG_FILE=$PIPELINE_CONFIG" bash "$PIPELINE_LAUNCHER")
created=$(now_iso)
if [[ "$DRY_RUN" == 1 ]]; then
    printf 'DRY RUN: '; printf '%q ' "${command[@]}"; printf '\n'
    with_state_lock append_wave_row "$wave_id\t$wave_file\t${#samples[@]}\t\t\tDRY_RUN\t0\t0\tCANCELLED\t$created\tdry run; launcher not invoked"
    exit 0
fi
with_state_lock append_wave_row "$wave_id\t$wave_file\t${#samples[@]}\t\t$created\t\t0\t0\tCREATED\t$created\tlauncher starting"
set +e; "${command[@]}" > >(tee "$submit_log") 2>&1; rc=$?; set -e
job_id=$(sed -n 's/.*Submitted batch job \([0-9][0-9]*\).*/\1/p' "$submit_log" | tail -n 1)
if ((rc!=0)) || [[ -z "$job_id" ]]; then
    note="launcher failed rc=$rc${job_id:+ job=$job_id}; see $submit_log"
    with_state_lock update_wave_row "$wave_id" "status=FAILED" "slurm_state=SUBMIT_FAILED" "notes=$note"
    log "$note"; exit 1
fi
finalize_wave() {
    update_wave_row "$wave_id" "pipeline_job_id=$job_id" "status=SUBMITTED" "slurm_state=PENDING" "notes=submitted"
    local s attempts previous next_status
    for s in "${samples[@]}"; do previous=$(awk -F '\t' -v x="$s" 'NR>1&&$1==x{print $4}' "$STATUS_FILE"); next_status=WAVE_SUBMITTED; [[ "$previous" == PIPELINE_RETRY_READY ]] && next_status=PIPELINE_RETRY_RUNNING; attempts=$(awk -F '\t' -v x="$s" 'NR>1&&$1==x{print $7+1}' "$STATUS_FILE"); update_sample_fields "$s" "status=$next_status" "slurm_job_id=$job_id" "wave_id=$wave_id" "pipeline_attempts=$attempts" "last_pipeline_error=" "notes=manager wave submitted"; done
}
with_state_lock finalize_wave
log "Submitted wave $wave_id (${#samples[@]} samples) as Slurm job $job_id"
