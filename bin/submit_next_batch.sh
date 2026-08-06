#!/usr/bin/env bash
# Backward-compatible name: this submits one manager wave, not one pipeline batch.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files; validate_config
[[ "$ENABLE_PIPELINE_SUBMIT" == 1 ]] || { log "ENABLE_PIPELINE_SUBMIT=0"; exit 0; }
(( $(active_wave_count) < MAX_ACTIVE_PIPELINE_WAVES )) || { log "Maximum active manager waves reached"; exit 0; }
determine_manager_phase; phase=$(manager_phase)
[[ "$phase" != PAUSED_DISK_PRESSURE ]] || { log "Pipeline submission paused by work filesystem pressure"; exit 0; }
used=$(disk_used_percent); dirs=$(local_sample_dir_count)
(( used < STOP_SUBMIT_PERCENT )) || { log "Results disk ${used}% >= ${STOP_SUBMIT_PERCENT}%"; exit 0; }
(( dirs < MAX_LOCAL_SAMPLE_DIRS )) || { log "Local sample dirs ${dirs} >= ${MAX_LOCAL_SAMPLE_DIRS}"; exit 0; }
eligible=PENDING; [[ "$phase" == DEFERRED_RETRY ]] && eligible=PIPELINE_DEFERRED_RETRY
mapfile -t samples < <(awk -F '\t' -v s="$eligible" 'NR>1&&$4==s{print $1}' "$STATUS_FILE" | head -n "$PIPELINE_WAVE_SIZE")
((${#samples[@]})) || { log "No eligible samples"; exit 0; }
seq_file="${MANAGER_ROOT}/state/wave_sequence"; exec 7>"${MANAGER_ROOT}/state/locks/wave_id.lock"; flock -x 7
seq=0; [[ -s "$seq_file" ]] && read -r seq < "$seq_file"; seq=$((seq+1)); printf '%s\n' "$seq" > "${seq_file}.tmp.$$"; mv "${seq_file}.tmp.$$" "$seq_file"; flock -u 7
wave_id=$(printf 'wave_%s_%s_%06d' "$(date -u +%Y%m%dT%H%M%SZ)" "$HPC_NAME" "$seq")
wave_file="${MANAGER_ROOT}/manifests/pipeline_waves/${wave_id}.samples.tsv"; submit_log="${MANAGER_ROOT}/logs/${wave_id}.submit.log"
for sample in "${samples[@]}"; do printf '%s\t%s\n' "$sample" "$(sample_species "$sample")"; done > "$wave_file"
manifest_sha=$(file_sha256 "$wave_file"); config_sha=$(file_sha256 "$PIPELINE_CONFIG"); git_commit=$(git_commit_or_unknown)
retry_mode=fresh; retry_of_wave_id=""; original_wave_id="$wave_id"; original_work_root=""; manifest_match=0; config_match=0; git_match=0
if [[ "$phase" == NORMAL && "$ENABLE_INFRASTRUCTURE_RESUME" == 1 ]] && [[ $(awk -F '\t' 'NR>1&&$4=="PIPELINE_RETRY_READY"{n++}END{print n+0}' "$STATUS_FILE") -gt 0 ]]; then
    old_wave=$(latest_failed_wave_for_samples "$wave_file")
    if [[ -n "$old_wave" ]]; then
        old_manifest=$(wave_field "$old_wave" sample_manifest); original_work_root=$(wave_field "$old_wave" work_root)
        old_config_sha=$(wave_field "$old_wave" pipeline_config_sha256); old_git=$(wave_field "$old_wave" pipeline_git_commit)
        old_manifest_sha=$(wave_field "$old_wave" pipeline_manifest_sha256); old_failure=$(wave_field "$old_wave" failure_class); old_resume=$(wave_field "$old_wave" resume_eligible)
        [[ "$old_manifest_sha" == "$manifest_sha" ]] && manifest_match=1
        [[ "$old_config_sha" == "$config_sha" ]] && config_match=1
        [[ "$old_git" == "$git_commit" ]] && git_match=1
        fingerprints_ok=0; [[ "$manifest_match" == 1 && "$config_match" == 1 && "$git_match" == 1 ]] && fingerprints_ok=1
        if [[ "$old_failure" == INFRASTRUCTURE && "$old_resume" == 1 && -n "$original_work_root" && -d "$original_work_root" && ( "$REQUIRE_RESUME_FINGERPRINT_MATCH" == 0 || "$fingerprints_ok" == 1 ) ]] && ! wave_is_active "$old_wave" && ! work_root_resume_in_use "$original_work_root"; then
            exec 8>"${original_work_root}/.manager_resume.lock"
            if flock -n 8; then retry_mode=resume; retry_of_wave_id="$old_wave"; original_wave_id=$(wave_field "$old_wave" original_wave_id); [[ -n "$original_wave_id" ]] || original_wave_id="$old_wave"; else log "Resume lock busy for $original_work_root; retry not submitted"; exit 0; fi
        fi
    fi
fi
work="${PIPELINE_WORK_ROOT}/${wave_id}"; [[ "$retry_mode" == resume ]] && work="$original_work_root"
work_layout=WAVE_ROOT; batch_lists="${work}/batch_lists"
if [[ "$PIPELINE_MODE" == streaming_per_sample ]]; then
    # A wave is only immutable scheduling metadata.  Workers always derive their
    # persistent directory as NF_BASE_WORK_DIR/sample_id.
    work="$PIPELINE_WORK_ROOT"; work_layout=PER_SAMPLE; batch_lists=""
    for sample in "${samples[@]}"; do safe_sample_id "$sample" || die "unsafe sample ID: $sample"; done
else
    mkdir -p "$batch_lists"
fi
log "INFO: retry_mode=$retry_mode"; log "INFO: retry_of_wave_id=$retry_of_wave_id"; log "INFO: original_work_root=$original_work_root"; log "INFO: selected_work_root=$work"; log "INFO: manifest_checksum_match=$manifest_match"; log "INFO: config_checksum_match=$config_match"; log "INFO: git_commit_match=$git_match"
if [[ "$PIPELINE_MODE" == streaming_per_sample ]]; then
 command=(env "FULL_SAMPLE_LIST=$wave_file" "PRE_OUTPUT_DIR=$LOCAL_RESULTS" "ROUND_OUTPUT_DIR=$LOCAL_RESULTS" "ROUND1_OUTDIR=$LOCAL_RESULTS" "NF_BASE_WORK_DIR=$PIPELINE_WORK_ROOT" "MAX_CONCURRENT=$SAMPLE_CHAIN_CONCURRENCY" "IMMEDIATE_SAMPLE_RETRIES=$IMMEDIATE_SAMPLE_RETRIES" "IMMEDIATE_RETRY_DELAY_SECONDS=$IMMEDIATE_RETRY_DELAY_SECONDS" "CLEAN_VALIDATED_STAGE_WORK=$CLEAN_VALIDATED_STAGE_WORK" "REMOVE_SAMPLE_ROOT_ON_SUCCESS=$REMOVE_SAMPLE_ROOT_ON_SUCCESS" "NF_CONFIG_FILE=$PIPELINE_CONFIG" "NEXTFLOW_MODULE=${NEXTFLOW_MODULE:-}" "STREAM_PARTITION=$STREAM_PARTITION" "GLOBAL_REF_DIR=${GLOBAL_REF_DIR:-}" "REF_DIR=${REF_DIR:-}" "NUCLEAR_ONLY_REF_DIR=${NUCLEAR_ONLY_REF_DIR:-}" "ENABLE_CHUNKED_ALIGNMENT=$ENABLE_CHUNKED_ALIGNMENT" bash "$PIPELINE_LAUNCHER")
else
 command=(env "FULL_SAMPLE_LIST=$wave_file" "PRE_OUTPUT_DIR=$LOCAL_RESULTS" "ROUND_OUTPUT_DIR=$LOCAL_RESULTS" "ROUND1_OUTDIR=$LOCAL_RESULTS" "NF_BASE_WORK_DIR=$work" "PIPELINE_RESUME=$([[ "$retry_mode" == resume ]] && echo 1 || echo 0)" "RETRY_OF_WAVE_ID=$retry_of_wave_id" "ORIGINAL_WAVE_ID=$original_wave_id" "BATCH_LIST_DIR=$batch_lists" "BATCH_SIZE=$PIPELINE_BATCH_SIZE" "CHAIN_CONCURRENT_BATCHES=$CHAIN_CONCURRENT_BATCHES" "NUMT_CONCURRENT=$NUMT_CONCURRENT" "CLEAN_ON_SUCCESS=$CLEAN_ON_SUCCESS" "ENABLE_CHUNKED_ALIGNMENT=$ENABLE_CHUNKED_ALIGNMENT" "NF_CONFIG_FILE=$PIPELINE_CONFIG" "NEXTFLOW_MODULE=${NEXTFLOW_MODULE:-}" bash "$PIPELINE_LAUNCHER")
fi
created=$(now_iso); batch_sha=unknown
layout_column=""; [[ "$PIPELINE_MODE" == streaming_per_sample ]] && layout_column=$'\t'"$work_layout"
if [[ "$DRY_RUN" == 1 ]]; then
    printf 'DRY RUN: '; printf '%q ' "${command[@]}"; printf '\n'
    with_state_lock append_wave_row "$wave_id\t$wave_file\t${#samples[@]}\t\t$created\tDRY_RUN\t0\t0\tCANCELLED\t$created\tdry run; phase=$phase; launcher not invoked\t$work\t$retry_of_wave_id\t$original_wave_id\t\t0\t$manifest_sha\t$config_sha\t$git_commit\t$batch_lists\t$batch_sha$layout_column"
    exit 0
fi
with_state_lock append_wave_row "$wave_id\t$wave_file\t${#samples[@]}\t\t$created\t\t0\t0\tCREATED\t$created\tlauncher starting; phase=$phase\t$work\t$retry_of_wave_id\t$original_wave_id\t\t0\t$manifest_sha\t$config_sha\t$git_commit\t$batch_lists\t$batch_sha$layout_column"
set +e; "${command[@]}" > >(tee "$submit_log") 2>&1; rc=$?; set -e
job_id=$(sed -n 's/.*Submitted batch job \([0-9][0-9]*\).*/\1/p' "$submit_log" | tail -n 1)
if ((rc!=0)) || [[ -z "$job_id" ]]; then
    note="launcher failed rc=$rc${job_id:+ job=$job_id}; see $submit_log"
    with_state_lock update_wave_row "$wave_id" "status=FAILED" "slurm_state=SUBMIT_FAILED" "notes=$note"
    log "$note"; exit 1
fi
if [[ "$PIPELINE_MODE" == streaming_per_sample ]]; then
 map_file="${MANAGER_ROOT}/state/array_sample_map/${wave_id}.tsv"
 { printf 'wave_id\tpipeline_job_id\tarray_task_id\tsample_id\treference_name\tsample_work_root\n'; i=0; while IFS=$'\t' read -r sample reference; do printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$wave_id" "$job_id" "$i" "$sample" "$reference" "$(sample_work_root "$sample")"; i=$((i+1)); done < "$wave_file"; } > "${map_file}.tmp.$$"
 mv "${map_file}.tmp.$$" "$map_file"; chmod a-w "$map_file" 2>/dev/null || true
fi
finalize_wave() {
    update_wave_row "$wave_id" "pipeline_job_id=$job_id" "status=SUBMITTED" "slurm_state=PENDING" "notes=submitted; phase=$phase"
    local s attempts previous next_status
    for s in "${samples[@]}"; do previous=$(awk -F '\t' -v x="$s" 'NR>1&&$1==x{print $4}' "$STATUS_FILE"); next_status=WAVE_SUBMITTED; [[ "$previous" == PIPELINE_DEFERRED_RETRY ]] && next_status=PIPELINE_DEFERRED_RUNNING; attempts=$(awk -F '\t' -v x="$s" 'NR>1&&$1==x{print $7+1}' "$STATUS_FILE"); update_sample_fields "$s" "status=$next_status" "slurm_job_id=$job_id" "wave_id=$wave_id" "pipeline_attempts=$attempts" "last_pipeline_error=" "notes=manager wave submitted; phase=$phase"; done
}
with_state_lock finalize_wave
log "Submitted wave $wave_id (${#samples[@]} samples) as Slurm job $job_id"
