#!/usr/bin/env bash
# Compatibility entry point for task-native submission scheduling.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; validate_config; ensure_state_files
[[ "$ENABLE_PIPELINE_SUBMIT" == 1 ]] || { log "ENABLE_PIPELINE_SUBMIT=0"; exit 0; }
# A single live submission owns the selected mode's global Slurm throttle.
(( $(active_submission_count) == 0 )) || { log "An active submission already owns the global concurrency budget"; exit 0; }
determine_manager_phase; phase=$(manager_phase)
[[ "$phase" != PAUSED_DISK_PRESSURE ]] || { log "Pipeline submission paused by work filesystem pressure"; exit 0; }
work_used=$(work_disk_used_percent)
(( work_used < WORK_STOP_SUBMIT_PERCENT )) || { log "Work disk stop-submit threshold reached (${work_used}% >= ${WORK_STOP_SUBMIT_PERCENT}%); existing work is not paused"; exit 0; }
(( $(disk_used_percent) < STOP_SUBMIT_PERCENT )) || { log "Results disk threshold reached"; exit 0; }
(( $(local_sample_dir_count) < MAX_LOCAL_SAMPLE_DIRS )) || { log "Local sample directory limit reached"; exit 0; }
eligible=PENDING; [[ "$phase" == DEFERRED_RETRY ]] && eligible=PIPELINE_DEFERRED_RETRY
# Eligibility is both an explicit runnable status and current cohort membership.
# This deliberately makes terminal OUT_OF_SCOPE rows impossible to submit.
mapfile -t samples < <(awk -F '\t' -v s="$eligible" 'NR==FNR{if(NF)a[$1]=1;next}NR>FNR&&FNR>1&&$4==s&&($1 in a){print $1}' "$ASSIGNED_SAMPLE_LIST" "$STATUS_FILE")
((${#samples[@]})) || { log "No eligible samples"; exit 0; }
required=${#samples[@]}; [[ "$PIPELINE_MODE" == batch ]] && required=$(( (${#samples[@]} + PIPELINE_BATCH_SIZE - 1) / PIPELINE_BATCH_SIZE ))
if command -v scontrol >/dev/null 2>&1; then
  max=$(scontrol show config 2>/dev/null | parse_slurm_max_array_size)
  if [[ "$max" =~ ^[0-9]+$ ]]; then
    log "INFO: slurm_max_array_size=$max"
    (( required < max )) || die "required array tasks ($required) exceed Slurm MaxArraySize=$max; no wave fallback was used"
  else
    log "WARNING: unable to parse a numeric Slurm MaxArraySize; array-size validation skipped"
  fi
fi
log "INFO: required_array_tasks=$required"
seq_file="${MANAGER_ROOT}/state/submission_sequence"; exec 7>"${MANAGER_ROOT}/state/locks/submission_id.lock"; flock -x 7
seq=0; [[ -s "$seq_file" ]] && read -r seq < "$seq_file"; seq=$((seq+1)); printf '%s\n' "$seq" > "$seq_file"; flock -u 7
submission_id=$(printf '%s_%s_%s_%06d' "$([[ "$PIPELINE_MODE" == batch ]] && echo batch || echo stream)" "$(date -u +%Y%m%dT%H%M%SZ)" "$HPC_NAME" "$seq")
manifest="${MANAGER_ROOT}/manifests/submissions/${submission_id}.samples.tsv"
for sample in "${samples[@]}"; do printf '%s\t%s\n' "$sample" "$(sample_species "$sample")"; done > "$manifest"
chmod a-w "$manifest" 2>/dev/null || true
work="$PIPELINE_WORK_ROOT"; batch_lists=""; work_layout=PER_SAMPLE
if [[ "$PIPELINE_MODE" == batch ]]; then work="${PIPELINE_WORK_ROOT}/submissions/${submission_id}"; batch_lists="$work/batch_lists"; work_layout=SUBMISSION_ROOT; mkdir -p "$batch_lists" "$work/batch_status"; fi
log_file="${MANAGER_ROOT}/logs/${submission_id}.submit.log"
common=("FULL_SAMPLE_LIST=$manifest" "PRE_OUTPUT_DIR=$LOCAL_RESULTS" "ROUND_OUTPUT_DIR=$LOCAL_RESULTS" "ROUND1_OUTDIR=$LOCAL_RESULTS" "NF_BASE_WORK_DIR=$work" "NF_CONFIG_FILE=$PIPELINE_CONFIG" "NEXTFLOW_MODULE=${NEXTFLOW_MODULE:-}")
if [[ "$PIPELINE_MODE" == streaming_per_sample ]]; then
 repo=$(realpath -m "$PIPELINE_REPO")
 log "INFO: pipeline_repo_dir=$repo"
 command=(env "${common[@]}" "MAX_CONCURRENT=$SAMPLE_CHAIN_CONCURRENCY" "PIPELINE_REPO_DIR=$repo" "STREAM_SMOKE_TEST=$STREAM_SMOKE_TEST" "IMMEDIATE_SAMPLE_RETRIES=$IMMEDIATE_SAMPLE_RETRIES" "IMMEDIATE_RETRY_DELAY_SECONDS=$IMMEDIATE_RETRY_DELAY_SECONDS" "CLEAN_VALIDATED_STAGE_WORK=$CLEAN_VALIDATED_STAGE_WORK" "REMOVE_SAMPLE_ROOT_ON_SUCCESS=$REMOVE_SAMPLE_ROOT_ON_SUCCESS" "SAMTOOLS_MODULE=${SAMTOOLS_MODULE:-}" "STREAM_PARTITION=$STREAM_PARTITION" "GLOBAL_REF_DIR=${GLOBAL_REF_DIR:-}" "REF_DIR=${REF_DIR:-}" "NUCLEAR_ONLY_REF_DIR=${NUCLEAR_ONLY_REF_DIR:-}" "ENABLE_CHUNKED_ALIGNMENT=$ENABLE_CHUNKED_ALIGNMENT" bash "$PIPELINE_LAUNCHER")
else
 command=(env "${common[@]}" "BATCH_LIST_DIR=$batch_lists" "BATCH_SIZE=$PIPELINE_BATCH_SIZE" "CHAIN_CONCURRENT_BATCHES=$CHAIN_CONCURRENT_BATCHES" "NUMT_CONCURRENT=$NUMT_CONCURRENT" "CLEAN_ON_SUCCESS=$CLEAN_ON_SUCCESS" "ENABLE_CHUNKED_ALIGNMENT=$ENABLE_CHUNKED_ALIGNMENT" bash "$PIPELINE_LAUNCHER")
fi
created=$(now_iso); sha=$(file_sha256 "$manifest"); cfgsha=$(file_sha256 "$PIPELINE_CONFIG"); gitsha=$(git_commit_or_unknown)
with_state_lock append_wave_row "$submission_id\t$manifest\t${#samples[@]}\t\t$created\t\t0\t0\tCREATED\t$created\tsubmission audit record; phase=$phase\t$work\t\t$submission_id\t\t0\t$sha\t$cfgsha\t$gitsha\t$batch_lists\tunknown\t$work_layout"
if [[ "$DRY_RUN" == 1 ]]; then printf 'DRY RUN: '; printf '%q ' "${command[@]}"; printf '\n'; with_state_lock update_wave_row "$submission_id" status=CANCELLED notes="dry run; phase=$phase"; exit 0; fi
set +e; "${command[@]}" > >(tee "$log_file") 2>&1; rc=$?; set -e
job_id=$(sed -n 's/.*Submitted batch job \([0-9][0-9]*\).*/\1/p' "$log_file" | tail -1)
(( rc == 0 )) && [[ -n "$job_id" ]] || { with_state_lock update_wave_row "$submission_id" status=FAILED slurm_state=SUBMIT_FAILED; die "launcher failed rc=$rc"; }
map="${MANAGER_ROOT}/state/submission_task_map/${submission_id}.tsv"; tmp="$map.tmp.$$"
printf 'submission_id\tpipeline_mode\tphase\tslurm_array_job_id\tarray_task_id\ttask_type\ttask_name\tsample_id\treference_name\tsample_work_root\tbatch_work_root\n' > "$tmp"
if [[ "$PIPELINE_MODE" == streaming_per_sample ]]; then
 i=1; while IFS=$'\t' read -r sample ref; do printf '%s\t%s\t%s\t%s\t%s\tSAMPLE\t%s\t%s\t%s\t%s\t\n' "$submission_id" "$PIPELINE_MODE" "$phase" "$job_id" "$i" "$sample" "$sample" "$ref" "$(sample_work_root "$sample")" >> "$tmp"; i=$((i+1)); done < "$manifest"
else
 # The launcher's exact, lexically sorted batch files are authoritative.
 mapfile -t files < <(find "$batch_lists" -maxdepth 1 -type f -name 'sample_batch_*' -print | LC_ALL=C sort)
 if ((${#files[@]}==0)); then log "WARNING: legacy launcher produced no batch files; creating compatibility files"; split -d -a 3 -l "$PIPELINE_BATCH_SIZE" "$manifest" "$batch_lists/sample_batch_"; mapfile -t files < <(find "$batch_lists" -type f -name 'sample_batch_*' -print | LC_ALL=C sort); fi
 task=0; for file in "${files[@]}"; do name=${file##*/}; while IFS=$'\t' read -r sample ref; do [[ -n "$sample" ]] || continue; printf '%s\t%s\t%s\t%s\t%s\tBATCH\t%s\t%s\t%s\t\t%s\n' "$submission_id" "$PIPELINE_MODE" "$phase" "$job_id" "$task" "$name" "$sample" "$ref" "$work" >> "$tmp"; done < "$file"; task=$((task+1)); done
 (( task == required )) || die "launcher batch mapping count $task differs from expected $required"
fi
mv "$tmp" "$map"; chmod a-w "$map" 2>/dev/null || true
# Keep the old streaming map readable for safe migration tooling.
if [[ "$PIPELINE_MODE" == streaming_per_sample ]]; then awk -F '\t' 'BEGIN{OFS="\t";print "submission_id","array_job_id","array_task_id","sample_id","reference_name","sample_work_root","phase"} NR>1{print $1,$4,$5,$8,$9,$10,$3}' "$map" > "${MANAGER_ROOT}/state/array_sample_map/${submission_id}.tsv"; fi
finalize(){ update_wave_row "$submission_id" "pipeline_job_id=$job_id" status=SUBMITTED slurm_state=PENDING; local s attempts old next; for s in "${samples[@]}"; do old=$(awk -F '\t' -v x="$s" 'NR>1&&$1==x{print $4}' "$STATUS_FILE"); attempts=$(awk -F '\t' -v x="$s" 'NR>1&&$1==x{print $7+1}' "$STATUS_FILE"); next=PIPELINE_SUBMITTED; [[ "$old" == PIPELINE_DEFERRED_RETRY ]] && next=PIPELINE_DEFERRED_RUNNING; update_sample_fields "$s" "status=$next" "slurm_job_id=$job_id" "wave_id=$submission_id" "pipeline_attempts=$attempts" "last_pipeline_error=" "notes=task-native submission; phase=$phase"; done; }
with_state_lock finalize
log "Submitted $submission_id (${#samples[@]} samples, $required tasks) as Slurm array $job_id"
