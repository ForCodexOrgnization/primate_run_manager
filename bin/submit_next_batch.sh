#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"
ensure_state_files

used=$(disk_used_percent)
local_dirs=$(local_sample_dir_count)
if (( used >= STOP_SUBMIT_PERCENT )); then log "Disk ${used}% >= ${STOP_SUBMIT_PERCENT}%; not submitting"; exit 0; fi
if (( local_dirs >= MAX_LOCAL_SAMPLE_DIRS )); then log "Local sample dirs ${local_dirs} >= ${MAX_LOCAL_SAMPLE_DIRS}; not submitting"; exit 0; fi

mapfile -t samples < <(get_samples_by_status '^PENDING$' | head -n "$PIPELINE_BATCH_SIZE")
((${#samples[@]})) || { log "No PENDING samples"; exit 0; }

batch_id="pipeline_$(date -u +%Y%m%dT%H%M%SZ)_${HPC_NAME}"
batch_file="${MANAGER_ROOT}/manifests/pipeline_batches/${batch_id}.txt"
: > "$batch_file"
for sample in "${samples[@]}"; do printf '%s\t%s\n' "$sample" "$(sample_species "$sample")" >> "$batch_file"; done

module load "$NEXTFLOW_MODULE" || true
submit_log="${MANAGER_ROOT}/logs/${batch_id}.submit.log"
set +e
FULL_SAMPLE_LIST="$batch_file" \
MAX_CONCURRENT="$PIPELINE_MAX_CONCURRENT" \
NF_BASE_WORK_DIR="$NF_BASE_WORK_DIR" \
PRE_OUTPUT_DIR="$LOCAL_RESULTS" \
ROUND_OUTPUT_DIR="$LOCAL_RESULTS" \
ROUND1_OUTDIR="$LOCAL_RESULTS" \
NF_CONFIG_FILE="$PIPELINE_CONFIG" \
CLEAN_ON_SUCCESS=1 \
bash "$PIPELINE_SCRIPT" 2>&1 | tee "$submit_log"
rc=${PIPESTATUS[0]}
set -e
((rc==0)) || die "Pipeline launcher failed; see $submit_log"
job_id=$(sed -n 's/.*Submitted batch job \([0-9][0-9]*\).*/\1/p' "$submit_log" | tail -n1)
[[ -n "$job_id" ]] || die "Could not parse Slurm job ID from $submit_log"
for sample in "${samples[@]}"; do with_state_lock update_sample_row "$sample" SUBMITTED "$job_id" "$batch_id" "" "" "streaming array submitted"; done
log "Submitted ${#samples[@]} samples as job $job_id"
