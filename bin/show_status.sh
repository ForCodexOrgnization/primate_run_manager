#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files; determine_manager_phase
count_status(){ awk -F '\t' -v s="$1" 'NR>1&&$4==s{n++}END{print n+0}' "$STATUS_FILE"; }
printf 'Pipeline mode: %s\nManager phase: %s\n' "$PIPELINE_MODE" "$(manager_phase)"
printf 'Pending samples: %s\nReady to transfer: %s\nDeferred retry: %s\nDeferred failed: %s\nOUT_OF_SCOPE: %s\n' "$(count_status PENDING)" "$(count_status READY_TO_TRANSFER)" "$(count_status PIPELINE_DEFERRED_RETRY)" "$(count_status PIPELINE_DEFERRED_FAILED)" "$(count_status OUT_OF_SCOPE)"
active=$(awk -F '\t' 'NR>1&&$9~/^(CREATED|SUBMITTED|RUNNING)$/{print $1;exit}' "$WAVE_STATUS_FILE"); printf 'Active submission: %s\n' "${active:-none}"
job=""; [[ -z "$active" ]] || job=$(wave_field "$active" pipeline_job_id); printf 'Slurm array job ID: %s\n' "${job:-none}"
if [[ "$PIPELINE_MODE" == streaming_per_sample ]]; then
 printf 'Sample concurrency: %s\n' "$SAMPLE_CHAIN_CONCURRENCY"
 hold_file="${MANAGER_ROOT}/state/streaming_array_disk_holds.tsv"
 held=0; [[ ! -s "$hold_file" ]] || held=$(awk 'END{print NR>0?NR-1:0}' "$hold_file")
 printf 'Disk pressure array admission: %s\nArray elements held by manager: %s\nArray release threshold: %s%%\n' "$([[ "$held" -gt 0 ]] && echo HELD || echo ACTIVE)" "$held" "$WORK_ARRAY_RELEASE_PERCENT"
 if (( held > 0 )); then printf 'Array hold reason: %s\n' "$(awk -F '\t' 'NR==2{print $4;exit}' "$hold_file")"; fi
 printf 'Submitted sample tasks: %s\nRunning sample tasks: %s\n' "$(count_status PIPELINE_SUBMITTED)" "$(awk -F '\t' 'NR>1&&$4~/^(PIPELINE_RUNNING|PIPELINE_DEFERRED_RUNNING)$/{n++}END{print n+0}' "$STATUS_FILE")"
else
 printf 'Batch size: %s\nBatch concurrency: %s\n' "$PIPELINE_BATCH_SIZE" "$CHAIN_CONCURRENT_BATCHES"
 if [[ -n "$active" && -s "${MANAGER_ROOT}/state/submission_task_map/${active}.tsv" ]]; then printf 'Total batches in active submission: %s\n' "$(awk -F '\t' 'NR>1&&!seen[$5]++{n++}END{print n+0}' "${MANAGER_ROOT}/state/submission_task_map/${active}.tsv")"; fi
fi
printf 'Work filesystem usage: %s%%\nTotal samples: %s\nCounts by status:\n' "$(work_disk_used_percent)" "$(awk 'END{print NR-1}' "$STATUS_FILE")"
awk -F '\t' 'NR>1{n[$4]++}END{for(s in n)printf "  %-24s %d\n",s,n[s]}' "$STATUS_FILE" | sort
