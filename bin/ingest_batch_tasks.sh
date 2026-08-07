#!/usr/bin/env bash
# Reconcile each batch array element independently; never wait for its siblings.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ "$PIPELINE_MODE" == batch ]] || exit 0
[[ -d "${MANAGER_ROOT}/state/submission_task_map" ]] || exit 0
# Refresh formal validation before classifying terminal tasks.
"${SCRIPT_DIR}/scan_active_results.sh" "$1"
while IFS=$'\t' read -r submission phase job task task_name work; do
 state=$(submission_task_state "$job" "$task")
 [[ -n "$state" ]] || continue
 if slurm_state_is_active "$state"; then
   while IFS=$'\t' read -r sample current; do
     case "$current" in PIPELINE_SUBMITTED|WAVE_SUBMITTED|PIPELINE_RUNNING) next=PIPELINE_RUNNING;; PIPELINE_DEFERRED_RUNNING) next=PIPELINE_DEFERRED_RUNNING;; *) continue;; esac
     with_state_lock update_sample_fields "$sample" "status=$next" "notes=exact batch array element ${job}_${task} active: $state"
   done < <(awk -F '\t' -v j="$job" -v t="$task" 'NR==FNR&&NR>1&&$4==j&&$5==t{s[$8]=1;next} FNR>1&&($1 in s){print $1"\t"$4}' "${MANAGER_ROOT}/state/submission_task_map/${submission}.tsv" "$STATUS_FILE")
   continue
 fi
 while IFS=$'\t' read -r sample current attempts; do
   case "$current" in READY_TO_TRANSFER|TRANSFERRING|TRANSFERRED_FULL|LOCAL_FINAL_RETAINED|PIPELINE_INCOMPLETE_REVIEW) continue;; esac
   if awk -F '\t' -v s="$sample" 'NR>1&&$1==s&&$8==1{ok=1}END{exit !ok}' "$VALIDATION_FILE"; then
     with_state_lock update_sample_fields "$sample" status=READY_TO_TRANSFER last_pipeline_error= notes="formal outputs valid after exact batch task ${job}_${task} terminal"
   else
     next=$(deferred_terminal_status "$phase" "$attempts")
     failed_file="${work}/batch_status/${task_name}.failed_samples.tsv"; reason="TERMINAL_BATCH_${state}_INCOMPLETE"
     [[ -s "$failed_file" ]] && awk -F '\t' -v s="$sample" '$1==s{found=1}END{exit !found}' "$failed_file" && reason="BATCH_REPORTED_SAMPLE_FAILURE"
     with_state_lock update_sample_fields "$sample" "status=$next" "last_pipeline_error=$reason" "notes=exact batch task terminal; per-sample validation incomplete"
   fi
 done < <(awk -F '\t' -v j="$job" -v t="$task" 'NR==FNR&&NR>1&&$4==j&&$5==t{s[$8]=1;next} FNR>1&&($1 in s){print $1"\t"$4"\t"$7}' "${MANAGER_ROOT}/state/submission_task_map/${submission}.tsv" "$STATUS_FILE")
done < <(for f in "${MANAGER_ROOT}"/state/submission_task_map/*.tsv; do [[ -e "$f" ]] || continue; awk -F '\t' 'NR>1&&!seen[$4 FS $5]++{print $1"\t"$3"\t"$4"\t"$5"\t"$7"\t"$11}' "$f"; done)
