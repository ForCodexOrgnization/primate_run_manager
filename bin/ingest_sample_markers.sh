#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ "$PIPELINE_MODE" == streaming_per_sample ]] || exit 0
submission_phase() {
  local sample=$1 submission=$2 map phase notes
  map="${MANAGER_ROOT}/state/submission_task_map/${submission}.tsv"
  if [[ -s "$map" ]]; then phase=$(awk -F '\t' -v s="$sample" 'NR>1&&$6=="SAMPLE"&&$8==s{print $3;exit}' "$map"); fi
  if [[ -z "${phase:-}" ]]; then
    notes=$(wave_field "$submission" notes); phase=NORMAL
    [[ "$notes" == *phase=DEFERRED_RETRY* ]] && phase=DEFERRED_RETRY
  fi
  printf '%s\n' "$phase"
}
while IFS=$'\t' read -r sample current attempts submission; do
 safe_sample_id "$sample" || continue
 case "$current" in READY_TO_TRANSFER|LOCAL_FINAL_RETAINED|PIPELINE_INCOMPLETE_REVIEW) continue;; esac
 base="${PIPELINE_WORK_ROOT}/.sample_state/${sample}"
 if awk -F '\t' -v s="$sample" 'NR>1&&$1==s&&$8==1{ok=1}END{exit !ok}' "$VALIDATION_FILE"; then
   with_state_lock update_sample_fields "$sample" status=READY_TO_TRANSFER last_pipeline_error= notes="formal outputs validated independently"
   continue
 fi
 state=$(sample_array_state "$sample"); phase=$(submission_phase "$sample" "$submission")
 if slurm_state_is_executing "$state"; then
   next=PIPELINE_RUNNING; [[ "$phase" == DEFERRED_RETRY ]] && next=PIPELINE_DEFERRED_RUNNING
   with_state_lock update_sample_fields "$sample" "status=$next" "notes=exact sample array element executing: $state; phase=$phase"
   continue
 fi
 if slurm_state_is_active "$state"; then
   # PENDING (including manager-held work) is owned but has not executed.
   with_state_lock update_sample_fields "$sample" "status=PIPELINE_SUBMITTED" "notes=exact sample array element not executing: $state; phase=$phase"
   continue
 fi
 if [[ -z "$state" ]]; then
   [[ -n "$submission" ]] || continue
   submission_status=$(wave_field "$submission" status); [[ -n "$submission_status" ]] || continue
   [[ "$submission_status" =~ ^(CREATED|SUBMITTED|RUNNING)$ ]] && continue
   state=ACCOUNTING_RECORD_UNAVAILABLE
 elif ! slurm_state_is_terminal "$state"; then
   continue
 fi
 next=$(deferred_terminal_status "$phase" "$attempts")
 if [[ -s "${base}.failure.tsv" ]]; then reason=$(marker_field "${base}.failure.tsv" failure_reason); reason=${reason:-TERMINAL_FAILURE}
 else reason="TERMINAL_${state}_WITHOUT_VALID_OUTPUT_OR_MARKER"; fi
 with_state_lock update_sample_fields "$sample" "status=$next" "last_pipeline_error=$reason" "notes=exact sample array element terminal; outputs incomplete; phase=$phase"
done < <(awk -F '\t' 'NR>1&&$4~/^(WAVE_SUBMITTED|PIPELINE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_DEFERRED_RUNNING)$/{print $1"\t"$4"\t"$7"\t"$6}' "$STATUS_FILE")

# Completion markers request validation only for the samples that own them.
# The manager cycle may suppress this call and perform its single scan below.
if [[ "${INGEST_SKIP_VALIDATION:-0}" != 1 ]]; then
  targets=$(mktemp); trap 'rm -f "$targets"' EXIT
  find "${PIPELINE_WORK_ROOT}/.sample_state" -maxdepth 1 -type f -name '*.complete.tsv' -printf '%f\n' 2>/dev/null |
    awk '{sub(/\.complete\.tsv$/,""); print}' > "$targets"
  [[ ! -s "$targets" ]] || SCAN_SAMPLE_LIST="$targets" "${SCRIPT_DIR}/scan_active_results.sh" "$1"
fi
