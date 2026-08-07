#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ "$PIPELINE_MODE" == streaming_per_sample ]] || exit 0
# A completion marker asks the normal validator to inspect outputs; validation,
# rather than the marker alone, remains authoritative.
if find "${PIPELINE_WORK_ROOT}/.sample_state" -maxdepth 1 -type f -name '*.complete.tsv' -print -quit 2>/dev/null | grep -q .; then
  "${SCRIPT_DIR}/scan_active_results.sh" "$1"
fi
while IFS=$'\t' read -r sample current attempts submission; do
 safe_sample_id "$sample" || continue
 case "$current" in READY_TO_TRANSFER|LOCAL_FINAL_RETAINED|PIPELINE_INCOMPLETE_REVIEW) continue;; esac
 base="${PIPELINE_WORK_ROOT}/.sample_state/${sample}"
 if awk -F '\t' -v s="$sample" 'NR>1&&$1==s&&$8==1{ok=1}END{exit !ok}' "$VALIDATION_FILE"; then
   with_state_lock update_sample_fields "$sample" status=READY_TO_TRANSFER last_pipeline_error= notes="formal outputs validated independently"
   continue
 fi
 state=$(sample_array_state "$sample")
 if slurm_state_is_active "$state"; then
   next=PIPELINE_RUNNING; [[ "$current" == PIPELINE_DEFERRED_RUNNING ]] && next=PIPELINE_DEFERRED_RUNNING
   with_state_lock update_sample_fields "$sample" "status=$next" "notes=exact sample array element active: $state"
   continue
 fi
 # Empty state means accounting is temporarily unavailable, not terminal.
 [[ -n "$state" ]] || continue
 phase=$(awk -F '\t' -v s="$sample" 'NR>1&&$4==s{p=$7}END{print p}' "${MANAGER_ROOT}/state/array_sample_map/"*.tsv 2>/dev/null || true)
 if [[ -z "$phase" ]]; then notes=$(wave_field "$submission" notes); phase=NORMAL; [[ "$notes" == *phase=DEFERRED_RETRY* ]] && phase=DEFERRED_RETRY; fi
 next=$(deferred_terminal_status "$phase" "$attempts")
 if [[ -s "${base}.failure.tsv" ]]; then
   reason=$(marker_field "${base}.failure.tsv" failure_reason); reason=${reason:-TERMINAL_FAILURE}
 else
   reason="TERMINAL_${state}_WITHOUT_VALID_OUTPUT_OR_MARKER"
 fi
 with_state_lock update_sample_fields "$sample" "status=$next" "last_pipeline_error=$reason" "notes=exact sample array element terminal; outputs incomplete"
done < <(awk -F '\t' 'NR>1&&$4~/^(WAVE_SUBMITTED|PIPELINE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_DEFERRED_RUNNING)$/{print $1"\t"$4"\t"$7"\t"$6}' "$STATUS_FILE")
