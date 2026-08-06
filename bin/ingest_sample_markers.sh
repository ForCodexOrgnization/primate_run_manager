#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ "$PIPELINE_MODE" == streaming_per_sample ]] || exit 0
while IFS=$'\t' read -r sample current attempts; do
 safe_sample_id "$sample" || continue
 base="${PIPELINE_WORK_ROOT}/.sample_state/${sample}"
 # Validated output is authoritative and historical manual-review states remain manual.
 awk -F '\t' -v s="$sample" 'NR>1&&$1==s&&$8==1{ok=1}END{exit !ok}' "$VALIDATION_FILE" && { with_state_lock update_sample_fields "$sample" status=READY_TO_TRANSFER; continue; }
 [[ "$current" != PIPELINE_INCOMPLETE_REVIEW ]] || continue
 state=$(sample_array_state "$sample")
 if slurm_state_is_active "$state"; then
   with_state_lock update_sample_fields "$sample" status=PIPELINE_RUNNING notes="sample array element active: $state"
 elif [[ -s "${base}.failure.tsv" && -n "$state" ]]; then
   next=PIPELINE_DEFERRED_RETRY; (( attempts > MAX_DEFERRED_RETRIES )) && next=PIPELINE_DEFERRED_FAILED
   reason=$(marker_field "${base}.failure.tsv" failure_reason)
   with_state_lock update_sample_fields "$sample" "status=$next" "last_pipeline_error=$reason" "notes=terminal sample worker failure; deferred retry"
 fi
done < <(awk -F '\t' 'NR>1{print $1"\t"$4"\t"$7}' "$STATUS_FILE")
