#!/usr/bin/env bash
# Conservatively detach samples from an operator-cancelled submission.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$SCRIPT_DIR/../lib/common.sh"
cfg="${1:-}"; mode="${2:---dry-run}"
[[ "$mode" == --dry-run || "$mode" == --apply ]] || die "usage: $0 CONFIG [--dry-run|--apply]"
load_config "$cfg"; validate_config

target=$(resolve_cancelled_recovery_target) || exit $?
[[ $(awk -F= '$1=="nothing_to_recover"{print $2}' <<<"$target") == 1 ]] && { printf '%s\n' "$target"; exit 0; }
target_source=$(awk -F= '$1=="target_source"{print $2}' <<<"$target"); sid=$(awk -F= '$1=="submission_id"{print $2}' <<<"$target")
wave_status=$(awk -F= '$1=="wave_status"{print $2}' <<<"$target"); job=$(awk -F= '$1=="pipeline_job_id"{print $2}' <<<"$target"); map=$(awk -F= '$1=="map_file"{sub(/^[^=]*=/,"");print}' <<<"$target")
contract=$(slurm_cancelled_recovery_contract "${USER:-$(id -un)}" "$job"); outcome=${contract%%$'\n'*}; states=$(awk -F= '$1=="states"{print $2}' <<<"$contract"); cancelled=$(awk -F= '$1=="cancellation_evidence"{print $2}' <<<"$contract"); cancelled=${cancelled:-0}
case "$outcome" in LIVE) die 'target pipeline array remains present in the live queue; recovery did not modify state';; QUERY_ERROR) die 'Slurm live-queue query failed; recovery did not modify state';; ACCOUNTING_UNAVAILABLE) die 'Slurm accounting unavailable or insufficient; recovery did not modify state';; ACCOUNTING_AMBIGUOUS) die "Slurm accounting does not establish terminal array elements (states: ${states:-none}); recovery did not modify state";; TERMINAL) :;; *) die "unknown Slurm recovery outcome: $outcome";; esac
if [[ "$target_source" == active_submission ]] && ((cancelled == 0)); then
  die "Slurm accounting does not prove cancellation (states: ${states:-none}); recovery did not modify state"
fi

if [[ "$target_source" == orphaned_sample_wave_id ]]; then
  mapped=$(awk -F '\t' -v w="$sid" 'NR==FNR{if(NR>1) member[$8]=1;next} NR>1&&$6==w&&$4~/^(PIPELINE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_DEFERRED_RUNNING)$/&&member[$1]{n++} END{print n+0}' "$map" "$STATUS_FILE")
else
  mapped=$(awk -F '\t' 'NR>1&&!seen[$8]++{n++}END{print n+0}' "$map")
fi
retry=0; complete=0; failed=0; outside=0
while IFS= read -r sample; do
  [[ -n "$sample" ]] || continue
  status=$(awk -F '\t' -v s="$sample" 'NR>1&&$1==s{print $4;exit}' "$STATUS_FILE")
  sample_wave=$(awk -F '\t' -v s="$sample" 'NR>1&&$1==s{print $6;exit}' "$STATUS_FILE")
  # Membership in the immutable map is necessary but not sufficient: never
  # detach a row which has subsequently acquired different ownership.
  [[ "$sample_wave" == "$sid" ]] || continue
  assigned=0; awk -F '\t' -v s="$sample" '$1==s{ok=1}END{exit !ok}' "$ASSIGNED_SAMPLE_LIST" && assigned=1
  valid=0; awk -F '\t' -v s="$sample" 'NR>1&&$1==s&&$8==1{ok=1}END{exit !ok}' "$VALIDATION_FILE" && valid=1
  if ((valid)) || [[ "$status" =~ ^(LOCAL_FINAL_RETAINED|READY_TO_TRANSFER|TRANSFERRING)$ ]]; then complete=$((complete+1))
  elif [[ "$status" =~ ^(PIPELINE_DEFERRED_FAILED|TERMINAL_FAILURE)$ ]]; then failed=$((failed+1))
  elif ((assigned == 0)); then outside=$((outside+1))
  elif [[ "$status" =~ ^(PIPELINE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_DEFERRED_RUNNING)$ ]]; then retry=$((retry+1))
  fi
done < <(awk -F '\t' 'NR>1&&!seen[$8]++{print $8}' "$map")

printf 'target_source=%s\nsubmission_id=%s\nwave_status=%s\npipeline_job_id=%s\nslurm_states=%s\nmapped=%s\ncomplete=%s\nretry=%s\npreserved_failed=%s\noutside_scope=%s\n' "$target_source" "$sid" "$wave_status" "$job" "$states" "$mapped" "$complete" "$retry" "$failed" "$outside"
[[ "$mode" == --apply ]] || exit 0

apply_recovery() {
  local sample status sample_wave assigned valid notes wave_notes
  if [[ "$target_source" == active_submission ]]; then
    wave_notes=$(wave_field "$sid" notes)
    wave_notes="${wave_notes}${wave_notes:+; }operator recovery reconciled terminal Slurm array after confirmed cancellation"
    update_wave_row "$sid" status=CANCELLED slurm_state=CANCELLED "notes=$wave_notes"
  fi
  while IFS= read -r sample; do
    [[ -n "$sample" ]] || continue
    status=$(awk -F '\t' -v s="$sample" 'NR>1&&$1==s{print $4;exit}' "$STATUS_FILE")
    # Completed and terminal rows are deliberately not rewritten. Normal scans
    # own promotion of validated output; recovery only detaches incomplete work.
    [[ "$status" =~ ^(PIPELINE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_DEFERRED_RUNNING)$ ]] || continue
    sample_wave=$(awk -F '\t' -v s="$sample" 'NR>1&&$1==s{print $6;exit}' "$STATUS_FILE")
    [[ "$sample_wave" == "$sid" ]] || continue
    if ! awk -F '\t' -v s="$sample" '$1==s{ok=1}END{exit !ok}' "$ASSIGNED_SAMPLE_LIST"; then
      notes=$(awk -F '\t' -v s="$sample" 'NR>1&&$1==s{print $14;exit}' "$STATUS_FILE")
      notes="${notes}${notes:+; }operator recovery retired cancelled-submission sample outside assigned scope"
      update_sample_fields "$sample" status=OUT_OF_SCOPE slurm_job_id= wave_id= last_pipeline_error= "notes=$notes"
      continue
    fi
    if awk -F '\t' -v s="$sample" 'NR>1&&$1==s&&$8==1{ok=1}END{exit !ok}' "$VALIDATION_FILE"; then continue; fi
    notes=$(awk -F '\t' -v s="$sample" 'NR>1&&$1==s{print $14;exit}' "$STATUS_FILE")
    notes="${notes}${notes:+; }operator recovery detached confirmed cancelled submission $sid"
    update_sample_fields "$sample" status=PIPELINE_DEFERRED_RETRY slurm_job_id= wave_id= last_pipeline_error= "notes=$notes"
  done < <(awk -F '\t' 'NR>1&&!seen[$8]++{print $8}' "$map")
}
with_state_lock apply_recovery
