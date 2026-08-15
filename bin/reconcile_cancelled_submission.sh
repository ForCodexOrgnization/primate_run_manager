#!/usr/bin/env bash
# Conservatively detach samples from an operator-cancelled submission.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$SCRIPT_DIR/../lib/common.sh"
cfg="${1:-}"; mode="${2:---dry-run}"
[[ "$mode" == --dry-run || "$mode" == --apply ]] || die "usage: $0 CONFIG [--dry-run|--apply]"
load_config "$cfg"; validate_config

mapfile -t active < <(awk -F '\t' 'NR>1&&$9~/^(CREATED|SUBMITTED|RUNNING)$/{print $1}' "$WAVE_STATUS_FILE")
target_source=active_submission
if ((${#active[@]} == 1)); then
  sid=${active[0]}
elif ((${#active[@]} > 1)); then
  die "recovery requires exactly one active/stale submission (found ${#active[@]}: ${active[*]})"
else
  # A previous recovery may already have made the wave terminal before all of
  # its sample ownership was detached.  Infer that wave only from statuses
  # which still mean that pipeline work is assigned.
  mapfile -t orphaned_waves < <(awk -F '\t' '
    NR>1 && $4~/^(PIPELINE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_DEFERRED_RUNNING)$/ && $6!="" {count[$6]++}
    END {for (wave in count) print wave "\t" count[wave]}
  ' "$STATUS_FILE" | sort)
  if ((${#orphaned_waves[@]} == 0)); then
    printf 'target_source=orphaned_sample_wave_id\nnothing_to_recover=1\n'
    exit 0
  fi
  if ((${#orphaned_waves[@]} > 1)); then
    printf 'recovery blocked: orphaned running samples reference multiple waves:\n' >&2
    printf '  %s\n' "${orphaned_waves[@]}" >&2
    die 'recovery requires exactly one orphaned sample wave ID'
  fi
  sid=${orphaned_waves[0]%%$'\t'*}
  target_source=orphaned_sample_wave_id
  wave_matches=$(awk -F '\t' -v w="$sid" 'NR>1&&$1==w{n++}END{print n+0}' "$WAVE_STATUS_FILE")
  ((wave_matches == 1)) || die "orphaned sample wave $sid does not resolve to exactly one wave_status row (found $wave_matches)"
  wave_status=$(wave_field "$sid" status)
  [[ "$wave_status" == CANCELLED ]] || die "orphaned sample wave $sid is not cancellation-compatible (status: ${wave_status:-empty}; required: CANCELLED)"
fi
wave_status=$(wave_field "$sid" status)
job=$(wave_field "$sid" pipeline_job_id)
[[ "$job" =~ ^[0-9]+$ ]] || die "active submission $sid has no valid pipeline job ID"
map="$MANAGER_ROOT/state/submission_task_map/$sid.tsv"; [[ -s "$map" ]] || die "submission task map missing: $map"

if [[ "$target_source" == orphaned_sample_wave_id ]]; then
  # Do not silently detach ownership which is absent from the immutable,
  # authoritative submission membership map.
  while IFS=$'\t' read -r sample _; do
    awk -F '\t' -v s="$sample" 'NR>1&&$8==s{ok=1}END{exit !ok}' "$map" ||
      die "orphaned running sample $sample is not present in submission task map for $sid"
  done < <(awk -F '\t' -v w="$sid" 'NR>1&&$6==w&&$4~/^(PIPELINE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_DEFERRED_RUNNING)$/{print $1}' "$STATUS_FILE")
fi

# squeue is authoritative for liveness. Query the whole array so a live child
# cannot be hidden by a terminal parent accounting row.
queue=$(squeue --noheader --jobs="$job" --format='%T' 2>/dev/null) || die "Slurm squeue query failed; recovery did not modify state"
while IFS= read -r state; do
  [[ -z "$state" ]] && continue
  if slurm_state_is_active "$state" || [[ "$(slurm_normalize_state "$state")" != CANCELLED ]]; then
    die "active pipeline work still exists; recovery did not modify it (Slurm state: $state)"
  fi
done <<<"$queue"
[[ -z "${queue//$'\n'/}" ]] || die "pipeline array remains visible in squeue; recovery did not modify it"

command -v sacct >/dev/null 2>&1 || die "Slurm accounting unavailable; recovery did not modify state"
accounting=$(sacct -n -j "$job" --format=JobID,State --parsable2 2>/dev/null) || die "Slurm accounting query failed; recovery did not modify state"
cancelled=0; ambiguous=0; states=""
while IFS='|' read -r jid state _; do
  [[ -n "$jid" && -n "$state" ]] || continue
  state=$(slurm_normalize_state "$state"); states="${states}${states:+,}$state"
  case "$state" in CANCELLED) cancelled=1;; COMPLETED) :;; *) ambiguous=1;; esac
done <<<"$accounting"
((cancelled == 1 && ambiguous == 0)) || die "Slurm accounting does not unambiguously prove cancellation (states: ${states:-none}); recovery did not modify state"

if [[ "$target_source" == orphaned_sample_wave_id ]]; then
  mapped=$(awk -F '\t' -v w="$sid" 'NR==FNR{if(NR>1) member[$8]=1;next} NR>1&&$6==w&&$4~/^(PIPELINE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_DEFERRED_RUNNING)$/&&member[$1]{n++} END{print n+0}' "$map" "$STATUS_FILE")
else
  mapped=$(awk -F '\t' 'NR>1&&!seen[$8]++{n++}END{print n+0}' "$map")
fi
retry=0; complete=0; failed=0; outside=0
while IFS= read -r sample; do
  [[ -n "$sample" ]] || continue
  status=$(awk -F '\t' -v s="$sample" 'NR>1&&$1==s{print $4;exit}' "$STATUS_FILE")
  if [[ "$target_source" == orphaned_sample_wave_id ]]; then
    sample_wave=$(awk -F '\t' -v s="$sample" 'NR>1&&$1==s{print $6;exit}' "$STATUS_FILE")
    [[ "$sample_wave" == "$sid" ]] || continue
  fi
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
    if [[ "$target_source" == orphaned_sample_wave_id ]]; then
      sample_wave=$(awk -F '\t' -v s="$sample" 'NR>1&&$1==s{print $6;exit}' "$STATUS_FILE")
      [[ "$sample_wave" == "$sid" ]] || continue
    fi
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
