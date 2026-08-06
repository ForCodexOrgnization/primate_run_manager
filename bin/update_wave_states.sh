#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
command -v sacct >/dev/null 2>&1 || { log "sacct unavailable; wave states unchanged"; exit 0; }
mapfile -t waves < <(awk -F '\t' 'NR>1&&$9~/^(CREATED|SUBMITTED|RUNNING)$/{print $1"\t"$4}' "$WAVE_STATUS_FILE")
for entry in "${waves[@]}"; do
 wave=${entry%%$'\t'*}; job=${entry#*$'\t'}; [[ -n "$job" ]] || continue
 mapfile -t task_states < <(sacct -n -j "$job" --format=JobIDRaw,State --parsable2 2>/dev/null | awk -F '|' -v j="$job" '
   $1 !~ /\.(batch|extern)$/ && $1 ~ ("^" j "_[0-9]+$") {sub(/ .*/,"",$2); sub(/\+$/,"",$2); if($2!="") print $2}')
 # Preserve compatibility with a non-array launcher, but never mistake job steps
 # for jobs.  For arrays, only the task rows above are relevant.
 if ((${#task_states[@]}==0)); then
   mapfile -t task_states < <(sacct -n -j "$job" --format=JobIDRaw,State --parsable2 2>/dev/null | awk -F '|' -v j="$job" '$1==j{sub(/ .*/,"",$2); sub(/\+$/,"",$2);if($2!="")print $2}')
 fi
 ((${#task_states[@]})) || continue
 active=""; failed=0; all_completed=1; terminal_state=COMPLETED
 for state in "${task_states[@]}"; do
   case "$state" in
     PENDING|CONFIGURING|RUNNING|COMPLETING|REQUEUED|RESIZING|SUSPENDED) active="$state"; all_completed=0;;
     COMPLETED) ;;
     FAILED|CANCELLED|TIMEOUT|OUT_OF_MEMORY|NODE_FAIL|PREEMPTED|BOOT_FAIL) failed=1; terminal_state="$state"; all_completed=0;;
     *) all_completed=0;;
   esac
 done
 if [[ -n "$active" ]]; then
   wave_status=RUNNING; [[ "$active" == PENDING || "$active" == CONFIGURING ]] && wave_status=SUBMITTED
   with_state_lock update_wave_row "$wave" "slurm_state=$active" "status=$wave_status"
   if [[ "$wave_status" == RUNNING ]]; then while read -r s; do current=$(awk -F '\t' -v x="$s" 'NR>1&&$1==x{print $4}' "$STATUS_FILE"); [[ "$current" == WAVE_SUBMITTED ]] && with_state_lock update_sample_fields "$s" "status=PIPELINE_RUNNING"; done < <(samples_in_wave "$wave"); fi
   continue
 fi
 (( all_completed || failed )) || { log "$wave has unrecognized sacct terminal state; leaving active"; continue; }
 terminal=COMPLETE; (( failed )) && terminal=FAILED
 failure_class=""; resume_eligible=0
 if (( failed )); then
   case " $RESUME_ELIGIBLE_SLURM_STATES " in *" $terminal_state "*) failure_class=INFRASTRUCTURE; resume_eligible=1;; *) failure_class=PIPELINE; resume_eligible=0;; esac
 fi
 work_root=$(wave_field "$wave" work_root); wave_notes=$(wave_field "$wave" notes)
 wave_phase=NORMAL; [[ "$wave_notes" == *"phase=DEFERRED_RETRY"* ]] && wave_phase=DEFERRED_RETRY
 marker_class=""; marker_reason=""; failed_batches=0; marker_failed_samples=0
 if [[ -d "$work_root/batch_status" ]]; then
   failed_batches=$(find "$work_root/batch_status" -maxdepth 1 -type f -name '*.failure.tsv' | wc -l | tr -d ' ')
   marker_failed_samples=$(find "$work_root/batch_status" -maxdepth 1 -type f -name '*.failed_samples.tsv' -exec cat {} + 2>/dev/null | awk 'NF{seen[$1]=1}END{print length(seen)}')
   first_marker=$(find "$work_root/batch_status" -maxdepth 1 -type f -name '*.failure.tsv' -print -quit)
   if [[ -n "$first_marker" ]]; then marker_class=$(awk -F '\t' 'NR==2{print $1}' "$first_marker"); marker_reason=$(awk -F '\t' 'NR==2{print $2}' "$first_marker"); fi
 fi
 [[ -n "$marker_class" ]] && failure_class=$marker_class
 [[ -n "$failure_class" ]] || failure_class=UNKNOWN
 with_state_lock update_wave_row "$wave" "slurm_state=$terminal_state" "status=$terminal" "failure_class=$failure_class" "resume_eligible=$resume_eligible"
 "${SCRIPT_DIR}/scan_active_results.sh" "$1"
 complete=0; incomplete=0
 while read -r s; do
   st=$(awk -F '\t' -v x="$s" 'NR>1&&$1==x{print $4}' "$STATUS_FILE")
   if [[ "$st" == READY_TO_TRANSFER || "$st" == PIPELINE_COMPLETE ]]; then complete=$((complete+1)); else
     incomplete=$((incomplete+1)); attempts=$(awk -F '\t' -v x="$s" 'NR>1&&$1==x{print $7+0}' "$STATUS_FILE")
     next=PIPELINE_DEFERRED_RETRY
     if [[ "$st" == PIPELINE_RETRY_RUNNING ]]; then next=PIPELINE_FAILED; (( attempts < MAX_PIPELINE_RETRIES )) && next=PIPELINE_RETRY_READY
     if [[ "$wave_phase" == DEFERRED_RETRY ]]; then
       deferred_attempts=$((attempts-1)); (( deferred_attempts < 0 )) && deferred_attempts=0
       (( deferred_attempts >= MAX_DEFERRED_RETRIES )) && next=PIPELINE_DEFERRED_FAILED
     fi
     fi
     with_state_lock update_sample_fields "$s" "status=$next" "last_pipeline_error=$([[ "$terminal" == COMPLETE ]] && echo '' || echo "$terminal_state")" "notes=wave ended; per-sample outputs incomplete"
   fi
 done < <(awk -F '\t' -v w="$wave" 'NR>1&&$6==w{print $1}' "$STATUS_FILE")
 final=$terminal; ((complete>0&&incomplete>0)) && final=PARTIAL_COMPLETE
 ((incomplete==0 && failed==0)) && final=COMPLETE
 with_state_lock update_wave_row "$wave" "complete_count=$complete" "incomplete_count=$incomplete" "status=$final"
 diagnostics="${MANAGER_ROOT}/state/failure_diagnostics/${wave}"
 failure_tmp="${WAVE_FAILURE_FILE}.tmp.$$"; [[ -s "$WAVE_FAILURE_FILE" ]] || printf 'wave_id\tfailure_class\tfailure_reason\tfailed_batch_count\tfailed_sample_count\tdiagnostics_path\tupdated_at\n' > "$WAVE_FAILURE_FILE"
 awk -F '\t' -v w="$wave" 'NR==1||$1!=w' "$WAVE_FAILURE_FILE" > "$failure_tmp"
 printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$wave" "$failure_class" "${marker_reason:-$terminal_state}" "$failed_batches" "$incomplete" "$diagnostics" "$(now_iso)" >> "$failure_tmp"; mv "$failure_tmp" "$WAVE_FAILURE_FILE"
done
