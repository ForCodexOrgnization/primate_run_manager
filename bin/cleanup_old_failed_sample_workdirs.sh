#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ "$PIPELINE_MODE" == streaming_per_sample ]] || exit 0
used_now(){ [[ -n "${WORK_DISK_USED_PERCENT_OVERRIDE:-}" ]] && printf '%s\n' "$WORK_DISK_USED_PERCENT_OVERRIDE" || work_disk_used_percent; }
validated_complete(){ awk -F '\t' -v s="$1" 'NR>1&&$1==s&&$8==1{ok=1}END{exit !ok}' "$VALIDATION_FILE"; }
status_of(){ awk -F '\t' -v s="$1" 'NR>1&&$1==s{print $4;exit}' "$STATUS_FILE"; }
map_row(){ awk -F '\t' -v s="$1" 'NR>1&&$6=="SAMPLE"&&$8==s{line=$0}END{print line}' "${MANAGER_ROOT}"/state/submission_task_map/*.tsv 2>/dev/null || true; }
manager_owns_hold(){ awk -F '\t' -v j="$1" -v t="$2" 'NR>1&&$1==j&&$2==t{ok=1}END{exit !ok}' "$MANAGER_ROOT/state/streaming_array_disk_holds.tsv"; }
pending_reason(){ squeue --noheader --array --jobs="$1" --format='%F|%K|%T|%r' 2>/dev/null | awk -F'|' -v t="$2" '$2==t&&$3=="PENDING"{print $4;exit}'; }
declare -A count=([missing_workdir]=0 [validated_complete]=0 [missing_archive]=0 [active_slurm]=0 [markerless_terminal_not_safe]=0 [lock_busy]=0 [state_changed]=0 [deleted]=0)
used=$(used_now); (( used >= FAILED_CACHE_CLEAN_TRIGGER_PERCENT )) || exit 0
while IFS=$'\t' read -r sample manager_status; do
  (( $(used_now) >= FAILED_CACHE_CLEAN_TARGET_PERCENT )) || break
  safe_sample_id "$sample" || { count[markerless_terminal_not_safe]=$((count[markerless_terminal_not_safe]+1)); continue; }
  work=$(sample_work_root "$sample"); root=$(realpath -m "$PIPELINE_WORK_ROOT"); target=$(realpath -m "$work")
  [[ "$root" != / && "$target" == "$root/$sample" && -d "$target" ]] || { count[missing_workdir]=$((count[missing_workdir]+1)); continue; }
  if validated_complete "$sample"; then count[validated_complete]=$((count[validated_complete]+1)); continue; fi
  row=$(map_row "$sample"); [[ -n "$row" ]] || { count[markerless_terminal_not_safe]=$((count[markerless_terminal_not_safe]+1)); continue; }
  phase=$(cut -f3 <<<"$row"); job=$(cut -f4 <<<"$row"); task=$(cut -f5 <<<"$row")
  state=$(submission_task_state "$job" "$task"); kind=terminal
  if [[ "$state" == PENDING ]]; then
    kind=held_pending; reason=$(pending_reason "$job" "$task")
    if [[ "$phase" != DEFERRED_RETRY || "$reason" != JobHeldUser ]] || ! manager_owns_hold "$job" "$task"; then count[active_slurm]=$((count[active_slurm]+1)); continue; fi
  elif slurm_state_is_active "$state"; then count[active_slurm]=$((count[active_slurm]+1)); continue
  elif ! slurm_state_is_terminal "$state" || [[ ! "$manager_status" =~ ^(PIPELINE_DEFERRED_RETRY|PIPELINE_DEFERRED_FAILED)$ ]]; then
    count[markerless_terminal_not_safe]=$((count[markerless_terminal_not_safe]+1)); continue
  fi
  requeue_marker="$PIPELINE_WORK_ROOT/.sample_state/$sample.requeue.tsv"
  if [[ -s "$requeue_marker" ]] && awk -F '\t' -v j="$job" -v t="$task" 'NR==1{for(i=1;i<=NF;i++)h[$i]=i;next}$h["reason"]=="TIMEOUT_SIGNAL"&&$h["resume_eligible"]==1&&$h["array_job_id"]==j&&$h["array_task_id"]==t{ok=1}END{exit !ok}' "$requeue_marker"; then
    count[active_slurm]=$((count[active_slurm]+1)); continue
  fi
  marker="$PIPELINE_WORK_ROOT/.sample_state/$sample.failure.tsv"
  archive="$MANAGER_ROOT/state/failure_diagnostics/samples/$sample"
  if [[ -s "$marker" && ! -f "$archive/ARCHIVE_COMPLETE" ]]; then count[missing_archive]=$((count[missing_archive]+1)); continue; fi
  exec {lock_fd}>"$PIPELINE_WORK_ROOT/.locks/$sample.lock"
  flock -n "$lock_fd" || { exec {lock_fd}>&-; count[lock_busy]=$((count[lock_busy]+1)); continue; }
  # Re-read every ownership predicate after acquiring the worker lock.
  current=$(status_of "$sample"); state2=$(submission_task_state "$job" "$task"); safe=1
  validated_complete "$sample" && safe=0
  if [[ "$kind" == held_pending ]]; then
    reason2=$(pending_reason "$job" "$task")
    [[ "$current" == PIPELINE_SUBMITTED && "$phase" == DEFERRED_RETRY && "$state2" == PENDING && "$reason2" == JobHeldUser ]] || safe=0
    manager_owns_hold "$job" "$task" || safe=0
  else
    [[ "$current" =~ ^(PIPELINE_DEFERRED_RETRY|PIPELINE_DEFERRED_FAILED)$ ]] || safe=0
    slurm_state_is_terminal "$state2" || safe=0
  fi
  [[ -d "$target" ]] || safe=0
  if (( ! safe )); then flock -u "$lock_fd"; exec {lock_fd}>&-; count[state_changed]=$((count[state_changed]+1)); continue; fi
  # Markerless failures have no worker archive, so preserve a compact manager
  # archive before deletion. All commands here are deliberately best effort.
  mkdir -p "$archive"
  { printf 'sample_id\t%s\nmanager_status\t%s\nslurm_state\t%s\njob_task\t%s_%s\nwork_root\t%s\nsize_bytes\t%s\n' "$sample" "$current" "$state2" "$job" "$task" "$target" "$(du -sb "$target" 2>/dev/null|awk '{print $1+0}')"; } > "$archive/cleanup_context.tsv"
  sacct -j "${job}_${task}" --format=JobID,State,ExitCode,Elapsed -P > "$archive/sacct.tsv" 2>&1 || true
  find "$target" -maxdepth 2 -type f -name '*.log' -print 2>/dev/null | sort | awk 'NR<=10' > "$archive/log_paths.txt"
  while IFS= read -r logf; do printf '\n==> %s <==\n' "$logf"; tail -n 40 "$logf" 2>/dev/null || true; done < "$archive/log_paths.txt" > "$archive/log_tails.txt"
  touch "$archive/ARCHIVE_COMPLETE"
  bytes=$(du -sb "$target" 2>/dev/null|awk '{print $1+0}'); [[ "$DRY_RUN" == 1 ]] || rm -rf --one-file-system -- "$target"
  receipt="$MANAGER_ROOT/state/receipts/failed_sample_work_cleanup/$sample.$(date -u +%Y%m%dT%H%M%SZ).tsv"
  epoch=0; [[ ! -s "$marker" ]] || epoch=$(marker_field "$marker" first_failure_epoch)
  cleanup_reason=markerless_or_archived_terminal
  [[ "$kind" != held_pending ]] || cleanup_reason=manager_owned_held_pending
  [[ -s "$marker" || "$state2" != CANCELLED ]] || cleanup_reason=terminal_cancelled_without_failure_marker
  printf 'sample_id\tprevious_manager_status\tslurm_state\tcache_status\tcache_deleted_at\tcache_bytes_released\tretry_mode\twork_root\tcleanup_reason\tfirst_failure_epoch\n%s\t%s\t%s\tDELETED\t%s\t%s\tfresh\t%s\t%s\t%s\n' "$sample" "$current" "$state2" "$(now_iso)" "${bytes:-0}" "$target" "$cleanup_reason" "${epoch:-0}" > "$receipt"
  with_state_lock update_sample_fields "$sample" "notes=work cache reclaimed under disk pressure; future deferred retry must run fresh"
  flock -u "$lock_fd"; exec {lock_fd}>&-; count[deleted]=$((count[deleted]+1))
done < <(awk -F '\t' 'NR>1&&$4~/^(PIPELINE_SUBMITTED|PIPELINE_DEFERRED_RETRY|PIPELINE_DEFERRED_FAILED)$/{print $1"\t"$4}' "$STATUS_FILE")
for reason in missing_workdir validated_complete missing_archive active_slurm markerless_terminal_not_safe lock_busy state_changed deleted; do log "cleanup_skip_count $reason=${count[$reason]}"; done
log "Failed-cache cleanup stopped: eligible candidates exhausted (deleted ${count[deleted]})"
