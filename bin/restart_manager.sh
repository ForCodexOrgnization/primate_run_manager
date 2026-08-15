#!/usr/bin/env bash
# Default mode restarts ONLY primate_manager_daemon. Recovery never cancels,
# holds, releases, or requeues a live pipeline array.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"; source "$ROOT/lib/common.sh"
cfg="${1:-}"; shift || true; dry=0; skip=0; recover=0; current_user=${USER:-$(id -un)}
for arg in "$@"; do case "$arg" in --dry-run) dry=1;; --skip-cycle) skip=1;; --recover-cancelled) recover=1;; *) die "unknown option: $arg";; esac; done
((recover == 0 || skip == 0)) || die '--skip-cycle cannot be combined with --recover-cancelled'
[[ -s "$cfg" ]] || die "Config missing: ${cfg:-<unset>}"; cfg="$(cd "$(dirname "$cfg")"&&pwd)/$(basename "$cfg")"
MANAGER_ROOT=$(bash -c 'source "$1"; printf %s "$MANAGER_ROOT"' _ "$cfg"); mkdir -p "$MANAGER_ROOT/state/locks"; exec 9>"$MANAGER_ROOT/state/locks/manager_restart.lock"; flock -n 9 || die 'another restart is active'
"$SCRIPT_DIR/manager_restart_preflight.sh" "$cfg" --allow-scope-reconciliation; load_config "$cfg"; validate_config
commit=$(git -C "$ROOT" rev-parse HEAD 2>/dev/null||echo unknown); tree=clean; [[ -z "$(git -C "$ROOT" status --porcelain 2>/dev/null)" ]] || tree=dirty; echo "Git commit: $commit"; echo "Working tree: $tree$([[ $tree == dirty ]]&&echo ' (WARNING)'||true)"
daemons(){ squeue --noheader --user="$current_user" --name=primate_manager_daemon --states=RUNNING,PENDING --format='%A|%T|%j'|awk -F'|' '$3=="primate_manager_daemon"'; }
old=$(daemons); old_id=$(awk -F'|' 'NF{print $1;exit}'<<<"$old"); old_id=${old_id:-none}; active=$(awk -F'\t' 'NR>1&&$9~/^(CREATED|SUBMITTED|RUNNING)$/{print $1;exit}' "$WAVE_STATUS_FILE"); array=""; [[ -z $active ]]||array=$(wave_field "$active" pipeline_job_id); work=$(work_disk_used_percent)
recovery_plan=""
if ((recover)); then
  recovery_plan=$("$SCRIPT_DIR/reconcile_cancelled_submission.sh" "$cfg" --dry-run)
  if [[ $(awk -F= '$1=="nothing_to_recover"{print $2}' <<<"$recovery_plan") != 1 ]]; then
    active=$(awk -F= '$1=="submission_id"{print $2}' <<<"$recovery_plan")
    array=$(awk -F= '$1=="pipeline_job_id"{print $2}' <<<"$recovery_plan")
  fi
fi
if ((dry)); then
 if ((recover)); then
   echo 'Recovery dry run: no state, Slurm, daemon, hold, or filesystem changes made.'
   printf '%s\nOld submission: %s\nOld Slurm array: %s\nOld daemon: %s\nCurrent work filesystem: %s%%\nNew submission allowed by work disk: %s\nDaemon would be restarted: yes\n' "$recovery_plan" "${active:-none}" "${array:-none}" "$old_id" "$work" "$([[ $work -lt $WORK_STOP_SUBMIT_PERCENT ]] && echo yes || echo 'no (disk pressure)')"
 else
   printf 'Dry run: no Slurm or manager state changes made.\nPlan: snapshot, TERM daemon only, wait, reconcile, %s, submit/verify daemon, status.\nProtected pipeline array: %s\n' "$([[ $skip == 1 ]]&&echo 'skip cycle'||echo 'one guarded cycle')" "${array:-none}"
 fi
 exit 0
fi
# The plan above validates cancellation before the daemon is stopped.  The
# apply helper resolves the target and repeats both Slurm queries immediately
# before mutation, closing the verification race.
stamp=$(date -u +%Y%m%dT%H%M%SZ); snap="$MANAGER_ROOT/state/restart_snapshots/$stamp"; i=0; while [[ -e $snap ]]; do i=$((i+1)); snap="$MANAGER_ROOT/state/restart_snapshots/$stamp-$i"; done; mkdir -p "$snap"
for f in sample_status.tsv wave_status.tsv output_validation.tsv manager_phase.tsv transfer_tasks.tsv streaming_array_disk_holds.tsv; do [[ ! -e "$MANAGER_ROOT/state/$f" ]]||cp -p "$MANAGER_ROOT/state/$f" "$snap/"; done
for d in submission_task_map array_sample_map; do [[ ! -d "$MANAGER_ROOT/state/$d" ]]||cp -a "$MANAGER_ROOT/state/$d" "$snap/"; done
pipeline_commit=$(git -C "$PIPELINE_REPO" rev-parse HEAD 2>/dev/null || echo unknown)
printf 'timestamp\t%s\nhostname\t%s\nuser\t%s\nconfig_path\t%s\ngit_commit\t%s\npipeline_git_commit\t%s\nold_daemon_job_id\t%s\nactive_pipeline_submission_id\t%s\nactive_pipeline_slurm_array_id\t%s\nwork_filesystem_usage_percent\t%s\noperation_mode\t%s\n' "$(date -u +%FT%TZ)" "$(hostname)" "$current_user" "$cfg" "$commit" "$pipeline_commit" "$old_id" "${active:-none}" "${array:-none}" "$work" "$([[ $recover == 1 ]] && echo recover_cancelled || echo daemon_restart)" >"$snap/restart_metadata.tsv"
"$SCRIPT_DIR/stop_manager_daemon.sh" "$cfg"
timeout=${MANAGER_RESTART_STOP_TIMEOUT_SECONDS:-120}; poll=${MANAGER_RESTART_POLL_SECONDS:-2}; [[ $timeout =~ ^[1-9][0-9]*$ && $poll =~ ^[1-9][0-9]*$ ]]||die 'invalid restart timeout'; deadline=$((SECONDS+timeout))
while [[ -n "$(daemons)" ]]; do ((SECONDS<deadline))||die "daemon stop timeout after ${timeout}s; no new daemon submitted"; echo "Waiting for manager daemon $old_id..."; sleep "$poll"; done
exec 8>"$MANAGER_ROOT/state/locks/manager_cycle.lock"; flock -n 8||die 'manager-cycle lock remains busy'
recovery_result=""
if ((recover)); then recovery_result=$("$SCRIPT_DIR/reconcile_cancelled_submission.sh" "$cfg" --apply); fi
# This never cancels work: unsafe/active samples make reconciliation fail.
"$SCRIPT_DIR/reconcile_assigned_sample_scope.sh" "$cfg"
# The normal cycle's active_submission_count/phase protections are the duplicate
# submission guard; restart does not bypass or replace them.
if ((skip)); then :; elif ((recover)); then MANAGER_CYCLE_LOCK_HELD=1 MANAGER_RECOVERY_MODE=1 "$SCRIPT_DIR/manager_cycle.sh" "$cfg"; else flock -u 8; "$SCRIPT_DIR/manager_cycle.sh" "$cfg"; fi
flock -u 8
# Validate the reconciled state before installing the replacement daemon.
"$SCRIPT_DIR/manager_restart_preflight.sh" "$cfg"
if ! out=$("$SCRIPT_DIR/submit_manager_daemon.sh" "$cfg"); then
    printf '%s\n' \
        'ERROR: manager cycle/reconciliation completed, but new daemon submission failed.' \
        'Manager state is preserved.' \
        'No pipeline array was modified.' \
        'Check Slurm partition/QOS/walltime and rerun submit_manager_daemon.sh.' >&2
    exit 1
fi
echo "$out"; new=$(awk '/Submitted manager daemon Slurm job/{print $6}'<<<"$out"|tail -1); [[ -n $new ]]||die 'new daemon ID not captured'; sleep "${MANAGER_RESTART_VERIFY_DELAY_SECONDS:-2}"
after=$(daemons); [[ $(awk 'NF{n++}END{print n+0}'<<<"$after") == 1 ]]||die 'new daemon verification count failed'; state=$(awk -F'|' -v id="$new" '$1==id{print $2}'<<<"$after"); [[ -n $state ]]||die "daemon $new not RUNNING/PENDING"
if ((recover)); then
 new_active=$(awk -F'\t' 'NR>1&&$9~/^(CREATED|SUBMITTED|RUNNING)$/{print $1;exit}' "$WAVE_STATUS_FILE")
 array_after=""; [[ -z "$new_active" ]] || array_after=$(wave_field "$new_active" pipeline_job_id)
else
 if [[ -n $active ]]; then array_after=$(wave_field "$active" pipeline_job_id); [[ $array_after == "$array" ]]||die 'pipeline array identity changed'; else array_after=; fi
fi
phase=$(manager_phase); hold="$MANAGER_ROOT/state/streaming_array_disk_holds.tsv"; held=0; [[ ! -s $hold ]]||held=$(awk 'END{print NR-1}' "$hold"); count(){ awk -F'\t' -v r="$1" 'NR>1&&$4~r{n++}END{print n+0}' "$STATUS_FILE"; }
printf '\nManager restart successful\n--------------------------\nConfig: %s\nOld daemon: %s STOPPED\nNew daemon: %s %s\nPipeline array: %s%s\nPipeline mode: %s\nSample concurrency: %s\nManager phase: %s\nWork filesystem: %s%%\nDisk admission: %s\n\nSamples:\n  retained: %s\n  ready_to_transfer: %s\n  deferred_retry: %s\n  submitted/running: %s\n\nRestart snapshot:\n  %s\n\n' "$cfg" "$old_id" "$new" "$state" "${array_after:-none}" "$([[ -n $array ]]&&echo ' (unchanged)'||true)" "$PIPELINE_MODE" "$SAMPLE_CHAIN_CONCURRENCY" "$phase" "$(work_disk_used_percent)" "$([[ $held -gt 0 ]]&&echo HELD||echo ACTIVE)" "$(count '^LOCAL_FINAL_RETAINED$')" "$(count '^READY_TO_TRANSFER$')" "$(count '^PIPELINE_DEFERRED_RETRY$')" "$(count '^(PIPELINE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_DEFERRED_RUNNING)$')" "$snap"
if ((recover)); then
 printf '\nManager recovery successful\n---------------------------\nManager git commit: %s\nPipeline git commit: %s\nOld submission: %s\nOld Slurm array: %s\nOld submission reconciled as: CANCELLED\nNew submission: %s\nNew Slurm array: %s\nRecovered submission details:\n%s\n' "$commit" "$pipeline_commit" "${active:-none}" "${array:-none}" "${new_active:-none}" "${array_after:-not submitted: disk pressure or no runnable samples}" "$recovery_result"
 if [[ $(work_disk_used_percent) -ge $WORK_STOP_SUBMIT_PERCENT && -z ${array_after:-} ]]; then echo 'recovery complete; pipeline resubmission deferred by disk pressure'; fi
fi
if ! "$SCRIPT_DIR/show_status.sh" "$cfg"; then
    printf 'WARNING: manager daemon %s restarted successfully, but final status display failed; run bin/show_status.sh %q for details.\n' "$new" "$cfg" >&2
fi
