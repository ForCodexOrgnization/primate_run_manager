#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files; determine_manager_phase
count_status(){ awk -F '\t' -v s="$1" 'NR>1&&$4==s{n++}END{print n+0}' "$STATUS_FILE"; }
printf 'Pipeline mode: %s\nManager phase: %s\n' "$PIPELINE_MODE" "$(manager_phase)"
daemon_file="$MANAGER_ROOT/state/manager_daemon_status.tsv"
if [[ -s "$daemon_file" ]]; then
 printf 'Daemon job ID: %s\nDaemon code SHA: %s\nDaemon repository SHA: %s\n' "$(awk -F '\t' 'NR==2{print $1}' "$daemon_file")" "$(awk -F '\t' 'NR==2{print $3}' "$daemon_file")" "$(awk -F '\t' 'NR==2{print $4}' "$daemon_file")"
else printf 'Daemon job ID: unknown\nDaemon code SHA: unknown\nDaemon repository SHA: %s\n' "$(git_commit_or_unknown)"; fi
if [[ -s "$GLOBUS_HEALTH_FILE" ]]; then
 printf 'Globus health: %s\nGlobus last operation: %s\nGlobus last check: %s\n' "$(awk -F '\t' 'NR==2{print $1}' "$GLOBUS_HEALTH_FILE")" "$(awk -F '\t' 'NR==2{print $2}' "$GLOBUS_HEALTH_FILE")" "$(awk -F '\t' 'NR==2{print $4}' "$GLOBUS_HEALTH_FILE")"
else printf 'Globus health: UNKNOWN (not checked)\n'; fi
printf 'Active transfer count: %s\n' "$(awk -F '\t' 'NR>1&&$3=="ACTIVE"{n++}END{print n+0}' "$TRANSFER_TASK_FILE")"
if [[ -s "$MANAGER_CYCLE_STATUS_FILE" ]]; then printf 'Last cycle status: %s (rc=%s, finished=%s, code=%s)\n' "$(awk -F '\t' 'NR==2{print $1}' "$MANAGER_CYCLE_STATUS_FILE")" "$(awk -F '\t' 'NR==2{print $2}' "$MANAGER_CYCLE_STATUS_FILE")" "$(awk -F '\t' 'NR==2{print $4}' "$MANAGER_CYCLE_STATUS_FILE")" "$(awk -F '\t' 'NR==2{print $5}' "$MANAGER_CYCLE_STATUS_FILE")"; else printf 'Last cycle status: UNKNOWN\n'; fi
printf 'Pending samples: %s\nReady to transfer: %s\nDeferred retry: %s\nDeferred failed: %s\nOUT_OF_SCOPE: %s\n' "$(count_status PENDING)" "$(count_status READY_TO_TRANSFER)" "$(count_status PIPELINE_DEFERRED_RETRY)" "$(count_status PIPELINE_DEFERRED_FAILED)" "$(count_status OUT_OF_SCOPE)"
active=$(awk -F '\t' 'NR>1&&$9~/^(CREATED|SUBMITTED|RUNNING)$/{print $1;exit}' "$WAVE_STATUS_FILE"); printf 'Active submission: %s\n' "${active:-none}"
job=""; [[ -z "$active" ]] || job=$(wave_field "$active" pipeline_job_id); printf 'Slurm array job ID: %s\n' "${job:-none}"
if [[ "$PIPELINE_MODE" == streaming_per_sample ]]; then
 printf 'Streaming submission window: %s\n' "$([[ $STREAMING_SUBMISSION_WINDOW == 0 ]] && echo unlimited || echo $STREAMING_SUBMISSION_WINDOW)"
 printf 'Global sample concurrency target: %s\n' "$SAMPLE_CHAIN_CONCURRENCY"
 hold_file="${MANAGER_ROOT}/state/streaming_array_disk_holds.tsv"
 held=0; [[ ! -s "$hold_file" ]] || held=$(awk 'END{print (NR > 0 ? NR - 1 : 0)}' "$hold_file")
 printf 'Disk pressure array admission: %s\nArray elements held by manager: %s\nArray release threshold: %s%%\n' "$([[ "$held" -gt 0 ]] && echo HELD || echo ACTIVE)" "$held" "$WORK_ARRAY_RELEASE_PERCENT"
 if (( held > 0 )); then printf 'Array hold reason: %s\n' "$(awk -F '\t' 'NR==2{print $4;exit}' "$hold_file")"; fi
 printf 'Manager lifecycle submitted: %s\nManager lifecycle executing: %s\n' "$(count_status PIPELINE_SUBMITTED)" "$(awk -F '\t' 'NR>1&&$4~/^(PIPELINE_RUNNING|PIPELINE_DEFERRED_RUNNING)$/{n++}END{print n+0}' "$STATUS_FILE")"
 live_running=0; live_pending=0; live_held=0
 if command -v squeue >/dev/null 2>&1; then
   while IFS='|' read -r st reason; do
     case "$st" in RUNNING|CONFIGURING|COMPLETING) live_running=$((live_running+1));; PENDING) live_pending=$((live_pending+1)); [[ "$reason" == JobHeldUser || "$reason" == JobHeldAdmin ]] && live_held=$((live_held+1));; esac
   done < <(jobs=$(awk -F '\t' 'NR>1&&$9~/^(CREATED|SUBMITTED|RUNNING)$/&&$4~/^[0-9]+$/{print $4}' "$WAVE_STATUS_FILE" | sort -u | paste -sd,); [[ -z "$jobs" ]] || squeue --noheader --array --jobs="$jobs" --format='%T|%r' 2>/dev/null)
 fi
 printf 'Actual live Slurm RUNNING: %s\nActual live Slurm PENDING: %s\nActual live Slurm HELD: %s\n' "$live_running" "$live_pending" "$live_held"
 ledger="$MANAGER_ROOT/state/streaming_array_holds.tsv"
 resume_priority=0; global_held=0; disk_held=0
 if [[ -s "$ledger" ]]; then
   resume_priority=$(awk -F '\t' 'NR>1&&$3=="RESUME_PRIORITY"{n++}END{print n+0}' "$ledger")
   global_held=$(awk -F '\t' 'NR>1&&$3=="GLOBAL_CONCURRENCY"{n++}END{print n+0}' "$ledger")
   disk_held=$(awk -F '\t' 'NR>1&&$3=="DISK_PRESSURE"{n++}END{print n+0}' "$ledger")
 fi
 admission_status="$MANAGER_ROOT/state/streaming_admission_status.tsv"
 resume_waiting=0; [[ ! -s "$admission_status" ]] || resume_waiting=$(awk -F '\t' 'NR==2{print $1+0}' "$admission_status")
 active_count=$(active_submission_count)
 printf 'Resume candidates waiting: %s\nFresh tasks held for resume priority: %s\nTasks held for global concurrency: %s\nTasks held for disk pressure: %s\nActive streaming submissions: %s\n' "$resume_waiting" "$resume_priority" "$global_held" "$disk_held" "$active_count"
else
 printf 'Batch size: %s\nBatch concurrency: %s\n' "$PIPELINE_BATCH_SIZE" "$CHAIN_CONCURRENT_BATCHES"
 if [[ -n "$active" && -s "${MANAGER_ROOT}/state/submission_task_map/${active}.tsv" ]]; then printf 'Total batches in active submission: %s\n' "$(awk -F '\t' 'NR>1&&!seen[$5]++{n++}END{print n+0}' "${MANAGER_ROOT}/state/submission_task_map/${active}.tsv")"; fi
fi
printf 'Work filesystem usage: %s%%\nTotal samples: %s\nCounts by status:\n' "$(work_disk_used_percent)" "$(awk 'END{print NR-1}' "$STATUS_FILE")"
awk -F '\t' 'NR>1{n[$4]++}END{for(s in n)printf "  %-24s %d\n",s,n[s]}' "$STATUS_FILE" | sort
