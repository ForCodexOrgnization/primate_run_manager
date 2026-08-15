#!/usr/bin/env bash
# Read-only restart checks.  In particular, do not call ensure_state_files here.
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$SCRIPT_DIR/../lib/common.sh"
cfg="${1:-}"; blocked=0; warnings=0; current_user=${USER:-$(id -un)}
block(){ echo "  BLOCKED: $*"; blocked=1; }; warn(){ echo "  WARNING: $*"; warnings=1; }
if [[ -z "$cfg" || ! -s "$cfg" ]] || ! bash -n "$cfg"; then echo "Restart readiness: BLOCKED"; echo "  BLOCKED: invalid config: ${cfg:-<unset>}"; exit 2; fi
if ! load_config "$cfg" || ! validate_config; then echo 'Restart readiness: BLOCKED'; echo '  BLOCKED: config validation failed'; exit 2; fi
echo 'Manager restart preflight'; echo "Config: $cfg"
scripts=(manager_cycle.sh submit_manager_daemon.sh stop_manager_daemon.sh show_status.sh ingest_sample_markers.sh)
[[ "$PIPELINE_MODE" != streaming_per_sample ]] || scripts+=(control_streaming_array_admission.sh)
for s in "${scripts[@]}"; do [[ -x "$SCRIPT_DIR/$s" ]] && bash -n "$SCRIPT_DIR/$s" || block "required script invalid: bin/$s"; done
echo "Required scripts: $([[ $blocked == 0 ]] && echo OK || echo ERROR)"

lines=""; if command -v squeue >/dev/null; then lines=$(squeue --noheader --user="$current_user" --name=primate_manager_daemon --states=RUNNING,PENDING --format='%A|%T|%j' 2>/dev/null | awk -F'|' '$3=="primate_manager_daemon"') || block 'Slurm daemon query failed'; else block 'squeue not found'; fi
n=$(awk 'NF{n++}END{print n+0}' <<<"$lines"); echo 'Manager daemon:'
if ((n>1)); then echo "  ERROR: multiple daemons ($n)"; block 'multiple active manager daemons'
elif ((n==1)); then IFS='|' read -r id state _ <<<"$lines"; echo "  $state job $id"
else echo '  NOT RUNNING'; warn 'no daemon currently running'; fi

exec 8>"$MANAGER_ROOT/state/locks/manager_cycle.lock"
if flock -n 8; then echo 'Manager cycle lock: FREE'; flock -u 8; else echo 'Manager cycle lock: BUSY'; warn 'a manager cycle is currently running'; fi

echo 'Sample state:'; total=0
if [[ ! -s "$STATUS_FILE" ]]; then block "missing state file: $STATUS_FILE"
else
 [[ "$(head -n1 "$STATUS_FILE")" == "$(state_header)" ]] || block 'manager state schema invalid'
 total=$(awk 'END{print NR-1}' "$STATUS_FILE"); statuses=(PENDING PIPELINE_SUBMITTED PIPELINE_RUNNING PIPELINE_DEFERRED_RETRY PIPELINE_DEFERRED_RUNNING PIPELINE_DEFERRED_FAILED READY_TO_TRANSFER TRANSFERRING LOCAL_FINAL_RETAINED); sum=0
 for s in "${statuses[@]}"; do c=$(awk -F'\t' -v s="$s" 'NR>1&&$4==s{n++}END{print n+0}' "$STATUS_FILE"); printf '  %-28s %s\n' "$s" "$c"; sum=$((sum+c)); done
 other=$(awk -F'\t' 'NR>1&&$4!~/^(PENDING|PIPELINE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_DEFERRED_RETRY|PIPELINE_DEFERRED_RUNNING|PIPELINE_DEFERRED_FAILED|READY_TO_TRANSFER|TRANSFERRING|LOCAL_FINAL_RETAINED)$/{n++}END{print n+0}' "$STATUS_FILE"); sum=$((sum+other)); printf '  %-28s %s\n  TOTAL                        %s\n' other/unknown "$other" "$total"
 [[ $sum == "$total" ]] || block "state count sum $sum != rows $total"
 dup=$(awk -F'\t' 'NR>1&&++a[$1]==2{n++}END{print n+0}' "$STATUS_FILE"); adup=$(awk -F'\t' 'NF&&++a[$1]==2{n++}END{print n+0}' "$ASSIGNED_SAMPLE_LIST")
 missing=$(awk -F'\t' 'NR==FNR{if(FNR>1)a[$1]=1;next}NF&&!($1 in a){n++}END{print n+0}' "$STATUS_FILE" "$ASSIGNED_SAMPLE_LIST"); extra=$(awk -F'\t' 'NR==FNR{a[$1]=1;next}FNR>1&&!($1 in a){n++}END{print n+0}' "$ASSIGNED_SAMPLE_LIST" "$STATUS_FILE")
 echo "  missing manager samples: $missing"; echo "  manager samples not assigned: $extra"; echo "  duplicates: $dup manager, $adup assigned"
 ((dup==0 && adup==0)) || block 'duplicate sample IDs'; ((missing==0 && extra==0)) || block 'manager/assignment sample mismatch'
 (( $(awk -F'\t' 'NR>1&&$4=="PIPELINE_DEFERRED_RETRY"{n++}END{print n+0}' "$STATUS_FILE") == 0 )) || warn 'deferred retry samples exist'
 (( $(awk -F'\t' 'NR>1&&$4=="READY_TO_TRANSFER"{n++}END{print n+0}' "$STATUS_FILE") == 0 )) || warn 'samples await transfer'
fi

active=""
if [[ ! -s "$WAVE_STATUS_FILE" ]]; then block "missing state file: $WAVE_STATUS_FILE"
else nf=$(awk -F'\t' 'NR==1{print NF}' "$WAVE_STATUS_FILE"); [[ $nf == 21 || $nf == 22 ]] || block 'wave state schema invalid'; active=$(awk -F'\t' 'NR>1&&$9~/^(CREATED|SUBMITTED|RUNNING)$/{print $1}' "$WAVE_STATUS_FILE"); fi
an=$(awk 'NF{n++}END{print n+0}' <<<"$active")
if ((an>1)); then block 'multiple active submissions'
elif ((an==1)); then sid="$active"; job=$(wave_field "$sid" pipeline_job_id); map="$MANAGER_ROOT/state/submission_task_map/$sid.tsv"; [[ -s "$map" ]] || block "active submission missing task map: $map"; mapped=0; [[ ! -s "$map" ]] || mapped=$(awk 'END{print NR-1}' "$map"); managed=$(awk -F'\t' 'NR>1&&$4~/^(PIPELINE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_DEFERRED_RUNNING)$/{n++}END{print n+0}' "$STATUS_FILE"); slurm=$(squeue --noheader --jobs="$job" --format='%T' 2>/dev/null | awk 'NF{print;exit}' || true); printf 'Active submission:\n  submission_id: %s\n  Slurm array job ID: %s\n  mapped sample count: %s\n  manager submitted/running count: %s\n  current Slurm state: %s\n' "$sid" "$job" "$mapped" "$managed" "${slurm:-not in squeue}"; warn 'active pipeline array is supported and will not be modified'
else echo 'Active submission: none'; fi

used=$(work_disk_used_percent 2>/dev/null) || { used=unknown; warn 'work filesystem inaccessible'; }; hold="$MANAGER_ROOT/state/streaming_array_disk_holds.tsv"; held=0; [[ ! -s "$hold" ]] || held=$(awk 'END{print NR-1}' "$hold"); admission=$([[ $held -gt 0 ]]&&echo HELD||echo ACTIVE)
printf 'Work filesystem:\n  PIPELINE_WORK_ROOT: %s\n  used: %s%%\n  stop/critical/release: %s%%/%s%%/%s%%\n  disk-pressure admission: %s\n  manager-held array elements: %s\n' "$PIPELINE_WORK_ROOT" "$used" "$WORK_STOP_SUBMIT_PERCENT" "$WORK_CRITICAL_PERCENT" "$WORK_ARRAY_RELEASE_PERCENT" "$admission" "$held"
[[ $used == unknown ]] || ((used<WORK_STOP_SUBMIT_PERCENT)) || warn 'work disk pressure'
ru=$(df -P "$LOCAL_RESULTS" 2>/dev/null|awk 'NR==2{print $5}'); dirs=$(local_sample_dir_count 2>/dev/null||echo unknown); ready=$(awk -F'\t' 'NR>1&&$4=="READY_TO_TRANSFER"{n++}END{print n+0}' "$STATUS_FILE" 2>/dev/null||echo 0); trans=$(awk -F'\t' 'NR>1&&$4=="TRANSFERRING"{n++}END{print n+0}' "$STATUS_FILE" 2>/dev/null||echo 0)
printf 'Results/transfer:\n  LOCAL_RESULTS usage: %s\n  local sample directories: %s\n  READY_TO_TRANSFER: %s\n  TRANSFERRING: %s\n' "${ru:-unknown}" "$dirs" "$ready" "$trans"
if ((blocked)); then result=BLOCKED; rc=2; elif ((warnings)); then result=WARNING; rc=0; else result=READY; rc=0; fi; echo "Restart readiness: $result"; exit $rc
