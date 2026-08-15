#!/usr/bin/env bash
# Retire historical manager rows which no longer belong to the assigned cohort.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$SCRIPT_DIR/../lib/common.sh"
[[ $# -ge 1 && $# -le 2 ]] || die "Usage: $0 CONFIG [--dry-run]"
cfg=$1; dry=0; [[ ${2:-} != --dry-run ]] || dry=1
[[ $# == 1 || ${2:-} == --dry-run ]] || die "Usage: $0 CONFIG [--dry-run]"
load_config "$cfg"; validate_config; [[ -s "$STATUS_FILE" ]] || die "Missing manager state: $STATUS_FILE"
mkdir -p "$MANAGER_ROOT/state/receipts/sample_scope_reconciliation" "$PIPELINE_WORK_ROOT/.locks"
# A scan writes output_validation under the manager cycle. Holding this lock for
# the reconciliation closes the check/update race and proves validation is idle.
exec 8>"$MANAGER_ROOT/state/locks/manager_cycle.lock"
flock -n 8 || die "manager cycle/output validation is active; scope reconciliation refused"

assigned_tmp=$(mktemp); trap 'rm -f "$assigned_tmp"' EXIT
awk -F '\t' 'NF{print $1}' "$ASSIGNED_SAMPLE_LIST" > "$assigned_tmp"
is_assigned(){ awk -v s="$1" '$1==s{yes=1}END{exit !yes}' "$assigned_tmp"; }
active_status(){ case "$1" in PIPELINE_SUBMITTED|WAVE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_RETRY_RUNNING|PIPELINE_DEFERRED_RUNNING|TRANSFERRING) return 0;; *) return 1;; esac; }
exact_task_active(){
  local sample=$1 row job task state
  row=$(awk -F '\t' -v s="$sample" 'NR>1&&$8==s{r=$0}END{print r}' "$MANAGER_ROOT"/state/submission_task_map/*.tsv 2>/dev/null || true)
  [[ -z $row ]] || { job=$(cut -f4 <<<"$row"); task=$(cut -f5 <<<"$row"); state=$(squeue --noheader --jobs="${job}_${task}" --format='%T' 2>/dev/null | awk 'NF{print;exit}' || true); [[ -n $state ]] || state=$(submission_task_state "$job" "$task"); slurm_state_is_active "$state" && return 0; }
  return 1
}

changed=0; blocked=0
while IFS=$'\t' read -r sample status job wave attempts; do
  is_assigned "$sample" && continue
  if [[ $status == OUT_OF_SCOPE ]]; then printf 'UNCHANGED\t%s\tOUT_OF_SCOPE\n' "$sample"; continue; fi
  reason=""
  active_status "$status" && reason="manager status is active ($status)"
  [[ -n $reason ]] || ! exact_task_active "$sample" || reason="exact submission task is active"
  exec {sample_fd}>"$PIPELINE_WORK_ROOT/.locks/${sample}.lock"
  if [[ -z $reason ]] && ! flock -n "$sample_fd"; then reason="sample lock is busy"; fi
  if [[ -n $reason ]]; then printf 'BLOCKED\t%s\t%s\n' "$sample" "$reason"; blocked=$((blocked+1)); exec {sample_fd}>&-; continue; fi
  if ((dry)); then printf 'WOULD_RETIRE\t%s\t%s\n' "$sample" "$status"; flock -u "$sample_fd"; exec {sample_fd}>&-; continue; fi
  ts=$(now_iso); note="sample removed from current assigned sample list; retired as OUT_OF_SCOPE at $ts; previous_status=$status"
  with_state_lock update_sample_fields "$sample" status=OUT_OF_SCOPE "notes=$note"
  receipt="$MANAGER_ROOT/state/receipts/sample_scope_reconciliation/${sample}.${ts//[:\-]/}.tsv"
  printf 'sample_id\tprevious_status\tnew_status\ttimestamp\tassigned_list\told_slurm_job_id\told_wave_id\treason\n%s\t%s\tOUT_OF_SCOPE\t%s\t%s\t%s\t%s\tnot_in_assigned_sample_list\n' "$sample" "$status" "$ts" "$ASSIGNED_SAMPLE_LIST" "$job" "$wave" > "$receipt"
  printf 'RETIRED\t%s\t%s\tOUT_OF_SCOPE\n' "$sample" "$status"; changed=$((changed+1)); flock -u "$sample_fd"; exec {sample_fd}>&-
done < <(awk -F '\t' 'NR>1{print $1,$4,$5,$6,$7}' OFS='\t' "$STATUS_FILE")
printf 'Scope reconciliation: retired=%s blocked=%s dry_run=%s\n' "$changed" "$blocked" "$dry"
((blocked == 0))
