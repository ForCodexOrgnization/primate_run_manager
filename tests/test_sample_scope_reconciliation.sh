#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test_helper.sh"; new_env
source "$REPO/lib/common.sh"; load_config "$T/config.sh"; ensure_state_files
bash "$REPO/bin/initialize_samples.sh" "$T/config.sh"
# s1 is removed and terminal; s2 remains assigned; s3 is removed but active.
printf 's2\tsp2\n' > "$ASSIGNED_SAMPLE_LIST"
awk -F '\t' -v OFS='\t' 'NR==1{print;next}$1=="s1"{$4="PIPELINE_DEFERRED_RETRY";$5="700";$6="old";$7=4}$1=="s3"{$4="PIPELINE_RUNNING";$5="701";$6="live"}{print}' "$STATUS_FILE" > "$T/x"; mv "$T/x" "$STATUS_FILE"
cat > "$T/mockbin/squeue" <<'EOF'
#!/usr/bin/env bash
case "$*" in *701*) echo RUNNING;; esac
EOF
cat > "$T/mockbin/sacct" <<'EOF'
#!/usr/bin/env bash
case "$*" in *700*) echo '700_1|CANCELLED|' ;; *701*) echo '701_1|RUNNING|' ;; esac
EOF
chmod +x "$T/mockbin/squeue" "$T/mockbin/sacct"
mkdir -p "$MANAGER_ROOT/state/submission_task_map"
printf 'submission_id\tpipeline_mode\tphase\tslurm_array_job_id\tarray_task_id\ttask_type\ttask_name\tsample_id\treference_name\tsample_work_root\tbatch_work_root\nold\tstreaming_per_sample\tNORMAL\t700\t1\tSAMPLE\ts1\ts1\tsp1\t%s/s1\t\nlive\tstreaming_per_sample\tNORMAL\t701\t1\tSAMPLE\ts3\ts3\tsp3\t%s/s3\t\n' "$PIPELINE_WORK_ROOT" "$PIPELINE_WORK_ROOT" > "$MANAGER_ROOT/state/submission_task_map/maps.tsv"

if bash "$REPO/bin/reconcile_assigned_sample_scope.sh" "$T/config.sh" > "$T/reconcile"; then echo 'active sample did not block reconciliation' >&2; exit 1; fi
assert awk -F '\t' '$1=="s1"&&$4=="OUT_OF_SCOPE"&&$5==700&&$6=="old"&&$7==4{ok=1}END{exit !ok}' "$STATUS_FILE"
assert awk -F '\t' '$1=="s2"&&$4=="PENDING"{ok=1}END{exit !ok}' "$STATUS_FILE"
assert awk -F '\t' '$1=="s3"&&$4=="PIPELINE_RUNNING"{ok=1}END{exit !ok}' "$STATUS_FILE"
assert grep -q 'not_in_assigned_sample_list' "$MANAGER_ROOT"/state/receipts/sample_scope_reconciliation/*.tsv

# Busy sample locks are conservative and dry-run never mutates state.
awk -F '\t' -v OFS='\t' '$1=="s3"{$4="PIPELINE_DEFERRED_RETRY"}{print}' "$STATUS_FILE" > "$T/x"; mv "$T/x" "$STATUS_FILE"
exec 8>"$PIPELINE_WORK_ROOT/.locks/s3.lock"; flock -x 8
before=$(sha256sum "$STATUS_FILE")
! bash "$REPO/bin/reconcile_assigned_sample_scope.sh" "$T/config.sh" --dry-run > "$T/dry"
[[ $before == "$(sha256sum "$STATUS_FILE")" ]]; flock -u 8

# Preflight accepts historical rows, but blocks runnable extras and re-added rows.
awk -F '\t' -v OFS='\t' '$1=="s3"{$4="OUT_OF_SCOPE"}{print}' "$STATUS_FILE" > "$T/x"; mv "$T/x" "$STATUS_FILE"
out=$(bash "$REPO/bin/manager_restart_preflight.sh" "$T/config.sh"); grep -q 'historical OUT_OF_SCOPE samples: 2' <<< "$out"
awk -F '\t' -v OFS='\t' '$1=="s3"{$4="PENDING"}{print}' "$STATUS_FILE" > "$T/x"; mv "$T/x" "$STATUS_FILE"
! bash "$REPO/bin/manager_restart_preflight.sh" "$T/config.sh" > "$T/preflight" 2>&1; grep -q 'reconcile_assigned_sample_scope' "$T/preflight"
awk -F '\t' -v OFS='\t' '$1=="s3"{$4="OUT_OF_SCOPE"}{print}' "$STATUS_FILE" > "$T/x"; mv "$T/x" "$STATUS_FILE"; printf 's2\tsp2\ns3\tsp3\n' > "$ASSIGNED_SAMPLE_LIST"
! bash "$REPO/bin/manager_restart_preflight.sh" "$T/config.sh" > "$T/readded" 2>&1; grep -q 'explicit audited reactivation' "$T/readded"

# Submission and deferred/requeue selection are cohort-and-status allowlists.
! rg -q 'OUT_OF_SCOPE' <(awk -F '\t' '$4~/^(PENDING|PIPELINE_DEFERRED_RETRY)$/{print $1}' "$STATUS_FILE")
grep -q '($1 in a)' "$REPO/bin/submit_next_batch.sh"
grep -q 'sample_id.*in cohort' "$REPO/bin/requeue_unsuccessful_samples.sh"
echo 'sample scope reconciliation tests passed'
