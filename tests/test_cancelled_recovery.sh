#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test_helper.sh"; new_env
source "$REPO/lib/common.sh"; load_config "$T/config.sh"; ensure_state_files
bash "$REPO/bin/initialize_samples.sh" "$T/config.sh"

cat >"$T/mockbin/squeue" <<'EOF'
#!/usr/bin/env bash
[[ -e "$TEST_ROOT/live" ]] && echo RUNNING
if [[ -e "$TEST_ROOT/invalid_job" ]]; then echo 'squeue: error: Invalid job id specified' >&2; exit 1; fi
if [[ -e "$TEST_ROOT/controller_error" ]]; then echo 'slurm_load_jobs error: Socket timed out' >&2; exit 1; fi
exit 0
EOF
cat >"$T/mockbin/sacct" <<'EOF'
#!/usr/bin/env bash
[[ -e "$TEST_ROOT/unknown" ]] && echo '12345_1|UNKNOWN|' || echo '12345_1|CANCELLED by 1000|'
EOF
chmod +x "$T/mockbin/squeue" "$T/mockbin/sacct"

# One cancelled deferred submission with an immutable membership map.
printf 'old\tmanifest\t3\t12345\tnow\tPENDING\t0\t3\tSUBMITTED\tnow\told\t%s\t\told\t\t0\tx\tx\tx\t\tunknown\n' "$T/work" >>"$WAVE_STATUS_FILE"
cat >"$MANAGER_ROOT/state/submission_task_map/old.tsv" <<'EOF'
submission_id	pipeline_mode	phase	slurm_array_job_id	array_task_id	task_type	task_name	sample_id	reference_name	sample_work_root	batch_work_root
old	streaming_per_sample	DEFERRED_RETRY	12345	1	SAMPLE	s1	s1	sp1	/work/s1	
old	streaming_per_sample	DEFERRED_RETRY	12345	2	SAMPLE	s2	s2	sp2	/work/s2	
old	streaming_per_sample	DEFERRED_RETRY	12345	3	SAMPLE	s3	s3	sp3	/work/s3	
EOF
awk -F'\t' -v OFS='\t' 'NR==1{print;next}{$4="PIPELINE_DEFERRED_RUNNING";$5=12345;$6="old";$7=4;if($1=="s3")$4="PIPELINE_DEFERRED_FAILED";print}' "$STATUS_FILE" >"$STATUS_FILE.x"; mv "$STATUS_FILE.x" "$STATUS_FILE"
validation_header >"$VALIDATION_FILE"; printf 's2\t1\t1\t1\t1\t1\t1\t1\tnow\tok\n' >>"$VALIDATION_FILE"

before=$(sha256sum "$STATUS_FILE" "$WAVE_STATUS_FILE")
out=$(bash "$REPO/bin/reconcile_cancelled_submission.sh" "$T/config.sh" --dry-run)
[[ "$before" == "$(sha256sum "$STATUS_FILE" "$WAVE_STATUS_FILE")" ]]
grep -q '^retry=1$' <<<"$out"; grep -q '^complete=1$' <<<"$out"; grep -q '^preserved_failed=1$' <<<"$out"

touch "$T/live"
if bash "$REPO/bin/reconcile_cancelled_submission.sh" "$T/config.sh" --apply >"$T/live.out" 2>&1; then echo 'live array accepted' >&2; exit 1; fi
grep -q 'active pipeline work still exists' "$T/live.out"; [[ "$before" == "$(sha256sum "$STATUS_FILE" "$WAVE_STATUS_FILE")" ]]; rm "$T/live"
touch "$T/unknown"
if bash "$REPO/bin/reconcile_cancelled_submission.sh" "$T/config.sh" --apply >"$T/unknown.out" 2>&1; then echo 'ambiguous accounting accepted' >&2; exit 1; fi
grep -q 'does not establish terminal array elements' "$T/unknown.out"; [[ "$before" == "$(sha256sum "$STATUS_FILE" "$WAVE_STATUS_FILE")" ]]; rm "$T/unknown"

# An old job absent from squeue is safe, but unrelated query failures block.
touch "$T/invalid_job"
bash "$REPO/bin/reconcile_cancelled_submission.sh" "$T/config.sh" --dry-run >/dev/null
rm "$T/invalid_job"; touch "$T/controller_error"
if bash "$REPO/bin/reconcile_cancelled_submission.sh" "$T/config.sh" --dry-run >"$T/controller.out" 2>&1; then echo 'controller failure accepted' >&2; exit 1; fi
grep -q 'Slurm squeue query failed' "$T/controller.out"; rm "$T/controller_error"

bash "$REPO/bin/reconcile_cancelled_submission.sh" "$T/config.sh" --apply >"$T/applied"
assert awk -F'\t' '$1=="old"&&$9=="CANCELLED"{ok=1}END{exit !ok}' "$WAVE_STATUS_FILE"
assert awk -F'\t' '$1=="s1"&&$4=="PIPELINE_DEFERRED_RETRY"&&$5==""&&$6==""&&$7==4{ok=1}END{exit !ok}' "$STATUS_FILE"
assert awk -F'\t' '$1=="s2"&&$4=="PIPELINE_DEFERRED_RUNNING"&&$7==4{ok=1}END{exit !ok}' "$STATUS_FILE"
assert awk -F'\t' '$1=="s3"&&$4=="PIPELINE_DEFERRED_FAILED"&&$7==4{ok=1}END{exit !ok}' "$STATUS_FILE"
determine_manager_phase; [[ $(manager_phase) == DEFERRED_RETRY ]]

# A second invocation safely rediscovers the terminal wave through the one
# validated row which still retains ownership, but does not requeue that row.
after=$(sha256sum "$STATUS_FILE" "$WAVE_STATUS_FILE")
second=$(bash "$REPO/bin/reconcile_cancelled_submission.sh" "$T/config.sh" --apply)
grep -q '^target_source=orphaned_sample_wave_id$' <<<"$second"
grep -q '^retry=0$' <<<"$second"
[[ "$after" == "$(sha256sum "$STATUS_FILE" "$WAVE_STATUS_FILE")" ]]

# Production regression: the wave was already reconciled, but sample ownership
# was only partially reconciled.  Recovery must be resumable without reopening
# the CANCELLED wave or applying attempt-limit terminalization.
new_env
seq 1 110 | awk '{printf "sample%03d\tsp%03d\n",$1,$1}' >"$T/samples/list.tsv"
source "$REPO/lib/common.sh"; load_config "$T/config.sh"; ensure_state_files
bash "$REPO/bin/initialize_samples.sh" "$T/config.sh"
cat >"$T/mockbin/squeue" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
cat >"$T/mockbin/sacct" <<'EOF'
#!/usr/bin/env bash
cat <<'ROWS'
9211505|CANCELLED by 1000|
9211505_1|CANCELLED by 1000|
9211505_1.batch|FAILED|
9211505_1.extern|UNKNOWN|
9211505_2|CANCELLED|
9211505_3|FAILED|
9211505_3.batch|RUNNING|
9211505_4|COMPLETED|
9211505_5|CANCELLED|
ROWS
EOF
chmod +x "$T/mockbin/squeue" "$T/mockbin/sacct"
printf 'old_cancelled\tmanifest\t100\t9211505\tnow\tCANCELLED\t0\t100\tCANCELLED\tnow\talready reconciled\n' >>"$WAVE_STATUS_FILE"
{
  printf 'submission_id\tpipeline_mode\tphase\tslurm_array_job_id\tarray_task_id\ttask_type\ttask_name\tsample_id\treference_name\tsample_work_root\tbatch_work_root\n'
  seq 1 110 | awk '{printf "old_cancelled\tstreaming_per_sample\tDEFERRED_RETRY\t9211505\t%d\tSAMPLE\tsample%03d\tsample%03d\tsp%03d\t/work/sample%03d\t\n",$1,$1,$1,$1,$1}'
} >"$MANAGER_ROOT/state/submission_task_map/old_cancelled.tsv"
awk -F'\t' -v OFS='\t' 'NR==1{print;next} NR<=101{$4="PIPELINE_DEFERRED_RUNNING";$5=9211505;$6="old_cancelled";$7=7;$8="stale cancellation error"} NR>101{$4="PIPELINE_DEFERRED_FAILED";$5=9211505;$6="old_cancelled";$7=9} {print}' "$STATUS_FILE" >"$STATUS_FILE.x"; mv "$STATUS_FILE.x" "$STATUS_FILE"
wave_before=$(awk -F'\t' '$1=="old_cancelled"' "$WAVE_STATUS_FILE")
failed_before=$(awk -F'\t' '$4=="PIPELINE_DEFERRED_FAILED"' "$STATUS_FILE" | sha256sum)
out=$(bash "$REPO/bin/reconcile_cancelled_submission.sh" "$T/config.sh" --dry-run)
grep -q '^target_source=orphaned_sample_wave_id$' <<<"$out"
grep -q '^wave_status=CANCELLED$' <<<"$out"
grep -q '^slurm_states=CANCELLED,CANCELLED,FAILED,COMPLETED,CANCELLED$' <<<"$out"
grep -q '^mapped=100$' <<<"$out"; grep -q '^retry=100$' <<<"$out"; grep -q '^preserved_failed=10$' <<<"$out"
bash "$REPO/bin/reconcile_cancelled_submission.sh" "$T/config.sh" --apply >/dev/null
assert awk -F'\t' 'NR>1&&NR<=101&&($4!="PIPELINE_DEFERRED_RETRY"||$5!=""||$6!=""||$7!=7||$8!=""){exit 1}' "$STATUS_FILE"
[[ "$failed_before" == "$(awk -F'\t' '$4=="PIPELINE_DEFERRED_FAILED"' "$STATUS_FILE" | sha256sum)" ]]
[[ "$wave_before" == "$(awk -F'\t' '$1=="old_cancelled"' "$WAVE_STATUS_FILE")" ]]

# Multiple orphan owners are ambiguous even when both waves are CANCELLED.
printf 'other_cancelled\tmanifest\t1\t9211506\tnow\tCANCELLED\t0\t1\tCANCELLED\tnow\tother\n' >>"$WAVE_STATUS_FILE"
awk -F'\t' -v OFS='\t' '$1=="sample001"{$4="PIPELINE_DEFERRED_RUNNING";$6="old_cancelled"}$1=="sample002"{$4="PIPELINE_DEFERRED_RUNNING";$6="other_cancelled"}1' "$STATUS_FILE" >"$STATUS_FILE.x"; mv "$STATUS_FILE.x" "$STATUS_FILE"
if bash "$REPO/bin/reconcile_cancelled_submission.sh" "$T/config.sh" --dry-run >"$T/multiple.out" 2>&1; then echo 'multiple orphan waves accepted' >&2; exit 1; fi
grep -q $'old_cancelled\t1' "$T/multiple.out"
grep -q $'other_cancelled\t1' "$T/multiple.out"
echo 'cancelled recovery tests passed'
