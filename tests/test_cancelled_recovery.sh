#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test_helper.sh"; new_env
source "$REPO/lib/common.sh"; load_config "$T/config.sh"; ensure_state_files
bash "$REPO/bin/initialize_samples.sh" "$T/config.sh"

cat >"$T/mockbin/squeue" <<'EOF'
#!/usr/bin/env bash
[[ -e "$TEST_ROOT/live" ]] && echo RUNNING
exit 0
EOF
cat >"$T/mockbin/sacct" <<'EOF'
#!/usr/bin/env bash
[[ -e "$TEST_ROOT/unknown" ]] && echo '12345|UNKNOWN|' || echo '12345|CANCELLED by 1000|'
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
grep -q 'does not unambiguously prove cancellation' "$T/unknown.out"; [[ "$before" == "$(sha256sum "$STATUS_FILE" "$WAVE_STATUS_FILE")" ]]; rm "$T/unknown"

bash "$REPO/bin/reconcile_cancelled_submission.sh" "$T/config.sh" --apply >"$T/applied"
assert awk -F'\t' '$1=="old"&&$9=="CANCELLED"{ok=1}END{exit !ok}' "$WAVE_STATUS_FILE"
assert awk -F'\t' '$1=="s1"&&$4=="PIPELINE_DEFERRED_RETRY"&&$5==""&&$6==""&&$7==4{ok=1}END{exit !ok}' "$STATUS_FILE"
assert awk -F'\t' '$1=="s2"&&$4=="PIPELINE_DEFERRED_RUNNING"&&$7==4{ok=1}END{exit !ok}' "$STATUS_FILE"
assert awk -F'\t' '$1=="s3"&&$4=="PIPELINE_DEFERRED_FAILED"&&$7==4{ok=1}END{exit !ok}' "$STATUS_FILE"
determine_manager_phase; [[ $(manager_phase) == DEFERRED_RETRY ]]

# A second invocation cannot rediscover the terminal wave and cannot mutate it.
after=$(sha256sum "$STATUS_FILE" "$WAVE_STATUS_FILE")
if bash "$REPO/bin/reconcile_cancelled_submission.sh" "$T/config.sh" --apply >/dev/null 2>&1; then echo 'terminal wave accepted twice' >&2; exit 1; fi
[[ "$after" == "$(sha256sum "$STATUS_FILE" "$WAVE_STATUS_FILE")" ]]
echo 'cancelled recovery tests passed'
