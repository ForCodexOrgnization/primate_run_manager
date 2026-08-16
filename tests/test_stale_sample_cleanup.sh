#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test_helper.sh"
new_env
cat >> "$T/config.sh" <<EOF
PIPELINE_MODE=streaming_per_sample
FAILED_CACHE_CLEAN_TRIGGER_PERCENT=1
FAILED_CACHE_CLEAN_TARGET_PERCENT=1
WORK_DISK_CHECK_PATH=$T/work
EOF
mkdir -p "$T/work/S1" "$T/work/S2" "$T/work/S3" \
    "$T/work/S1.stale.20260815T010203Z.12" \
    "$T/work/S1.stale.bad" "$T/work/S1.stale.20260816.1234" "$T/work/.locks"
echo canonical > "$T/work/S1/data"; echo stale > "$T/work/S1.stale.20260815T010203Z.12/data"
cat > "$T/mockbin/du" <<'MOCK'
#!/usr/bin/env bash
printf '%s\n' "${@: -1}" >> "$TEST_ROOT/du_paths"
exec /usr/bin/du "$@"
MOCK
chmod +x "$T/mockbin/du"
dry=$(WORK_DISK_USED_PERCENT_OVERRIDE=99 "$REPO/bin/cleanup_stale_sample_workdirs.sh" "$T/config.sh" --dry-run 2>&1)
assert test -d "$T/work/S1.stale.20260815T010203Z.12"
assert grep -q 'decision=ELIGIBLE reason=detached_stale_generation' <<< "$dry"
assert test "$(wc -l < "$T/du_paths")" -eq 1
assert grep -Fxq "$T/work/S1.stale.20260815T010203Z.12" "$T/du_paths"
assert test -z "$(grep 'stale.bad\|stale.20260816' <<< "$dry" || true)"
# A live sample lock protects its detached generation.
flock "$T/work/.locks/S1.lock" -c 'sleep 3' & locker=$!; sleep .1
locked=$(WORK_DISK_USED_PERCENT_OVERRIDE=99 "$REPO/bin/cleanup_stale_sample_workdirs.sh" "$T/config.sh" --apply 2>&1)
wait "$locker"
assert test -d "$T/work/S1.stale.20260815T010203Z.12"
assert grep -q 'reason=active_sample_lock' <<< "$locked"
# A stale generation explicitly owned by a live array task is protected. A
# terminal historical task-map reference, however, does not retain it forever.
map="$T/manager/state/submission_task_map/old.tsv"
printf 'submission_id\tpipeline_mode\tphase\tslurm_array_job_id\tarray_task_id\ttask_type\ttask_name\tsample_id\treference_name\tsample_work_root\tbatch_work_root\n' > "$map"
printf 'old\tstreaming_per_sample\tNORMAL\t800\t1\tSAMPLE\tS1\tS1\tsp\t%s\t\n' "$T/work/S1.stale.20260815T010203Z.12" >> "$map"
cat > "$T/mockbin/sacct" <<'MOCK'
#!/usr/bin/env bash
printf '800_1|%s\n' "$(cat "$TEST_ROOT/task_state")"
MOCK
chmod +x "$T/mockbin/sacct"; printf RUNNING > "$T/task_state"
active=$(WORK_DISK_USED_PERCENT_OVERRIDE=99 "$REPO/bin/cleanup_stale_sample_workdirs.sh" "$T/config.sh" --apply 2>&1)
assert test -d "$T/work/S1.stale.20260815T010203Z.12"
assert grep -q 'reason=active_slurm_ownership' <<< "$active"
printf FAILED > "$T/task_state"
WORK_DISK_USED_PERCENT_OVERRIDE=99 "$REPO/bin/cleanup_stale_sample_workdirs.sh" "$T/config.sh" --apply >/dev/null
assert test -d "$T/work/S1"
assert test ! -d "$T/work/S1.stale.20260815T010203Z.12"
assert test -d "$T/work/S1.stale.bad"
assert test -d "$T/work/S1.stale.20260816.1234"
receipt=$(find "$T/manager/state/receipts/stale_sample_work_cleanup" -type f | head -1)
assert grep -q $'sample_id\tstale_path\tbytes_released\tcleanup_time\teligibility_reason\twork_filesystem_usage_before_cleanup' "$receipt"
assert grep -q 'detached_stale_generation' "$receipt"
echo 'Stale sample cleanup regression tests passed.'
