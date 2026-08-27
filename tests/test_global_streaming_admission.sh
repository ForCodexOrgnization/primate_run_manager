#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test_helper.sh"
new_env
cat >> "$T/config.sh" <<EOF2
PIPELINE_MODE=streaming_per_sample
SAMPLE_CHAIN_CONCURRENCY=10
WORK_DISK_CHECK_PATH=$T/work
WORK_CRITICAL_PERCENT=90
WORK_ARRAY_RELEASE_PERCENT=75
EOF2
mkdir -p "$T/work/.sample_state" "$T/manager/state/submission_task_map"
# Old production-compatible array plus a manager-created initially held array.
printf 'old\tm\t2\t700\tnow\tRUNNING\t0\t2\tRUNNING\tnow\told tail\t%s\t\told\t\t0\tx\tx\tx\t\tx\tPER_SAMPLE\n' "$T/work" >> "$T/manager/state/wave_status.tsv"
printf 'new\tm\t1095\t800\tnow\tPENDING\t0\t1095\tSUBMITTED\tnow\tnew held\t%s\t\tnew\t\t0\tx\tx\tx\t\tx\tPER_SAMPLE\n' "$T/work" >> "$T/manager/state/wave_status.tsv"
map="$T/manager/state/submission_task_map/new.tsv"
printf 'submission_id\tpipeline_mode\tphase\tslurm_array_job_id\tarray_task_id\ttask_type\ttask_name\tsample_id\treference_name\tsample_work_root\tbatch_work_root\n' > "$map"
for i in $(seq 1 1095); do printf 'new\tstreaming_per_sample\tNORMAL\t800\t%s\tSAMPLE\ts%s\ts%s\tsp\t%s/s%s\t\n' "$i" "$i" "$i" "$T/work" "$i" >> "$map"; done
printf 'array_job_id\tarray_task_id\thold_reason\theld_at\n' > "$T/manager/state/streaming_array_holds.tsv"
for i in $(seq 1 1095); do printf '800\t%s\tINITIAL_SUBMISSION\tnow\n' "$i" >> "$T/manager/state/streaming_array_holds.tsv"; done
printf 2 > "$T/old_running"; printf 0 > "$T/new_running"; : > "$T/released"; : > "$T/scontrol.log"
cat > "$T/mockbin/squeue" <<'MOCK'
#!/usr/bin/env bash
old=$(cat "$TEST_ROOT/old_running"); new=$(cat "$TEST_ROOT/new_running")
declare -A is_released=()
while read -r task; do [[ -z "$task" ]] || is_released[$task]=1; done < "$TEST_ROOT/released"
for i in $(seq 1 "$old"); do printf '700|%s|RUNNING|None\n' "$i"; done
for i in $(seq 1 1095); do
  if (( i <= new )); then printf '800|%s|RUNNING|None\n' "$i"
  elif [[ -n "${is_released[$i]:-}" ]]; then printf '800|%s|PENDING|Resources\n' "$i"
  else printf '800|%s|PENDING|JobHeldUser\n' "$i"; fi
done
MOCK
cat > "$T/mockbin/scontrol" <<'MOCK'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$TEST_ROOT/scontrol.log"
case "$1" in
 release) task=${2#800_}; grep -qx "$task" "$TEST_ROOT/released" || printf '%s\n' "$task" >> "$TEST_ROOT/released" ;;
 hold) task=${2#800_}; grep -vx "$task" "$TEST_ROOT/released" > "$TEST_ROOT/released.tmp" || true; mv "$TEST_ROOT/released.tmp" "$TEST_ROOT/released" ;;
esac
MOCK
chmod +x "$T/mockbin/squeue" "$T/mockbin/scontrol"
run_admission(){ WORK_DISK_USED_PERCENT_OVERRIDE=50 "$REPO/bin/control_streaming_array_admission.sh" "$T/config.sh" >/dev/null; }
run_admission
assert test "$(wc -l < "$T/released")" -eq 8
assert test "$(sort -n "$T/released" | paste -sd, )" = "1,2,3,4,5,6,7,8"
printf 1 > "$T/old_running"; printf 8 > "$T/new_running"
run_admission
assert grep -qx 9 "$T/released"
assert test "$(wc -l < "$T/released")" -eq 9
# A timeout marker appears while task 9 is runnable but not started. The next
# pass holds that fresh task and releases the exact resume candidate instead.
cat > "$T/work/.sample_state/s10.requeue.tsv" <<'MARKER'
reason	resume_eligible	array_job_id	array_task_id
TIMEOUT_SIGNAL	1	800	10
MARKER
run_admission
assert grep -qx 10 "$T/released"
assert test "$(wc -l < "$T/released")" -eq 9
assert grep -qx 'hold 800_9' "$T/scontrol.log"
# At each transition, RUNNING plus scheduler-eligible pending never exceeds 10.
assert test $(( $(cat "$T/old_running") + $(cat "$T/new_running") + $(wc -l < "$T/released") - $(cat "$T/new_running") )) -le 10
echo 'Global streaming admission regression tests passed.'
