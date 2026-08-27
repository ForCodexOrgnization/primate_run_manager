#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test_helper.sh"
new_env
cat > "$T/samples/list.tsv" <<'SAMPLES'
s1	sp
s2	sp
s3	sp
s4	sp
s5	sp
s6	sp
SAMPLES
cat >> "$T/config.sh" <<EOF2
PIPELINE_MODE=streaming_per_sample
SAMPLE_CHAIN_CONCURRENCY=10
WORK_DISK_CHECK_PATH=$T/work
WORK_STOP_SUBMIT_PERCENT=75
WORK_EMERGENCY_CLEAN_PERCENT=82
WORK_CRITICAL_PERCENT=90
WORK_ARRAY_RELEASE_PERCENT=75
FAILED_CACHE_CLEAN_TRIGGER_PERCENT=0
FAILED_CACHE_CLEAN_TARGET_PERCENT=0
EOF2
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null

# An active streaming array: only pending elements are held; running task 1 is untouched.
printf 'stream1\tm\t3\t700\tnow\tRUNNING\t0\t3\tRUNNING\tnow\tactive\t%s\t\tstream1\t\t0\tx\tx\tx\t\tx\tPER_SAMPLE\n' "$T/work" >> "$T/manager/state/wave_status.tsv"
cat > "$T/mockbin/squeue" <<'MOCK'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$TEST_ROOT/squeue.log"
case "$(cat "$TEST_ROOT/queue_mode")" in
 pending)
   if [[ -f "$TEST_ROOT/scontrol.log" && $(grep -c '^hold ' "$TEST_ROOT/scontrol.log") -ge 2 ]]; then
     printf '700|2|PENDING|JobHeldUser\n700|3|PENDING|JobHeldUser\n'
   else printf '700|2|PENDING|Resources\n700|3|PENDING|Priority\n'; fi ;;
 held) printf '700|2|PENDING|JobHeldUser\n700|3|PENDING|JobHeldUser\n' ;;
 empty) : ;;
esac
MOCK
cat > "$T/mockbin/scontrol" <<'MOCK'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$TEST_ROOT/scontrol.log"
MOCK
chmod +x "$T/mockbin/squeue" "$T/mockbin/scontrol"
printf pending > "$T/queue_mode"
WORK_DISK_USED_PERCENT_OVERRIDE=50 "$REPO/bin/control_streaming_array_admission.sh" "$T/config.sh" >/dev/null 2>&1
assert test ! -s "$T/scontrol.log"
# Stop-submit pressure is not array-admission pressure: full concurrency remains.
WORK_DISK_USED_PERCENT_OVERRIDE=80 "$REPO/bin/control_streaming_array_admission.sh" "$T/config.sh" >/dev/null 2>&1
assert test ! -s "$T/scontrol.log"
WORK_DISK_USED_PERCENT_OVERRIDE=90 "$REPO/bin/control_streaming_array_admission.sh" "$T/config.sh" >/dev/null
assert grep -qx 'hold 700_2' "$T/scontrol.log"
assert grep -qx 'hold 700_3' "$T/scontrol.log"
assert grep -q -- "--format=%F|%K|%T|%r" "$T/squeue.log"
if grep -q '700_1' "$T/scontrol.log"; then exit 1; fi
assert test "$(awk 'END{print NR-1}' "$T/manager/state/streaming_array_disk_holds.tsv")" -eq 2
# Repeated critical cycles are idempotent.
printf held > "$T/queue_mode"
WORK_DISK_USED_PERCENT_OVERRIDE=95 "$REPO/bin/control_streaming_array_admission.sh" "$T/config.sh" >/dev/null
assert test "$(grep -c '^hold ' "$T/scontrol.log")" -eq 2
# Manager-owned disk holds persist throughout the configured hysteresis band.
for used in 89 80 76; do
  WORK_DISK_USED_PERCENT_OVERRIDE=$used "$REPO/bin/control_streaming_array_admission.sh" "$T/config.sh" >/dev/null
  assert test "$(grep -c '^release ' "$T/scontrol.log" || true)" -eq 0
  assert test "$(awk -F '\t' '$3=="DISK_PRESSURE"{n++}END{print n+0}' "$T/manager/state/streaming_array_holds.tsv")" -eq 2
done
# A second manager reason may clear without releasing the physical disk hold.
sed -i 's/^SAMPLE_CHAIN_CONCURRENCY=10$/SAMPLE_CHAIN_CONCURRENCY=1/' "$T/config.sh"
WORK_DISK_USED_PERCENT_OVERRIDE=90 "$REPO/bin/control_streaming_array_admission.sh" "$T/config.sh" >/dev/null
assert awk -F '\t' '$1==700&&$2==3&&$3=="DISK_PRESSURE"{d=1}$1==700&&$2==3&&$3=="GLOBAL_CONCURRENCY"{g=1}END{exit !(d&&g)}' "$T/manager/state/streaming_array_holds.tsv"
sed -i 's/^SAMPLE_CHAIN_CONCURRENCY=1$/SAMPLE_CHAIN_CONCURRENCY=10/' "$T/config.sh"
WORK_DISK_USED_PERCENT_OVERRIDE=80 "$REPO/bin/control_streaming_array_admission.sh" "$T/config.sh" >/dev/null
assert awk -F '\t' '$1==700&&$2==3&&$3=="DISK_PRESSURE"{d=1}$3=="GLOBAL_CONCURRENCY"{g=1}END{exit !d || g}' "$T/manager/state/streaming_array_holds.tsv"
assert test "$(grep -c '^release ' "$T/scontrol.log" || true)" -eq 0
WORK_DISK_USED_PERCENT_OVERRIDE=75 "$REPO/bin/control_streaming_array_admission.sh" "$T/config.sh" >/dev/null
assert test "$(grep -c '^release ' "$T/scontrol.log")" -eq 2
# Terminal/cancelled elements disappear from squeue and their records are pruned
# even while usage remains inside the hysteresis band.
printf empty > "$T/queue_mode"
WORK_DISK_USED_PERCENT_OVERRIDE=80 "$REPO/bin/control_streaming_array_admission.sh" "$T/config.sh" >/dev/null
assert test "$(awk 'END{print NR-1}' "$T/manager/state/streaming_array_disk_holds.tsv")" -eq 0
: > "$T/scontrol.log"
printf pending > "$T/queue_mode"
WORK_DISK_USED_PERCENT_OVERRIDE=90 "$REPO/bin/control_streaming_array_admission.sh" "$T/config.sh" >/dev/null
printf held > "$T/queue_mode"
WORK_DISK_USED_PERCENT_OVERRIDE=75 "$REPO/bin/control_streaming_array_admission.sh" "$T/config.sh" >/dev/null
assert test "$(grep -c '^release ' "$T/scontrol.log")" -eq 2
assert test "$(awk 'END{print NR-1}' "$T/manager/state/streaming_array_disk_holds.tsv")" -eq 0
assert grep -q '^SAMPLE_CHAIN_CONCURRENCY=10$' "$T/config.sh"

# WORK_STOP_SUBMIT_PERCENT gates only creation of a new submission.
(
new_env
cat >> "$T/config.sh" <<EOF2
WORK_DISK_CHECK_PATH=$T/work
WORK_STOP_SUBMIT_PERCENT=75
WORK_EMERGENCY_CLEAN_PERCENT=82
WORK_CRITICAL_PERCENT=90
WORK_ARRAY_RELEASE_PERCENT=70
EOF2
printf 'config\n' > "$T/test.config"
cat > "$T/mockbin/df" <<'MOCK'
#!/usr/bin/env bash
printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\nmock 100 80 20 %s%% /mock\n' "$(cat "$TEST_ROOT/work_used")"
MOCK
chmod +x "$T/mockbin/df"
printf 80 > "$T/work_used"
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
stop_log=$("$REPO/bin/submit_next_batch.sh" "$T/config.sh" 2>&1)
assert grep -q 'Work disk stop-submit threshold reached' <<< "$stop_log"
assert test ! -e "$T/invocations"
assert test "$(awk 'END{print NR-1}' "$T/manager/state/wave_status.tsv")" -eq 0
printf 74 > "$T/work_used"
"$REPO/bin/submit_next_batch.sh" "$T/config.sh" >/dev/null
assert test "$(wc -l < "$T/invocations")" -eq 1
)

# Markerless cancellation cleanup safety matrix.
awk -F '\t' -v OFS='\t' 'NR==1{print;next} $1~/^s[1235]$/{$4="PIPELINE_DEFERRED_RETRY"} $1=="s4"{$4="READY_TO_TRANSFER"} $1=="s6"{$4="PIPELINE_DEFERRED_FAILED"}{print}' "$T/manager/state/sample_status.tsv" > "$T/x"; mv "$T/x" "$T/manager/state/sample_status.tsv"
# Validation must win for s3.
printf 's3\t1\t1\t1\t1\t1\t1\t1\tnow\tok\n' >> "$T/manager/state/output_validation.tsv"
for s in s1 s2 s3 s4 s5 s6; do mkdir -p "$T/work/$s"; printf data > "$T/work/$s/data"; done
mkdir -p "$T/work/.sample_state" "$T/work/.locks"
printf 'first_failure_epoch\tworker_state\n1\tFAILED\n' > "$T/work/.sample_state/s6.failure.tsv"
map="$T/manager/state/submission_task_map/test.tsv"
printf 'submission_id\tpipeline_mode\tphase\tslurm_array_job_id\tarray_task_id\ttask_type\ttask_name\tsample_id\treference_name\tsample_work_root\tbatch_work_root\n' > "$map"
i=1; for s in s1 s2 s3 s4 s5 s6; do printf 'x\tstreaming_per_sample\tNORMAL\t800\t%s\tSAMPLE\t%s\t%s\tsp\t%s/%s\t\n' "$i" "$s" "$s" "$T/work" "$s" >> "$map"; i=$((i+1)); done
cat > "$T/mockbin/sacct" <<'MOCK'
#!/usr/bin/env bash
for arg in "$@"; do [[ "$arg" == 800_* ]] && id=${arg#800_}; done
if [[ "$id" == 5 ]]; then
 n=0; [[ ! -f "$TEST_ROOT/s5calls" ]] || n=$(cat "$TEST_ROOT/s5calls"); n=$((n+1)); echo "$n" > "$TEST_ROOT/s5calls"
 [[ "$n" -ge 2 ]] && state=RUNNING || state=CANCELLED
else state=CANCELLED; fi
printf '800_%s|%s\n' "$id" "$state"
MOCK
chmod +x "$T/mockbin/sacct"
# Keep s2's sample lock busy throughout cleanup.
flock "$T/work/.locks/s2.lock" -c 'sleep 5' & locker=$!; sleep 0.1
cleanup_log=$(WORK_DISK_USED_PERCENT_OVERRIDE=99 "$REPO/bin/cleanup_old_failed_sample_workdirs.sh" "$T/config.sh" 2>&1)
kill "$locker" 2>/dev/null || true; wait "$locker" 2>/dev/null || true
assert test ! -d "$T/work/s1"
assert test -d "$T/work/s2"
assert test -d "$T/work/s3"
assert test -d "$T/work/s4"
assert test -d "$T/work/s5"
assert test -d "$T/work/s6"
receipt=$(find "$T/manager/state/receipts/failed_sample_work_cleanup" -name 's1.*.tsv')
assert grep -q $'cleanup_reason\tfirst_failure_epoch' "$receipt"
assert grep -q 'terminal_cancelled_without_failure_marker' "$receipt"
assert test "$(awk -F '\t' '$1=="s1"{print $4}' "$T/manager/state/sample_status.tsv")" = PIPELINE_DEFERRED_RETRY
assert grep -q 'deferred retry must run fresh' "$T/manager/state/sample_status.tsv"
assert grep -q 'eligible candidates exhausted' <<< "$cleanup_log"
# Ordinary FAILED cleanup remains blocked until diagnostics are archived.
mkdir -p "$T/manager/state/failure_diagnostics/samples/s6"; touch "$T/manager/state/failure_diagnostics/samples/s6/ARCHIVE_COMPLETE"
WORK_DISK_USED_PERCENT_OVERRIDE=99 "$REPO/bin/cleanup_old_failed_sample_workdirs.sh" "$T/config.sh" >/dev/null
assert test ! -d "$T/work/s6"

echo 'Disk-pressure regression tests passed.'
