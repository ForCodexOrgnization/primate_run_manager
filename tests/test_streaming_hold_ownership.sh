#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test_helper.sh"
new_env
cat >> "$T/config.sh" <<EOF2
PIPELINE_MODE=streaming_per_sample
SAMPLE_CHAIN_CONCURRENCY=0
WORK_DISK_CHECK_PATH=$T/work
WORK_CRITICAL_PERCENT=90
WORK_ARRAY_RELEASE_PERCENT=75
EOF2
mkdir -p "$T/work/.sample_state" "$T/manager/state/submission_task_map"
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
printf 'owners\tm\t4\t900\tnow\tPENDING\t0\t4\tSUBMITTED\tnow\townership\t%s\t\towners\t\t0\tx\tx\tx\t\tx\tPER_SAMPLE\n' "$T/work" >> "$T/manager/state/wave_status.tsv"
cat > "$T/manager/state/streaming_array_holds.tsv" <<'LEDGER'
array_job_id	array_task_id	hold_reason	held_at
900	2	INITIAL_SUBMISSION	stale
900	3	INITIAL_SUBMISSION	now
LEDGER
: > "$T/scontrol.log"
cat > "$T/mockbin/squeue" <<'MOCK'
#!/usr/bin/env bash
printf '900|1|PENDING|JobHeldUser\n'  # unrelated manual user hold
printf '900|2|PENDING|JobHeldAdmin\n' # administrator hold
printf '900|3|PENDING|JobHeldUser\n'  # manager INITIAL_SUBMISSION hold
if [[ -e "$TEST_ROOT/task4_held" ]]; then
  printf '900|4|PENDING|JobHeldUser\n'
else
  printf '900|4|PENDING|Resources\n'
fi
MOCK
cat > "$T/mockbin/scontrol" <<'MOCK'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$TEST_ROOT/scontrol.log"
[[ "$1 $2" != 'hold 900_4' ]] || touch "$TEST_ROOT/task4_held"
MOCK
chmod +x "$T/mockbin/squeue" "$T/mockbin/scontrol"

WORK_DISK_USED_PERCENT_OVERRIDE=50 "$REPO/bin/control_streaming_array_admission.sh" "$T/config.sh" >/dev/null
assert grep -qx 'hold 900_4' "$T/scontrol.log"
assert awk -F '\t' 'NR>1&&($2==1||$2==2){bad=1}$2==3{initial=1}$2==4{created=1}END{exit bad || !initial || !created}' "$T/manager/state/streaming_array_holds.tsv"

# Once policy clears, only holds with proven manager ownership are released.
sed -i 's/^SAMPLE_CHAIN_CONCURRENCY=0$/SAMPLE_CHAIN_CONCURRENCY=10/' "$T/config.sh"
WORK_DISK_USED_PERCENT_OVERRIDE=50 "$REPO/bin/control_streaming_array_admission.sh" "$T/config.sh" >/dev/null
assert grep -qx 'release 900_3' "$T/scontrol.log"
assert grep -qx 'release 900_4' "$T/scontrol.log"
assert test "$(grep -c '^release ' "$T/scontrol.log")" -eq 2
assert test "$(awk 'END{print NR-1}' "$T/manager/state/streaming_array_holds.tsv")" -eq 0

# Reconciliation is idempotent and never releases either external hold.
WORK_DISK_USED_PERCENT_OVERRIDE=50 "$REPO/bin/control_streaming_array_admission.sh" "$T/config.sh" >/dev/null
assert test "$(grep -c '^release ' "$T/scontrol.log")" -eq 2
if grep -Eq '^(hold|release) 900_[12]$' "$T/scontrol.log"; then exit 1; fi

# A transient release failure preserves every ownership reason and is retried.
printf 'owners\tm\t1\t800\tnow\tPENDING\t0\t1\tSUBMITTED\tnow\townership\t%s\t\towners\t\t0\tx\tx\tx\t\tx\tPER_SAMPLE\n' "$T/work" >> "$T/manager/state/wave_status.tsv"
cat > "$T/manager/state/streaming_array_holds.tsv" <<'LEDGER'
array_job_id	array_task_id	hold_reason	held_at
800	10	INITIAL_SUBMISSION	initial
800	10	GLOBAL_CONCURRENCY	concurrency
LEDGER
cat > "$T/mockbin/squeue" <<'MOCK'
#!/usr/bin/env bash
printf '800|1|PENDING|JobHeldUser\n'  # unrelated manual user hold
printf '800|2|PENDING|JobHeldAdmin\n' # administrator hold
[[ -e "$TEST_ROOT/task10_released" ]] || printf '800|10|PENDING|JobHeldUser\n'
MOCK
cat > "$T/mockbin/scontrol" <<'MOCK'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$TEST_ROOT/scontrol.log"
if [[ "$1 $2" == 'release 800_10' ]]; then
  attempts=$(grep -c '^release 800_10$' "$TEST_ROOT/scontrol.log")
  (( attempts > 1 )) || exit 1
  touch "$TEST_ROOT/task10_released"
fi
MOCK
chmod +x "$T/mockbin/squeue" "$T/mockbin/scontrol"

WORK_DISK_USED_PERCENT_OVERRIDE=50 "$REPO/bin/control_streaming_array_admission.sh" "$T/config.sh" >/dev/null 2>"$T/release.err"
assert grep -q 'release failure for manager-owned array element job=800 task=10' "$T/release.err"
assert test "$(grep -c $'^800\t10\t' "$T/manager/state/streaming_array_holds.tsv")" -eq 2
assert grep -q $'^800\t10\tINITIAL_SUBMISSION\tinitial$' "$T/manager/state/streaming_array_holds.tsv"
assert grep -q $'^800\t10\tGLOBAL_CONCURRENCY\tconcurrency$' "$T/manager/state/streaming_array_holds.tsv"

WORK_DISK_USED_PERCENT_OVERRIDE=50 "$REPO/bin/control_streaming_array_admission.sh" "$T/config.sh" >/dev/null
assert test "$(grep -c '^release 800_10$' "$T/scontrol.log")" -eq 2
assert test "$(awk 'END{print NR-1}' "$T/manager/state/streaming_array_holds.tsv")" -eq 0

# A successful release is not repeated, and external/admin holds remain untouched.
WORK_DISK_USED_PERCENT_OVERRIDE=50 "$REPO/bin/control_streaming_array_admission.sh" "$T/config.sh" >/dev/null
assert test "$(grep -c '^release 800_10$' "$T/scontrol.log")" -eq 2
if grep -Eq '^(hold|release) 800_[12]$' "$T/scontrol.log"; then exit 1; fi
echo 'Streaming hold ownership regression tests passed.'
