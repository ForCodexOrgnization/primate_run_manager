#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test_helper.sh"
new_env
mkdir -p "$T/manager/bin" "$T/manager/state/locks"
cp "$REPO/run_manager_daemon.slurm" "$T/manager/"
cat > "$T/manager/bin/manager_cycle.sh" <<'MOCK'
#!/usr/bin/env bash
n=0; [[ ! -s "$TEST_ROOT/cycles" ]] || n=$(cat "$TEST_ROOT/cycles")
n=$((n+1)); printf '%s\n' "$n" > "$TEST_ROOT/cycles"
if (( n == 2 )); then kill -TERM "$PPID"; fi
MOCK
cat > "$T/manager/bin/control_streaming_array_admission.sh" <<'MOCK'
#!/usr/bin/env bash
printf '%s\n' "$WORK_DISK_USED_PERCENT_OVERRIDE" >> "$TEST_ROOT/admission"
MOCK
cat > "$T/mockbin/df" <<'MOCK'
#!/usr/bin/env bash
n=0; [[ ! -s "$TEST_ROOT/probes" ]] || n=$(cat "$TEST_ROOT/probes")
n=$((n+1)); printf '%s\n' "$n" > "$TEST_ROOT/probes"
used=50; (( n >= 2 )) && used=90
printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\nmock 100 %s 10 %s%% /mock\n' "$used" "$used"
MOCK
chmod +x "$T/manager/bin/"*.sh "$T/mockbin/df"
cat > "$T/daemon.config" <<EOF
MANAGER_ROOT=$T/manager
PIPELINE_WORK_ROOT=$T/work
WORK_DISK_CHECK_PATH=$T/work
WORK_CRITICAL_PERCENT=90
MANAGER_POLL_SECONDS=30
DISK_PRESSURE_POLL_SECONDS=1
EOF
start=$(date +%s)
"$T/manager/run_manager_daemon.slurm" "$T/daemon.config" > "$T/daemon.log" 2>&1
elapsed=$(( $(date +%s) - start ))
assert test "$elapsed" -lt 10
assert test "$(cat "$T/probes")" -eq 2
assert test "$(cat "$T/cycles")" -eq 2
assert test "$(cat "$T/admission")" = $'50\n90'
assert grep -qx 90 "$T/admission"
assert grep -q 'Critical work disk pressure detected by lightweight probe' "$T/daemon.log"
echo 'Manager daemon lightweight disk-probe tests passed.'
