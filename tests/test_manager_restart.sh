#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test_helper.sh"; new_env
# Create valid, deliberately non-migrated manager metadata.
source "$REPO/lib/common.sh"; load_config "$T/config.sh"; ensure_state_files
awk -F'\t' -v OFS='\t' 'NR==1{print;next}{$4=(NR==2?"PIPELINE_DEFERRED_RETRY":"PENDING");print}' "$STATUS_FILE" >"$STATUS_FILE.tmp" 2>/dev/null || true
# initialize_samples provides exactly one manager row for every assignment.
bash "$REPO/bin/initialize_samples.sh" "$T/config.sh"
cat >"$T/mockbin/squeue" <<'EOF'
#!/usr/bin/env bash
[[ -s "$TEST_ROOT/daemon.queue" ]] && cat "$TEST_ROOT/daemon.queue"
exit 0
EOF
cat >"$T/mockbin/scancel" <<'EOF'
#!/usr/bin/env bash
echo "scancel $*" >>"$TEST_ROOT/slurm.calls"
EOF
chmod +x "$T/mockbin/squeue" "$T/mockbin/scancel"

out=$(bash "$REPO/bin/manager_restart_preflight.sh" "$T/config.sh")
grep -q 'NOT RUNNING' <<<"$out"; grep -q 'Restart readiness: WARNING' <<<"$out"

printf '100|RUNNING|primate_manager_daemon\n101|PENDING|primate_manager_daemon\n' >"$T/daemon.queue"
if bash "$REPO/bin/manager_restart_preflight.sh" "$T/config.sh" >"$T/multiple" 2>&1; then echo 'multiple daemons accepted' >&2; exit 1; fi
grep -q 'multiple daemons' "$T/multiple"

printf '100|RUNNING|primate_manager_daemon\n' >"$T/daemon.queue"
before=$(sha256sum "$STATUS_FILE" "$WAVE_STATUS_FILE")
bash "$REPO/bin/restart_manager.sh" "$T/config.sh" --dry-run >"$T/dry"
after=$(sha256sum "$STATUS_FILE" "$WAVE_STATUS_FILE"); [[ "$before" == "$after" ]]
grep -q 'no Slurm or manager state changes' "$T/dry"; [[ ! -e "$T/slurm.calls" ]]

# A non-empty hold table must be accepted by awk variants that require the
# ternary expression passed to print to be parenthesized.
cat >"$MANAGER_ROOT/state/streaming_array_disk_holds.tsv" <<'EOF'
job_id	array_task_id	held_at	reason
123	4	now	disk pressure
EOF
PIPELINE_MODE=streaming_per_sample bash "$REPO/bin/show_status.sh" "$T/config.sh" >"$T/status.out"
grep -q 'Array elements held by manager: 1' "$T/status.out"

# Exercise restart orchestration with deterministic daemon/helper doubles.  The
# production restart body is retained; only its external helper programs are
# replaced in this private copy.
cp -a "$REPO" "$T/restart-repo"
for helper in manager_restart_preflight reconcile_assigned_sample_scope; do
 cat >"$T/restart-repo/bin/${helper}.sh" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
done
cat >"$T/restart-repo/bin/stop_manager_daemon.sh" <<'EOF'
#!/usr/bin/env bash
: >"$TEST_ROOT/daemon.queue"
EOF
cat >"$T/restart-repo/bin/manager_cycle.sh" <<'EOF'
#!/usr/bin/env bash
echo cycle >>"$TEST_ROOT/restart.calls"
[[ ! -e "$TEST_ROOT/fail-cycle" ]]
EOF
cat >"$T/restart-repo/bin/submit_manager_daemon.sh" <<'EOF'
#!/usr/bin/env bash
echo submit >>"$TEST_ROOT/restart.calls"
if [[ -e "$TEST_ROOT/fail-submit" ]]; then
 echo 'sbatch: error: QOSMaxWallDurationPerJobLimit' >&2
 exit 1
fi
printf '200|PENDING|primate_manager_daemon\n' >"$TEST_ROOT/daemon.queue"
echo 'Submitted manager daemon Slurm job 200'
EOF
cat >"$T/restart-repo/bin/show_status.sh" <<'EOF'
#!/usr/bin/env bash
echo status >>"$TEST_ROOT/restart.calls"
exit 42
EOF
chmod +x "$T/restart-repo/bin/"*.sh
printf '100|RUNNING|primate_manager_daemon\n' >"$T/daemon.queue"
MANAGER_RESTART_VERIFY_DELAY_SECONDS=0 bash "$T/restart-repo/bin/restart_manager.sh" "$T/config.sh" >"$T/restart.out" 2>"$T/restart.err"
[[ $(grep -c '^cycle$' "$T/restart.calls") == 1 ]]
[[ $(grep -c '^submit$' "$T/restart.calls") == 1 ]]
grep -q 'Manager restart successful' "$T/restart.out"
grep -q 'WARNING: manager daemon 200 restarted successfully, but final status display failed' "$T/restart.err"
grep -qx '200|PENDING|primate_manager_daemon' "$T/daemon.queue"

# The real scope reconciler must accept the cycle lock inherited from restart;
# attempting to reopen/reacquire this lock used to make recovery fail here.
exec 8>"$MANAGER_ROOT/state/locks/manager_cycle.lock"; flock -n 8
MANAGER_CYCLE_LOCK_HELD=1 bash "$REPO/bin/reconcile_assigned_sample_scope.sh" "$T/config.sh" --dry-run >"$T/inherited-scope.out"
flock -u 8
grep -q 'Scope reconciliation: retired=0 blocked=0 dry_run=1' "$T/inherited-scope.out"

# A restart may be resumed after reconciliation has already detached every old
# owner.  With only deferred retries left, nothing_to_recover is a successful
# plan result: orchestration must retain the inherited cycle lock across scope
# reconciliation and the recovery cycle, submit through submit_next_batch, and
# install exactly one daemon.
rm -rf "$T/resume-repo"; cp -a "$REPO" "$T/resume-repo"
cat >"$T/resume-repo/bin/manager_restart_preflight.sh" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
cat >"$T/resume-repo/bin/reconcile_assigned_sample_scope.sh" <<'EOF'
#!/usr/bin/env bash
[[ "${MANAGER_CYCLE_LOCK_HELD:-0}" == 1 ]] || { echo 'scope lock was not inherited' >&2; exit 1; }
echo scope >>"$TEST_ROOT/resume.calls"
EOF
cat >"$T/resume-repo/bin/manager_cycle.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ "${MANAGER_CYCLE_LOCK_HELD:-0}" == 1 && "${MANAGER_RECOVERY_MODE:-0}" == 1 ]] || exit 1
source "$(dirname "$0")/../lib/common.sh"; load_config "$1"; determine_manager_phase
[[ $(manager_phase) == DEFERRED_RETRY ]] || { echo "wrong phase: $(manager_phase)" >&2; exit 1; }
echo cycle >>"$TEST_ROOT/resume.calls"
bash "$(dirname "$0")/submit_next_batch.sh" "$1"
EOF
cat >"$T/resume-repo/bin/stop_manager_daemon.sh" <<'EOF'
#!/usr/bin/env bash
: >"$TEST_ROOT/daemon.queue"
EOF
cat >"$T/resume-repo/bin/submit_manager_daemon.sh" <<'EOF'
#!/usr/bin/env bash
echo daemon >>"$TEST_ROOT/resume.calls"
printf '300|PENDING|primate_manager_daemon\n' >"$TEST_ROOT/daemon.queue"
echo 'Submitted manager daemon Slurm job 300'
EOF
cat >"$T/resume-repo/bin/show_status.sh" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod +x "$T/resume-repo/bin/"*.sh
awk -F'\t' -v OFS='\t' 'NR==1{print;next}{$4="PIPELINE_DEFERRED_RETRY";$5="";$6="";$7=4;print}' "$STATUS_FILE" >"$STATUS_FILE.resume"; mv "$STATUS_FILE.resume" "$STATUS_FILE"
awk -F'\t' 'NR==1{print}' "$WAVE_STATUS_FILE" >"$WAVE_STATUS_FILE.resume"; mv "$WAVE_STATUS_FILE.resume" "$WAVE_STATUS_FILE"
: >"$T/resume.calls"; rm -f "$T/invocations"; printf '100|RUNNING|primate_manager_daemon\n' >"$T/daemon.queue"
MANAGER_RESTART_VERIFY_DELAY_SECONDS=0 bash "$T/resume-repo/bin/restart_manager.sh" "$T/config.sh" --recover-cancelled >"$T/resume.out"
grep -q '^target_source=none$' "$T/resume.out"; grep -q '^nothing_to_recover=1$' "$T/resume.out"
[[ $(grep -c '^scope$' "$T/resume.calls") == 1 ]]
[[ $(grep -c '^cycle$' "$T/resume.calls") == 1 ]]
[[ $(grep -c '^daemon$' "$T/resume.calls") == 1 ]]
[[ $(wc -l <"$T/invocations") == 1 ]]
assert awk -F'\t' 'NR>1&&$9=="SUBMITTED"&&$11~/phase=DEFERRED_RETRY/{ok=1}END{exit !ok}' "$WAVE_STATUS_FILE"
grep -qx '300|PENDING|primate_manager_daemon' "$T/daemon.queue"
[[ -d "$T/work" ]]

# Genuine cycle failure remains fatal and occurs before daemon submission.
: >"$T/restart.calls"; touch "$T/fail-cycle"
printf '100|RUNNING|primate_manager_daemon\n' >"$T/daemon.queue"
if MANAGER_RESTART_VERIFY_DELAY_SECONDS=0 bash "$T/restart-repo/bin/restart_manager.sh" "$T/config.sh" >"$T/cycle-fail.out" 2>&1; then
 echo 'restart accepted manager cycle failure' >&2; exit 1
fi
grep -qx cycle "$T/restart.calls"
! grep -q '^submit$' "$T/restart.calls"
rm -f "$T/fail-cycle"

# Submission failure after successful reconciliation is explicit and does not
# repeat the cycle or alter reconciled manager state/pipeline metadata.
: >"$T/restart.calls"; touch "$T/fail-submit"
printf '100|RUNNING|primate_manager_daemon\n' >"$T/daemon.queue"
before=$(sha256sum "$STATUS_FILE" "$WAVE_STATUS_FILE")
if MANAGER_RESTART_VERIFY_DELAY_SECONDS=0 bash "$T/restart-repo/bin/restart_manager.sh" "$T/config.sh" >"$T/submit-fail.out" 2>&1; then
 echo 'restart accepted daemon submission failure' >&2; exit 1
fi
after=$(sha256sum "$STATUS_FILE" "$WAVE_STATUS_FILE"); [[ "$before" == "$after" ]]
[[ $(grep -c '^cycle$' "$T/restart.calls") == 1 ]]
[[ $(grep -c '^submit$' "$T/restart.calls") == 1 ]]
grep -q 'manager cycle/reconciliation completed, but new daemon submission failed' "$T/submit-fail.out"
grep -q 'Manager state is preserved' "$T/submit-fail.out"
grep -q 'No pipeline array was modified' "$T/submit-fail.out"
grep -q 'rerun submit_manager_daemon.sh' "$T/submit-fail.out"
rm -f "$T/fail-submit"

# An active streaming/batch submission without its audit map is always blocked.
awk -F'\t' -v OFS='\t' 'NR==1{print;next}' "$WAVE_STATUS_FILE" >"$WAVE_STATUS_FILE.tmp"
printf 'sub1\tmanifest\t3\t777\tnow\tRUNNING\t0\t3\tRUNNING\tnow\ttest\t%s\t\tsub1\t\t0\tx\tx\tx\t\tx\n' "$T/work" >>"$WAVE_STATUS_FILE.tmp"; mv "$WAVE_STATUS_FILE.tmp" "$WAVE_STATUS_FILE"
if bash "$REPO/bin/manager_restart_preflight.sh" "$T/config.sh" >"$T/map" 2>&1; then echo 'missing task map accepted' >&2; exit 1; fi
grep -q 'missing task map' "$T/map"

# Audit the central safety promises: concurrency remains configuration-owned and
# restart has no pipeline-array control operation.
grep -q '^SAMPLE_CHAIN_CONCURRENCY=10' <(bash -c 'source "$1"; echo SAMPLE_CHAIN_CONCURRENCY=${SAMPLE_CHAIN_CONCURRENCY:-10}' _ "$T/config.sh")
! grep -Eq 'scancel.*array|scontrol (hold|release|requeue)' "$REPO/bin/restart_manager.sh"
# The mutating restart path reconciles only after the daemon/manager-cycle locks
# are clear, while the earlier dry-run exit cannot reach that invocation.
grep -q 'reconcile_assigned_sample_scope.sh' "$REPO/bin/restart_manager.sh"
awk '/stop_manager_daemon.sh/{st=NR}/reconcile_assigned_sample_scope.sh/{rec=NR}END{exit !(st<rec)}' "$REPO/bin/restart_manager.sh"
# manager_cycle remains the reconciliation authority for both restart modes.
[[ $(grep -Ec 'MANAGER_CYCLE_LOCK_HELD=1 MANAGER_RECOVERY_MODE=1 .*manager_cycle\.sh|flock -u 8; .*manager_cycle\.sh' "$REPO/bin/restart_manager.sh") == 1 ]]
for helper in update_wave_states scan_active_results ingest_sample_markers ingest_batch_tasks archive_sample_failure_diagnostics; do
 ! grep -q '"\$SCRIPT_DIR/'"$helper"'\.sh"' "$REPO/bin/restart_manager.sh"
done
# Informational reporting is ordered after successful submit and verification.
awk '/submit_manager_daemon.sh/{submit=NR}/new daemon verification count failed/{verify=NR}/if ! "\$SCRIPT_DIR\/show_status.sh"/{status=NR}END{exit !(submit<verify&&verify<status)}' "$REPO/bin/restart_manager.sh"
# Keep portable awk ternaries repository-wide (excluding Git internals).
! grep -RIE --exclude-dir=.git --include='*.sh' 'print[[:space:]]+[^;(]*[[:alnum:]_][[:space:]]*[><=!]=?[[:space:]]*[^?;]+\?[^:;]+:[^);}]+' "$REPO"
echo 'manager restart tests passed'
