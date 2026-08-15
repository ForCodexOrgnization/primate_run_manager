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
echo 'manager restart tests passed'
