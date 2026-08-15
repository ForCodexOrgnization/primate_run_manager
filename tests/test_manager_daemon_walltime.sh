#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test_helper.sh"; new_env
export USER="${USER:-test-user}"

cat >"$T/mockbin/squeue" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
cat >"$T/mockbin/sbatch" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$TEST_ROOT/sbatch.calls"
echo 4321
EOF
chmod +x "$T/mockbin/squeue" "$T/mockbin/sbatch"
mkdir -p "$T/manager/bin" "$T/manager/state/locks"
cat >"$T/manager/bin/manager_cycle.sh" <<'EOF'
#!/usr/bin/env bash
kill -USR1 "$PPID"
EOF
chmod +x "$T/manager/bin/manager_cycle.sh"
cp "$REPO/run_manager_daemon.slurm" "$T/manager/"
cat >>"$T/config.sh" <<EOF
MANAGER_DAEMON_TIME=12:34:56
WORK_CRITICAL_PERCENT=90
DISK_PRESSURE_POLL_SECONDS=1
EOF

bash "$REPO/bin/submit_manager_daemon.sh" "$T/config.sh" >"$T/submit.out"
grep -q -- '--time=12:34:56' "$T/sbatch.calls"

: >"$T/sbatch.calls"
SLURM_JOB_ID=99 "$T/manager/run_manager_daemon.slurm" "$T/config.sh" >"$T/daemon.out"
grep -q -- '--dependency=afterany:99 --time=12:34:56' "$T/sbatch.calls"
grep -q 'Submitted replacement daemon job 4321' "$T/daemon.out"

! grep -q '5-00:00:00' "$REPO/run_manager_daemon.slurm"
grep -q '^MANAGER_DAEMON_TIME="23:30:00"$' "$REPO/config/mccleary.sh"
grep -q '^MANAGER_DAEMON_TIME=' "$REPO/config/bouchet.sh"
echo 'Manager daemon walltime tests passed.'
