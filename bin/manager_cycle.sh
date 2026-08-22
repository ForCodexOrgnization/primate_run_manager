#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
cfg="${1:-${RUN_MANAGER_CONFIG:-}}"; [[ -s "$cfg" ]] || die "Config missing"
# Read only MANAGER_ROOT to locate the global lock, then perform normal loading/validation.
MANAGER_ROOT=$(bash -c 'source "$1"; printf "%s" "$MANAGER_ROOT"' _ "$cfg")
mkdir -p "$MANAGER_ROOT/state/locks"
if [[ "${MANAGER_CYCLE_LOCK_HELD:-0}" != 1 ]]; then
  exec 8>"$MANAGER_ROOT/state/locks/manager_cycle.lock"; flock -n 8 || { log "Another manager cycle is active"; exit 0; }
fi
load_config "$cfg"; ensure_state_files; validate_config
cycle_started=$(now_iso); cycle_sha=$(git_commit_or_unknown)
record_cycle_status() {
  local rc=$? status=SUCCESS tmp="${MANAGER_CYCLE_STATUS_FILE}.tmp.$$"
  (( rc == 0 )) || status=FAILED
  printf 'status\texit_code\tstarted_at\tfinished_at\tcode_sha\n%s\t%s\t%s\t%s\t%s\n' "$status" "$rc" "$cycle_started" "$(now_iso)" "$cycle_sha" > "$tmp"
  mv "$tmp" "$MANAGER_CYCLE_STATUS_FILE"
}
trap record_cycle_status EXIT
run_transfer_step() {
  local helper="$1" rc=0
  "$helper" "$cfg" || rc=$?
  if (( rc != 0 )); then
    [[ -s "$GLOBUS_HEALTH_FILE" ]] || record_globus_health UNKNOWN "${helper##*/}" "$rc" "transfer helper failed"
    log "WARNING: transfer helper ${helper##*/} failed (rc=$rc); continuing manager cycle"
  fi
  return 0
}
# In streaming mode this updates submission audit metadata only; it never gates
# per-sample reconciliation or transfer readiness.
"${SCRIPT_DIR}/update_wave_states.sh" "$cfg"
# Reconcile exact task state first, so submitted/PENDING elements cannot enter
# the incremental validator. Completion-marker validation remains targeted.
INGEST_SKIP_VALIDATION=1 "${SCRIPT_DIR}/ingest_sample_markers.sh" "$cfg"
# Cheap transfer progress/submission runs before validation so already-READY
# samples are not delayed behind a slow output scan.
run_transfer_step "${SCRIPT_DIR}/check_globus_tasks.sh"
run_transfer_step "${SCRIPT_DIR}/cleanup_transferred_samples.sh"
run_transfer_step "${SCRIPT_DIR}/submit_globus_batch.sh"
if [[ "$ENABLE_FULL_SCAN_IN_MANAGER_CYCLE" == 1 ]]; then
    "${SCRIPT_DIR}/scan_results.sh" "$cfg"
elif [[ "$ENABLE_INCREMENTAL_SCAN_IN_MANAGER_CYCLE" == 1 ]]; then
    "${SCRIPT_DIR}/scan_active_results.sh" "$cfg"
fi
"${SCRIPT_DIR}/ingest_batch_tasks.sh" "$cfg"
"${SCRIPT_DIR}/archive_wave_failure_diagnostics.sh" "$cfg"
"${SCRIPT_DIR}/archive_sample_failure_diagnostics.sh" "$cfg"
if [[ "$PIPELINE_MODE" != streaming_per_sample ]]; then
    "${SCRIPT_DIR}/cleanup_terminal_deferred_wave_workdirs.sh" "$cfg"
fi
work_used=$(work_disk_used_percent)
# Close array admission before any potentially slow size/deletion work. This is
# intentionally ahead of GC so a 100%-full filesystem cannot start more tasks.
WORK_DISK_USED_PERCENT_OVERRIDE="$work_used" "${SCRIPT_DIR}/control_streaming_array_admission.sh" "$cfg"
# Detached quarantine generations are not reusable caches. Reclaim them first,
# including during recovery; recovery continues to preserve canonical caches.
if [[ "$PIPELINE_MODE" == streaming_per_sample ]] && (( work_used >= FAILED_CACHE_CLEAN_TRIGGER_PERCENT )); then
    "${SCRIPT_DIR}/cleanup_stale_sample_workdirs.sh" "$cfg" --apply
    work_used=$(work_disk_used_percent)
fi
if [[ "${MANAGER_RECOVERY_MODE:-0}" == 1 ]]; then
    log "Recovery cycle: preserving canonical pipeline work/cache directories"
elif [[ "$PIPELINE_MODE" == streaming_per_sample ]] && (( work_used >= FAILED_CACHE_CLEAN_TRIGGER_PERCENT )); then
    "${SCRIPT_DIR}/cleanup_old_failed_sample_workdirs.sh" "$cfg"
elif (( work_used >= WORK_EMERGENCY_CLEAN_PERCENT )); then
    "${SCRIPT_DIR}/cleanup_orphan_wave_workdirs.sh" "$cfg" --apply
else
    "${SCRIPT_DIR}/cleanup_orphan_wave_workdirs.sh" "$cfg" --dry-run >/dev/null
fi
# Cleanup can release enough space to resume; never submit using a stale df value.
work_used=$(work_disk_used_percent)
# A second pass verifies holds, or releases only manager-owned holds if GC moved
# usage through the configured hysteresis release threshold.
WORK_DISK_USED_PERCENT_OVERRIDE="$work_used" "${SCRIPT_DIR}/control_streaming_array_admission.sh" "$cfg"
determine_manager_phase
if (( work_used >= WORK_CRITICAL_PERCENT )); then log "CRITICAL: work filesystem is ${work_used}% used; pipeline submissions forbidden"; fi
# A second best-effort pass picks up READY samples produced by validation.
run_transfer_step "${SCRIPT_DIR}/check_globus_tasks.sh"
run_transfer_step "${SCRIPT_DIR}/cleanup_transferred_samples.sh"
run_transfer_step "${SCRIPT_DIR}/submit_globus_batch.sh"
[[ "$(manager_phase)" != PAUSED_DISK_PRESSURE && "$work_used" -lt "$WORK_CRITICAL_PERCENT" ]] && "${SCRIPT_DIR}/submit_next_batch.sh" "$cfg"
# Status rendering is informational.  Reconciliation and submission failures above
# remain fatal, but a display-only problem must not make restart leave no daemon.
"${SCRIPT_DIR}/show_status.sh" "$cfg" || log "WARNING: manager cycle completed, but status display failed"
