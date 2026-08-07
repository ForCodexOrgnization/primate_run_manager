#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
cfg="${1:-${RUN_MANAGER_CONFIG:-}}"; [[ -s "$cfg" ]] || die "Config missing"
# Read only MANAGER_ROOT to locate the global lock, then perform normal loading/validation.
MANAGER_ROOT=$(bash -c 'source "$1"; printf "%s" "$MANAGER_ROOT"' _ "$cfg")
mkdir -p "$MANAGER_ROOT/state/locks"; exec 8>"$MANAGER_ROOT/state/locks/manager_cycle.lock"; flock -n 8 || { log "Another manager cycle is active"; exit 0; }
load_config "$cfg"; ensure_state_files; validate_config
# In streaming mode this updates submission audit metadata only; it never gates
# per-sample reconciliation or transfer readiness.
"${SCRIPT_DIR}/update_wave_states.sh" "$cfg"
if [[ "$ENABLE_FULL_SCAN_IN_MANAGER_CYCLE" == 1 ]]; then
    "${SCRIPT_DIR}/scan_results.sh" "$cfg"
elif [[ "$ENABLE_INCREMENTAL_SCAN_IN_MANAGER_CYCLE" == 1 ]]; then
    "${SCRIPT_DIR}/scan_active_results.sh" "$cfg"
fi
# Exact array elements are reconciled before transfer and new submission work.
"${SCRIPT_DIR}/ingest_sample_markers.sh" "$cfg"
"${SCRIPT_DIR}/archive_wave_failure_diagnostics.sh" "$cfg"
"${SCRIPT_DIR}/archive_sample_failure_diagnostics.sh" "$cfg"
if [[ "$PIPELINE_MODE" != streaming_per_sample ]]; then
    "${SCRIPT_DIR}/cleanup_terminal_deferred_wave_workdirs.sh" "$cfg"
fi
work_used=$(work_disk_used_percent)
if [[ "$PIPELINE_MODE" == streaming_per_sample ]] && (( work_used >= FAILED_CACHE_CLEAN_TRIGGER_PERCENT )); then
    "${SCRIPT_DIR}/cleanup_old_failed_sample_workdirs.sh" "$cfg"
elif (( work_used >= WORK_EMERGENCY_CLEAN_PERCENT )); then
    "${SCRIPT_DIR}/cleanup_orphan_wave_workdirs.sh" "$cfg" --apply
else
    "${SCRIPT_DIR}/cleanup_orphan_wave_workdirs.sh" "$cfg" --dry-run >/dev/null
fi
# Cleanup can release enough space to resume; never submit using a stale df value.
work_used=$(work_disk_used_percent)
determine_manager_phase
if (( work_used >= WORK_CRITICAL_PERCENT )); then log "CRITICAL: work filesystem is ${work_used}% used; pipeline submissions forbidden"; fi
"${SCRIPT_DIR}/check_globus_tasks.sh" "$cfg"
"${SCRIPT_DIR}/cleanup_transferred_samples.sh" "$cfg"
"${SCRIPT_DIR}/submit_globus_batch.sh" "$cfg"
[[ "$(manager_phase)" != PAUSED_DISK_PRESSURE && "$work_used" -lt "$WORK_CRITICAL_PERCENT" ]] && "${SCRIPT_DIR}/submit_next_batch.sh" "$cfg"
"${SCRIPT_DIR}/show_status.sh" "$cfg"
