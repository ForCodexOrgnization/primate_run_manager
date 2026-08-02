#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
cfg="${1:-${RUN_MANAGER_CONFIG:-}}"; [[ -s "$cfg" ]] || die "Config missing"
# Read only MANAGER_ROOT to locate the global lock, then perform normal loading/validation.
MANAGER_ROOT=$(bash -c 'source "$1"; printf "%s" "$MANAGER_ROOT"' _ "$cfg")
mkdir -p "$MANAGER_ROOT/state/locks"; exec 8>"$MANAGER_ROOT/state/locks/manager_cycle.lock"; flock -n 8 || { log "Another manager cycle is active"; exit 0; }
load_config "$cfg"; ensure_state_files; validate_config
"${SCRIPT_DIR}/update_wave_states.sh" "$cfg"
"${SCRIPT_DIR}/scan_results.sh" "$cfg"
"${SCRIPT_DIR}/check_globus_tasks.sh" "$cfg"
"${SCRIPT_DIR}/cleanup_transferred_samples.sh" "$cfg"
"${SCRIPT_DIR}/submit_globus_batch.sh" "$cfg"
"${SCRIPT_DIR}/submit_next_batch.sh" "$cfg"
"${SCRIPT_DIR}/show_status.sh" "$cfg"
