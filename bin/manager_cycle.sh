#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"
ensure_state_files
exec 8>"${MANAGER_ROOT}/state/locks/manager_cycle.lock"
flock -n 8 || { log "Another manager cycle is active; exiting"; exit 0; }

"${SCRIPT_DIR}/scan_results.sh" "$1"
"${SCRIPT_DIR}/check_globus_tasks.sh" "$1"
"${SCRIPT_DIR}/cleanup_transferred_samples.sh" "$1"
"${SCRIPT_DIR}/submit_globus_batch.sh" "$1"
"${SCRIPT_DIR}/submit_next_batch.sh" "$1"

used=$(disk_used_percent)
log "Cycle complete: disk=${used}% local_sample_dirs=$(local_sample_dir_count)"
awk -F '\t' 'NR>1{c[$4]++} END{for(k in c) print k"\t"c[k]}' "$STATUS_FILE" | sort >&2
