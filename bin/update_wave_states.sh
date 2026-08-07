#!/usr/bin/env bash
# Historical filename retained; submission status is audit metadata only.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
command -v sacct >/dev/null 2>&1 || { log "sacct unavailable; submission audit states unchanged"; exit 0; }
if ! find "${MANAGER_ROOT}/state/submission_task_map" -type f -name "*.tsv" -print -quit 2>/dev/null | grep -q .; then exec "${SCRIPT_DIR}/update_legacy_wave_states.sh" "$1"; fi
while IFS=$'\t' read -r id job; do
 [[ -n "$job" ]] || continue
 map="${MANAGER_ROOT}/state/submission_task_map/${id}.tsv"; [[ -s "$map" ]] || continue
 active=0; complete=0; failed=0; total=0
 while read -r task; do
   state=$(submission_task_state "$job" "$task"); [[ -n "$state" ]] || continue; total=$((total+1))
   if slurm_state_is_active "$state"; then active=$((active+1)); elif [[ "$state" == COMPLETED ]]; then complete=$((complete+1)); else failed=$((failed+1)); fi
 done < <(awk -F '\t' 'NR>1&&!seen[$5]++{print $5}' "$map")
 (( total > 0 )) || continue
 if (( active > 0 )); then status=RUNNING; slurm=ACTIVE
 elif (( failed > 0 )); then status=FAILED; (( complete > 0 )) && status=PARTIAL_COMPLETE; slurm=TERMINAL
 else status=COMPLETE; slurm=COMPLETED; fi
 with_state_lock update_wave_row "$id" "status=$status" "slurm_state=$slurm" "complete_count=$complete" "incomplete_count=$failed" "notes=audit only; exact tasks reconciled independently"
done < <(awk -F '\t' 'NR>1&&$9~/^(CREATED|SUBMITTED|RUNNING|PARTIAL_COMPLETE)$/{print $1"\t"$4}' "$WAVE_STATUS_FILE")
