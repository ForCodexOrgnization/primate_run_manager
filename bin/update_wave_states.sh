#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
command -v sacct >/dev/null 2>&1 || { log "sacct unavailable; wave states unchanged"; exit 0; }
mapfile -t waves < <(awk -F '\t' 'NR>1&&$9~/^(CREATED|SUBMITTED|RUNNING)$/{print $1"\t"$4}' "$WAVE_STATUS_FILE")
for entry in "${waves[@]}"; do
 wave=${entry%%$'\t'*}; job=${entry#*$'\t'}; [[ -n "$job" ]] || continue
 state=$(sacct -n -X -j "$job" --format=State --parsable2 2>/dev/null | awk -F '|' 'NF{gsub(/[ +].*/,"",$1);if($1!=""){print $1;exit}}')
 [[ -n "$state" ]] || continue
 case "$state" in
  PENDING|CONFIGURING) with_state_lock update_wave_row "$wave" "slurm_state=$state" "status=SUBMITTED";;
  RUNNING|COMPLETING) with_state_lock update_wave_row "$wave" "slurm_state=$state" "status=RUNNING"; while read -r s; do with_state_lock update_sample_fields "$s" "status=PIPELINE_RUNNING"; done < <(samples_in_wave "$wave");;
  COMPLETED|FAILED|CANCELLED|TIMEOUT|OUT_OF_MEMORY|NODE_FAIL|PREEMPTED|BOOT_FAIL|DEADLINE)
    # Close the wave first, then let strict per-sample validation decide outcomes.
    terminal=COMPLETE; [[ "$state" == CANCELLED ]] && terminal=CANCELLED; [[ "$state" != COMPLETED && "$state" != CANCELLED ]] && terminal=FAILED
    with_state_lock update_wave_row "$wave" "slurm_state=$state" "status=$terminal"
    "${SCRIPT_DIR}/scan_results.sh" "$1"
    complete=0; incomplete=0
    while read -r s; do st=$(awk -F '\t' -v x="$s" 'NR>1&&$1==x{print $4}' "$STATUS_FILE"); if [[ "$st" == READY_TO_TRANSFER || "$st" == PIPELINE_COMPLETE ]]; then complete=$((complete+1)); else incomplete=$((incomplete+1)); with_state_lock update_sample_fields "$s" "status=PIPELINE_INCOMPLETE" "last_pipeline_error=$([[ "$state" == COMPLETED ]] && echo '' || echo "$state")" "notes=wave ended; per-sample outputs incomplete"; fi; done < <(awk -F '\t' -v w="$wave" 'NR>1&&$6==w{print $1}' "$STATUS_FILE")
    final=$terminal; ((complete>0&&incomplete>0)) && final=PARTIAL_COMPLETE; ((incomplete==0)) && final=COMPLETE
    with_state_lock update_wave_row "$wave" "complete_count=$complete" "incomplete_count=$incomplete" "status=$final";;
 esac
done
