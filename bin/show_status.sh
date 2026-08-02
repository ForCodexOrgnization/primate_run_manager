#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
printf 'Total samples: %s\n' "$(awk 'END{print NR-1}' "$STATUS_FILE")"
printf 'Counts by status:\n'; awk -F '\t' 'NR>1{n[$4]++}END{for(s in n)printf "  %-24s %d\n",s,n[s]}' "$STATUS_FILE" | sort
printf 'Active waves:\n'; awk -F '\t' 'NR>1&&$9~/^(CREATED|SUBMITTED|RUNNING)$/{printf "  %s job=%s\n",$1,$4}' "$WAVE_STATUS_FILE"
printf 'Active Globus task IDs:\n'; awk -F '\t' 'NR>1&&$3=="ACTIVE"{print "  "$2}' "$TRANSFER_TASK_FILE"
printf 'Disk use: %s%%\nLocal sample directories: %s\nRetained VCFs: %s\n' "$(disk_used_percent)" "$(local_sample_dir_count)" "$(find "$ANALYSIS_ROOT/vcf" -maxdepth 1 -type f -name '*.vcf.gz' 2>/dev/null | wc -l | tr -d ' ')"
printf 'Most recent errors:\n'; awk -F '\t' 'NR>1&&($8!=""||$4~/FAILED/){print $13"\t"$1"\t"($8!=""?$8:$14)}' "$STATUS_FILE" | sort -r | head -n 10
