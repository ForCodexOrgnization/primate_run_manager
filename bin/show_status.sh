#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
determine_manager_phase
printf 'Manager phase: %s\n' "$(manager_phase)"
printf 'Pending new samples: %s\n' "$(awk -F '\t' 'NR>1&&$4=="PENDING"{n++}END{print n+0}' "$STATUS_FILE")"
printf 'Normal active waves: %s\n' "$(normal_active_wave_count)"
printf 'Deferred retry samples: %s\n' "$(awk -F '\t' 'NR>1&&$4=="PIPELINE_DEFERRED_RETRY"{n++}END{print n+0}' "$STATUS_FILE")"
printf 'Deferred running samples: %s\n' "$(awk -F '\t' 'NR>1&&$4=="PIPELINE_DEFERRED_RUNNING"{n++}END{print n+0}' "$STATUS_FILE")"
printf 'Deferred failed samples: %s\n' "$(awk -F '\t' 'NR>1&&$4=="PIPELINE_DEFERRED_FAILED"{n++}END{print n+0}' "$STATUS_FILE")"
printf 'Work filesystem: %s%%\n' "$(work_disk_used_percent)"
printf 'Terminal waves pending cleanup: %s\n' "$(awk -F '\t' -v d="${MANAGER_ROOT}/state/receipts/deferred_wave_work_cleanup" 'NR>1&&$9~/^(PARTIAL_COMPLETE|FAILED)$/&&system("test -f \""d"/"$1".tsv\"")!=0{n++}END{print n+0}' "$WAVE_STATUS_FILE")"
printf 'Estimated cleanup bytes: %s\n' "$(awk -F '\t' 'NR>1&&$9~/^(PARTIAL_COMPLETE|FAILED)$/{print $12}' "$WAVE_STATUS_FILE" | xargs -r du -sb 2>/dev/null | awk '{n+=$1}END{print n+0}')"
printf 'Orphan work count: %s\n' "$(find "$PIPELINE_WORK_ROOT" -mindepth 1 -maxdepth 1 -type d -name 'wave_*' 2>/dev/null | wc -l | tr -d ' ')"
printf 'Total samples: %s\n' "$(awk 'END{print NR-1}' "$STATUS_FILE")"
printf 'Counts by status:\n'; awk -F '\t' 'NR>1{n[$4]++}END{for(s in n)printf "  %-24s %d\n",s,n[s]}' "$STATUS_FILE" | sort
printf 'Active waves:\n'; awk -F '\t' 'NR>1&&$9~/^(CREATED|SUBMITTED|RUNNING)$/{printf "  %s job=%s\n",$1,$4}' "$WAVE_STATUS_FILE"
printf 'Active Globus task IDs:\n'; awk -F '\t' 'NR>1&&$3=="ACTIVE"{print "  "$2}' "$TRANSFER_TASK_FILE"
printf 'Disk use: %s%%\nLocal sample directories: %s\nRetained VCFs: %s\n' "$(disk_used_percent)" "$(local_sample_dir_count)" "$(find "$ANALYSIS_ROOT/vcf" -maxdepth 1 -type f -name '*.vcf.gz' 2>/dev/null | wc -l | tr -d ' ')"
printf 'Most recent errors:\n'; awk -F '\t' 'NR>1&&($8!=""||$4~/FAILED/){print $13"\t"$1"\t"($8!=""?$8:$14)}' "$STATUS_FILE" | sort -r | head -n 10
