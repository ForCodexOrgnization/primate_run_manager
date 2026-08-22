#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ "$ENABLE_TRANSFER" == 1 ]] || exit 0
[[ "$DRY_RUN" == 1 ]] && { log "DRY_RUN: not querying Globus tasks"; exit 0; }
module_err=$(mktemp); err=$(mktemp); trap 'rm -f "$module_err" "$err"' EXIT
if ! (load_globus_module) 2>"$module_err"; then
  detail=$(cat "$module_err"); record_globus_health UNKNOWN task_check 127 "$detail"; log "Globus health UNKNOWN: $detail; ACTIVE tasks preserved"; exit 1
fi
mapfile -t rows < <(awk -F '\t' 'NR>1&&$3=="ACTIVE"{print $1"\t"$2"\t"$4}' "$TRANSFER_TASK_FILE")
unknown=0
for row in "${rows[@]}"; do
 IFS=$'\t' read -r batch task samples <<< "$row"
 : > "$err"; set +e; current=$(globus task show "$task" --format=UNIX --jmespath status 2>"$err"); rc=$?; set -e
 if (( rc != 0 )); then
   health=UNKNOWN; (( rc == 4 )) && health=AUTH_REQUIRED
   detail=$(cat "$err"); record_globus_health "$health" task_show "$rc" "$detail"
   log "Globus task $task status $health (rc=$rc): ${detail:-no stderr}; conservatively preserving ACTIVE"
   unknown=$((unknown+1)); continue
 fi
 new=ACTIVE
 case "$current" in
   SUCCEEDED) new=SUCCEEDED;;
   FAILED|CANCELED|CANCELLED|EXPIRED) new="$current"; globus task event-list "$task" --filter-errors --limit 20 >&2 || true;;
   ACTIVE|INACTIVE) ;;
   *) record_globus_health UNKNOWN task_show 0 "task $task returned status ${current:-empty}"; log "Globus task $task returned UNKNOWN status '${current:-empty}'; conservatively preserving ACTIVE"; unknown=$((unknown+1));;
 esac
 [[ "$new" == ACTIVE ]] && continue
 finish_task() { local tmp="${TRANSFER_TASK_FILE}.tmp.$$" s; awk -F '\t' -v OFS='\t' -v b="$batch" -v n="$new" -v t="$(now_iso)" 'NR==1{print;next}$1==b{$3=n;$6=t}{print}' "$TRANSFER_TASK_FILE" > "$tmp"; mv "$tmp" "$TRANSFER_TASK_FILE"; while read -r s; do if [[ "$new" == SUCCEEDED ]]; then update_sample_fields "$s" "status=TRANSFERRED_FULL" "globus_task_id=$task" "workspace_path=${DEST_ROOT%/}/$s/" "transfer_status=SUCCEEDED" "notes=Globus task succeeded"; else update_sample_fields "$s" "status=TRANSFER_FAILED" "globus_task_id=$task" "transfer_status=$new" "notes=Globus task $new"; fi; done < "$samples"; }
 with_state_lock finish_task
done
(( unknown > 0 )) || record_globus_health HEALTHY task_check 0 "checked ${#rows[@]} active task(s)"
(( unknown == 0 ))
