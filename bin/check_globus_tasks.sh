#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ "$ENABLE_TRANSFER" == 1 ]] || exit 0
[[ "$DRY_RUN" == 1 ]] && { log "DRY_RUN: not querying Globus tasks"; exit 0; }
command -v globus >/dev/null || die "globus CLI not found"
mapfile -t rows < <(awk -F '\t' 'NR>1&&$3=="ACTIVE"{print $1"\t"$2"\t"$4}' "$TRANSFER_TASK_FILE")
for row in "${rows[@]}"; do IFS=$'\t' read -r batch task samples <<< "$row"; current=$(globus task show "$task" --format=UNIX --jmespath status 2>/dev/null || echo UNKNOWN); new=ACTIVE
 case "$current" in SUCCEEDED) new=SUCCEEDED;; FAILED|CANCELED|CANCELLED|EXPIRED) new="$current"; globus task event-list "$task" --filter-errors --limit 20 >&2 || true;; esac
 [[ "$new" == ACTIVE ]] && continue
 finish_task() { local tmp="${TRANSFER_TASK_FILE}.tmp.$$" s; awk -F '\t' -v OFS='\t' -v b="$batch" -v n="$new" -v t="$(now_iso)" 'NR==1{print;next}$1==b{$3=n;$6=t}{print}' "$TRANSFER_TASK_FILE" > "$tmp"; mv "$tmp" "$TRANSFER_TASK_FILE"; while read -r s; do if [[ "$new" == SUCCEEDED ]]; then update_sample_fields "$s" "status=TRANSFERRED_FULL" "globus_task_id=$task" "workspace_path=${DEST_ROOT%/}/$s/" "transfer_status=SUCCEEDED" "notes=Globus task succeeded"; else update_sample_fields "$s" "status=TRANSFER_FAILED" "globus_task_id=$task" "transfer_status=$new" "notes=Globus task $new"; fi; done < "$samples"; }
 with_state_lock finish_task
done
