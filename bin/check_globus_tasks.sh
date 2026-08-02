#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"
ensure_state_files
command -v globus >/dev/null || die "globus CLI not found"

while IFS=$'\t' read -r batch_id task_id status sample_file submit_time last_update notes; do
    [[ "$batch_id" == batch_id ]] && continue
    [[ "$status" == ACTIVE ]] || continue
    current=$(globus task show "$task_id" --format=UNIX --jmespath status 2>/dev/null || echo UNKNOWN)
    case "$current" in
        SUCCEEDED)
            while IFS= read -r sample; do
                with_state_lock update_sample_row "$sample" TRANSFERRED_FULL "" "" "$task_id" "${DEST_ROOT%/}/${sample}/" "Globus task succeeded"
            done < "$sample_file"
            new_status=SUCCEEDED ;;
        FAILED|CANCELED|CANCELLED|EXPIRED)
            while IFS= read -r sample; do
                with_state_lock update_sample_row "$sample" TRANSFER_FAILED "" "" "$task_id" "" "Globus task $current"
            done < "$sample_file"
            new_status="$current" ;;
        *) new_status=ACTIVE ;;
    esac
    tmp="${TRANSFER_TASK_FILE}.tmp.$$"
    awk -F '\t' -v OFS='\t' -v b="$batch_id" -v s="$new_status" -v ts="$(now_iso)" 'NR==1{print;next} $1==b{$3=s;$6=ts} {print}' "$TRANSFER_TASK_FILE" > "$tmp"
    mv "$tmp" "$TRANSFER_TASK_FILE"
done < "$TRANSFER_TASK_FILE"
