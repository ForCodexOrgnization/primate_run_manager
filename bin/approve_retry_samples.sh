#!/usr/bin/env bash
set -euo pipefail
[[ $# -eq 2 ]] || { echo "Usage: $0 CONFIG SAMPLE_IDS_FILE" >&2; exit 2; }
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "$1"; ensure_state_files
[[ -s "$2" ]] || die "Sample ID file missing or empty: $2"
mapfile -t requested < <(awk 'NF{print $1}' "$2")
((${#requested[@]})) || die "No sample IDs supplied"
declare -A seen=()
for sample in "${requested[@]}"; do
    [[ -z "${seen[$sample]:-}" ]] || die "Duplicate requested sample: $sample"; seen[$sample]=1
    status=$(awk -F '\t' -v s="$sample" 'NR>1&&$1==s{print $4;exit}' "$STATUS_FILE")
    [[ "$status" == PIPELINE_INCOMPLETE_REVIEW ]] || die "$sample is not PIPELINE_INCOMPLETE_REVIEW (status=${status:-NOT_FOUND})"
done
approve_all() { local sample; for sample in "${requested[@]}"; do update_sample_fields "$sample" "status=PIPELINE_RETRY_READY" "notes=manual retry approved; existing outputs preserved"; done; }
with_state_lock approve_all
log "Approved ${#requested[@]} samples for conservative retry; no outputs were modified"
