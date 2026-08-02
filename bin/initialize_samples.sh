#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ -s "$ASSIGNED_SAMPLE_LIST" ]] || die "Assigned sample list missing or empty"
awk -F '\t' 'NF!=2||$1==""||$2==""{exit 1}' "$ASSIGNED_SAMPLE_LIST" || die "Sample list must be exactly two non-empty TAB-separated columns"
[[ -z "$(cut -f1 "$ASSIGNED_SAMPLE_LIST" | sort | uniq -d)" ]] || die "Duplicate sample IDs in assigned list"
add_samples() {
    local tmp="${STATUS_FILE}.tmp.$$"
    cat "$STATUS_FILE" > "$tmp"
    while IFS=$'\t' read -r sample species; do
        awk -F '\t' -v s="$sample" 'NR>1&&$1==s{found=1} END{exit !found}' "$STATUS_FILE" && continue
        printf '%s\t%s\t%s\tPENDING\t\t\t0\t\t\t\t\t\t%s\tinitialized\n' "$sample" "$species" "$HPC_NAME" "$(now_iso)" >> "$tmp"
    done < "$ASSIGNED_SAMPLE_LIST"
    mv "$tmp" "$STATUS_FILE"
}
with_state_lock add_samples
log "Initialized $(($(wc -l < "$STATUS_FILE")-1)) samples"
