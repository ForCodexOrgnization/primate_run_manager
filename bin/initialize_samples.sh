#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"
ensure_state_files

[[ -s "$ASSIGNED_SAMPLE_LIST" ]] || die "Assigned sample list missing or empty: $ASSIGNED_SAMPLE_LIST"

if awk -F '\t' 'NF!=2 || $1=="" || $2=="" {bad=1} END{exit bad}' "$ASSIGNED_SAMPLE_LIST"; then :; else
    die "Sample list must contain exactly two non-empty TAB-separated columns and no header"
fi

if cut -f1 "$ASSIGNED_SAMPLE_LIST" | sort | uniq -d | grep -q .; then
    die "Duplicate sample IDs detected in $ASSIGNED_SAMPLE_LIST"
fi

with_state_lock bash -c '
    set -euo pipefail
    tmp="${STATUS_FILE}.tmp.$$"
    state_header > "$tmp"
    while IFS=$'"'"'\t'"'"' read -r sample species; do
        old=$(awk -F '\t' -v s="$sample" '"'"'NR>1 && $1==s {print $0; exit}'"'"' "$STATUS_FILE" || true)
        if [[ -n "$old" ]]; then
            printf "%s\n" "$old" >> "$tmp"
        else
            printf "%s\t%s\t%s\tPENDING\t\t\t\t\t%s\tinitialized\n" "$sample" "$species" "$HPC_NAME" "$(now_iso)" >> "$tmp"
        fi
    done < "$ASSIGNED_SAMPLE_LIST"
    mv "$tmp" "$STATUS_FILE"
'

log "Initialized $(($(wc -l < "$STATUS_FILE")-1)) samples in $STATUS_FILE"
