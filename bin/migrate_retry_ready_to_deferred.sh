#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files; mode=${2:---dry-run}
[[ "$mode" == --dry-run || "$mode" == --apply ]] || die "usage: $0 CONFIG [--dry-run|--apply]"
awk -F '\t' 'NR>1&&$4=="PIPELINE_RETRY_READY"{print $1}' "$STATUS_FILE"
if [[ "$mode" == --apply ]]; then
 tmp="${STATUS_FILE}.tmp.$$"; awk -F '\t' -v OFS='\t' 'NR>1&&$4=="PIPELINE_RETRY_READY"{$4="PIPELINE_DEFERRED_RETRY";$14=($14==""?"manual retry migration":$14"; manual retry migration")}{print}' "$STATUS_FILE" > "$tmp"; mv "$tmp" "$STATUS_FILE"
fi
