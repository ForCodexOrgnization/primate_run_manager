#!/usr/bin/env bash
# Register results that predate manager-controlled waves without scheduling retries.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files; validate_config
"${SCRIPT_DIR}/initialize_samples.sh" "$1"
"${SCRIPT_DIR}/scan_results.sh" "$1"
classify_imports() {
    local sample status next
    while IFS=$'\t' read -r sample _ _ status _; do
        [[ "$sample" == sample_id || "$status" != PENDING || ! -d "$LOCAL_RESULTS/$sample" ]] && continue
        next=PIPELINE_INCOMPLETE_REVIEW
        [[ "$AUTO_RETRY_IMPORTED_INCOMPLETE" == 1 ]] && next=PIPELINE_RETRY_READY
        update_sample_fields "$sample" "status=$next" "notes=historical outputs imported incomplete; review required before retry"
    done < "$STATUS_FILE"
}
with_state_lock classify_imports
"${SCRIPT_DIR}/report_incomplete_samples.sh" "$1"
