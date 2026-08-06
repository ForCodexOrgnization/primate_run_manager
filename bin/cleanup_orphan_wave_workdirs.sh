#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
mode=${2:---dry-run}; [[ "$mode" == --dry-run || "$mode" == --apply ]] || die "usage: $0 CONFIG [--dry-run|--apply]"
[[ "$ENABLE_ORPHAN_WORK_CLEANUP" == 1 ]] || exit 0
root=$(realpath -m "$PIPELINE_WORK_ROOT"); [[ "$root" != / ]] || die "PIPELINE_WORK_ROOT may not be /"
find "$root" -mindepth 1 -maxdepth 1 -type d -name 'wave_*' -mmin "+$((ORPHAN_WORK_RETENTION_HOURS*60))" -print0 2>/dev/null | while IFS= read -r -d '' dir; do
 target=$(realpath -m "$dir"); [[ "$target" == "$root"/wave_* ]] || continue
 [[ ! -e "$target/.manager_resume.lock" ]] || continue
 if awk -F '\t' -v r="$target" 'NR>1&&$12==r&&$9~/^(CREATED|SUBMITTED|RUNNING)$/{found=1}END{exit !found}' "$WAVE_STATUS_FILE"; then continue; fi
 wave=$(awk -F '\t' -v r="$target" 'NR>1&&$12==r{w=$1;s=$9}END{if(s~/^(COMPLETE|PARTIAL_COMPLETE|FAILED|CANCELLED)$/)print w}' "$WAVE_STATUS_FILE")
 if [[ -n "$wave" && ! -f "${MANAGER_ROOT}/state/receipts/deferred_wave_work_cleanup/${wave}.tsv" && ! -f "${MANAGER_ROOT}/state/failure_diagnostics/${wave}/ARCHIVE_COMPLETE" ]]; then continue; fi
 printf '%s\n' "$target"; [[ "$mode" == --apply && "$DRY_RUN" == 0 ]] && rm -rf --one-file-system "$target"
done
