#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ "$CLEAN_TERMINAL_DEFERRED_WORK" == 1 && "$DRY_RUN" == 0 ]] || exit 0
while IFS=$'\t' read -r wave manifest work job; do
 [[ -n "$wave" && -n "$work" ]] || continue
 root=$(realpath -m "$PIPELINE_WORK_ROOT"); target=$(realpath -m "$work")
 [[ "$target" != / && "$target" != "$root" && "$target" == "$root"/wave_* ]] || { log "Refusing unsafe work root: $work"; continue; }
 [[ -f "${MANAGER_ROOT}/state/failure_diagnostics/${wave}/ARCHIVE_COMPLETE" ]] || { log "$wave diagnostics not complete"; continue; }
 wave_is_active "$wave" && continue
 if command -v sacct >/dev/null 2>&1 && [[ -n "$job" ]] && sacct -n -j "$job" --format=State --parsable2 2>/dev/null | awk -F '|' '{sub(/ .*/,"",$1);sub(/\+$/, "", $1)} $1~/^(PENDING|RUNNING|CONFIGURING|COMPLETING|REQUEUED|RESIZING|SUSPENDED)$/{active=1}END{exit !active}'; then continue; fi
 work_root_resume_in_use "$target" && continue
 [[ ! -e "$target/.manager_resume.lock" ]] || continue
 awk -F '\t' -v w="$wave" -v r="$target" 'NR>1&&$1!=w&&$12==r&&$9~/^(CREATED|SUBMITTED|RUNNING)$/{bad=1}END{exit bad?0:1}' "$WAVE_STATUS_FILE" && continue
 bad=$(awk -F '\t' -v w="$wave" 'NR>1&&$6==w&&$4!~/^(READY_TO_TRANSFER|TRANSFERRING|TRANSFERRED_FULL|LOCAL_FINAL_RETAINED|PIPELINE_DEFERRED_RETRY|PIPELINE_DEFERRED_FAILED)$/{n++}END{print n+0}' "$STATUS_FILE"); (( bad == 0 )) || continue
 failed=$(awk -F '\t' -v w="$wave" 'NR>1&&$6==w&&$4~/^PIPELINE_DEFERRED_/{n++}END{print n+0}' "$STATUS_FILE"); success=$(awk -F '\t' -v w="$wave" 'NR>1&&$6==w&&$4~/^(READY_TO_TRANSFER|TRANSFERRING|TRANSFERRED_FULL|LOCAL_FINAL_RETAINED)$/{n++}END{print n+0}' "$STATUS_FILE")
 bytes=$(du -sb "$target" 2>/dev/null | awk '{print $1+0}'); status=REMOVED
 [[ -e "$target" ]] && rm -rf --one-file-system "$target" || status=ALREADY_ABSENT
 receipt="${MANAGER_ROOT}/state/receipts/deferred_wave_work_cleanup/${wave}.tsv"; { printf 'wave_id\twork_root\tbytes_released\tcleanup_time\tfailed_samples\tsuccessful_samples\tdiagnostics_path\tstatus\n'; printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$wave" "$target" "$bytes" "$(now_iso)" "$failed" "$success" "${MANAGER_ROOT}/state/failure_diagnostics/${wave}" "$status"; } > "${receipt}.tmp.$$"; mv "${receipt}.tmp.$$" "$receipt"
done < <(awk -F '\t' 'NR>1&&$9~/^(PARTIAL_COMPLETE|FAILED)$/{print $1"\t"$2"\t"$12"\t"$4}' "$WAVE_STATUS_FILE")
