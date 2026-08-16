#!/usr/bin/env bash
# Garbage-collect detached launcher quarantine generations. Candidates are
# strictly direct children of PIPELINE_WORK_ROOT; this script never searches.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ "$PIPELINE_MODE" == streaming_per_sample ]] || exit 0
mode="${2:---apply}"; [[ "$mode" == --apply || "$mode" == --dry-run ]] || die "usage: $0 CONFIG [--dry-run|--apply]"
[[ "$mode" == --dry-run ]] && apply=0 || apply=$((1-DRY_RUN))
root=$(realpath -e "$PIPELINE_WORK_ROOT"); [[ "$root" != / ]] || die "unsafe PIPELINE_WORK_ROOT"
receipt_dir="$MANAGER_ROOT/state/receipts/stale_sample_work_cleanup"; mkdir -p "$receipt_dir"
used_now() { if [[ -n "${WORK_DISK_USED_PERCENT_OVERRIDE:-}" ]]; then printf '%s\n' "$WORK_DISK_USED_PERCENT_OVERRIDE"; else work_disk_used_percent; fi; }
stale_submission_safety() {
    local wanted="$1" map row job task state found=0
    for map in "$MANAGER_ROOT"/state/submission_task_map/*.tsv; do
        [[ -s "$map" ]] || continue
        while IFS=$'\t' read -r job task; do
            found=1
            state=$(submission_task_state "$job" "$task")
            if [[ -z "$state" ]]; then printf 'submission_state_unknown\n'; return
            elif slurm_state_is_active "$state"; then printf 'active_slurm_ownership\n'; return; fi
        done < <(awk -F '\t' -v p="$wanted" 'NR>1&&$10==p{print $4"\t"$5}' "$map")
    done
    (( found )) && printf 'terminal_submission_reference\n' || printf 'detached\n'
}
used=$(used_now); (( used >= FAILED_CACHE_CLEAN_TRIGGER_PERCENT )) || { log "Stale cleanup not needed: usage ${used}%"; exit 0; }

candidates=$(mktemp); trap 'rm -f "$candidates"' EXIT
# GNU find's maxdepth/mindepth are the direct-child safety boundary. The name
# filter avoids inspecting canonical workspaces; Bash still validates the full
# stale-generation grammar before any candidate is trusted.
find "$root" -mindepth 1 -maxdepth 1 -type d -name '*.stale.*' -printf '%T@\t%f\n' | sort -n > "$candidates"
deleted=0
while IFS=$'\t' read -r mtime base; do
    [[ -n "$base" ]] || continue
    if [[ ! "$base" =~ ^([A-Za-z0-9][A-Za-z0-9._-]*)\.stale\.([0-9]{8}T[0-9]{6}Z)\.([0-9]+)$ ]]; then
        continue
    fi
    sample=${BASH_REMATCH[1]}; timestamp=${BASH_REMATCH[2]}; generation=${BASH_REMATCH[3]}
    path="$root/$base"; decision=PROTECTED; reason=unsafe_path
    canonical="$root/$sample"; resolved=$(realpath -e "$path" 2>/dev/null || true)
    if ! safe_sample_id "$sample" || [[ "$resolved" != "$path" || "$path" == "$canonical" ]]; then reason=unsafe_path
    elif [[ -s "$MANAGER_ROOT/state/recovery_protected_stale_paths.tsv" ]] && awk -F '\t' -v p="$path" '$1==p{f=1}END{exit !f}' "$MANAGER_ROOT/state/recovery_protected_stale_paths.tsv"; then reason=active_recovery_protection
    else
        ownership=$(stale_submission_safety "$path")
        if [[ "$ownership" == active_slurm_ownership ]]; then reason=active_slurm_ownership
        elif [[ "$ownership" == submission_state_unknown ]]; then reason=submission_state_unknown
        else
            exec {lock_fd}>"$root/.locks/$sample.lock"
            if ! flock -n "$lock_fd"; then reason=active_sample_lock
            else decision=ELIGIBLE; reason=detached_stale_generation; fi
        fi
    fi
    bytes=$(du -sb -- "$path" 2>/dev/null | awk '{print $1+0}'); age=$(( $(date +%s) - ${mtime%.*} ))
    log "sample=$sample stale_path=$path age_seconds=$age size_bytes=${bytes:-unknown} decision=$decision reason=$reason"
    if [[ "$decision" == ELIGIBLE && "$apply" == 1 ]]; then
        # Recheck direct-child identity immediately before the destructive step.
        [[ "$(dirname -- "$path")" == "$root" && "$(realpath -e "$path")" == "$path" ]] || { flock -u "$lock_fd"; exec {lock_fd}>&-; continue; }
        before=$(used_now); receipt="$receipt_dir/${sample}.${timestamp}.${generation}.tsv"
        { printf 'sample_id\tstale_path\tbytes_released\tcleanup_time\teligibility_reason\twork_filesystem_usage_before_cleanup\n'; printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$sample" "$path" "${bytes:-0}" "$(now_iso)" "$reason" "$before"; } > "$receipt"
        if rm -rf --one-file-system -- "$path"; then deleted=$((deleted+1)); else log "ERROR: deletion failed; receipt=$receipt"; exit 1; fi
        flock -u "$lock_fd"; exec {lock_fd}>&-
        used=$(used_now); (( used < FAILED_CACHE_CLEAN_TARGET_PERCENT )) && { log "Stale cleanup stopped: target reached (${used}%)"; exit 0; }
    elif [[ "${lock_fd:-}" =~ ^[0-9]+$ ]]; then flock -u "$lock_fd" 2>/dev/null || true; exec {lock_fd}>&-; unset lock_fd; fi
done < "$candidates"
log "Stale cleanup stopped: candidates exhausted (deleted $deleted)"
