#!/usr/bin/env bash
# Register results that predate manager-controlled waves without scheduling retries.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
interactive_override_set=${ALLOW_INTERACTIVE_IMPORT+x}; interactive_override=${ALLOW_INTERACTIVE_IMPORT:-0}
load_config "${1:-}"
[[ "$interactive_override_set" == x ]] && ALLOW_INTERACTIVE_IMPORT="$interactive_override"
: "${REQUIRE_SLURM_FOR_EXISTING_IMPORT:=1}" "${ALLOW_INTERACTIVE_IMPORT:=0}"
if [[ "$REQUIRE_SLURM_FOR_EXISTING_IMPORT" == 1 && -z "${SLURM_JOB_ID:-}" && "$ALLOW_INTERACTIVE_IMPORT" != 1 ]]; then
    cat >&2 <<'EOF'
Historical import performs CRAM validation and must run through Slurm.
Use:
bin/submit_import_existing.sh CONFIG
EOF
    exit 1
fi
ensure_state_files; validate_config
"${SCRIPT_DIR}/initialize_samples.sh" "$1"
# Inventory first-level directories before validation.  Keep recognized and
# unrecognized names visible for audit rather than inferring samples from files.
existing_file="${MANAGER_ROOT}/state/existing_local_samples.tsv"
unrecognized_file="${MANAGER_ROOT}/state/unrecognized_local_directories.tsv"
inventory_local_directories() {
    local tmp_existing="${existing_file}.tmp.$$" tmp_unknown="${unrecognized_file}.tmp.$$" dir name excluded
    printf 'sample_id\tlocal_directory\n' > "$tmp_existing"
    printf 'directory_name\tlocal_directory\n' > "$tmp_unknown"
    while IFS= read -r -d '' dir; do
        name=${dir##*/}; excluded=0
        for excluded_name in $LOCAL_RESULTS_EXCLUDE_DIRS; do [[ "$name" == "$excluded_name" ]] && { excluded=1; break; }; done
        (( excluded )) && continue
        if awk -F '\t' -v s="$name" '$1==s{found=1} END{exit !found}' "$ASSIGNED_SAMPLE_LIST"; then
            printf '%s\t%s\n' "$name" "$dir" >> "$tmp_existing"
        else
            printf '%s\t%s\n' "$name" "$dir" >> "$tmp_unknown"
        fi
    done < <(find "$LOCAL_RESULTS" -mindepth 1 -maxdepth 1 -type d -print0)
    mv "$tmp_existing" "$existing_file"; mv "$tmp_unknown" "$unrecognized_file"
}
with_state_lock inventory_local_directories
ALLOW_INTERACTIVE_FULL_SCAN=1 "${SCRIPT_DIR}/scan_results.sh" "$1"
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
