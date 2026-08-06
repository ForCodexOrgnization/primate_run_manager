#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ "$PIPELINE_MODE" == streaming_per_sample && "$ARCHIVE_FAILURE_DIAGNOSTICS" == 1 ]] || exit 0
while IFS=$'\t' read -r sample status wave; do
 safe_sample_id "$sample" || { log "Refusing unsafe sample ID in state: $sample"; continue; }
 marker="${PIPELINE_WORK_ROOT}/.sample_state/${sample}.failure.tsv"; [[ -s "$marker" ]] || continue
 archive="${MANAGER_ROOT}/state/failure_diagnostics/samples/${sample}"; [[ -f "$archive/ARCHIVE_COMPLETE" ]] && continue
 mkdir -p "$archive/log_tails"; cp -p "$marker" "$archive/failure.tsv"
 manifest=$(wave_field "$wave" sample_manifest 2>/dev/null || true)
 [[ -s "$manifest" ]] && awk -F '\t' -v s="$sample" '$1==s' "$manifest" > "$archive/manifest_row.tsv"
 work=$(sample_work_root "$sample"); bytes=$(du -sb "$work" 2>/dev/null | awk '{print $1+0}')
 { printf 'sample_id\treference_name\tfailed_stage\tfailure_class\tfailure_reason\timmediate_worker_attempt\tfirst_failure_epoch\tlast_failure_epoch\tfingerprint\tsample_work_bytes\n'
   printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$sample" "$(marker_field "$marker" reference_name)" "$(marker_field "$marker" failed_stage)" "$(marker_field "$marker" failure_class)" "$(marker_field "$marker" failure_reason)" "$(marker_field "$marker" immediate_worker_attempt)" "$(marker_field "$marker" first_failure_epoch)" "$(marker_field "$marker" last_failure_epoch)" "$(marker_field "$marker" fingerprint)" "${bytes:-0}"; } > "$archive/summary.tsv"
 find "$work" -type f 2>/dev/null | sed "s#^$work/##" > "$archive/original_paths.txt" || true
 find "$work" -type f \( -name '.nextflow.log' -o -name '.command.err' -o -name '.command.log' -o -name '*.log' \) -size "-${MAX_DIAGNOSTIC_FILE_BYTES}c" -print0 2>/dev/null | while IFS= read -r -d '' f; do tail -n "$MAX_DIAGNOSTIC_LOG_LINES" "$f" > "$archive/log_tails/$(printf '%s' "$f" | sha256sum | cut -c1-16).tail"; done
 map="${MANAGER_ROOT}/state/array_sample_map/${wave}.tsv"; if [[ -s "$map" ]] && command -v sacct >/dev/null 2>&1; then job=$(awk -F '\t' -v s="$sample" 'NR>1&&$4==s{print $2"_"$3;exit}' "$map"); [[ -n "$job" ]] && sacct -n -j "$job" --format=JobIDRaw,State,ExitCode,Elapsed,MaxRSS --parsable2 > "$archive/sacct.tsv" 2>/dev/null || true; fi
 : > "$archive/ARCHIVE_COMPLETE"
done < <(awk -F '\t' 'NR>1&&$4~/^(PIPELINE_DEFERRED_RETRY|PIPELINE_DEFERRED_FAILED)$/{print $1"\t"$4"\t"$6}' "$STATUS_FILE")
