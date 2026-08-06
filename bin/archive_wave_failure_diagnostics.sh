#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ "$ARCHIVE_FAILURE_DIAGNOSTICS" == 1 ]] || exit 0
archive_one() {
 local wave="$1" manifest work dest tmp job f rel size
 manifest=$(wave_field "$wave" sample_manifest); work=$(wave_field "$wave" work_root); job=$(wave_field "$wave" pipeline_job_id)
 dest="${MANAGER_ROOT}/state/failure_diagnostics/${wave}"; [[ -f "$dest/ARCHIVE_COMPLETE" ]] && return 0
 tmp="${dest}.tmp.$$"; rm -rf "$tmp"; mkdir -p "$tmp/files"
 printf 'wave_id\t%s\nwork_root\t%s\nwork_root_bytes\t%s\npipeline_commit\t%s\nconfig_checksum\t%s\nmanifest_checksum\t%s\nfailure_class\t%s\n' "$wave" "$work" "$(du -sb "$work" 2>/dev/null | awk '{print $1+0}')" "$(wave_field "$wave" pipeline_git_commit)" "$(wave_field "$wave" pipeline_config_sha256)" "$(wave_field "$wave" pipeline_manifest_sha256)" "$(wave_field "$wave" failure_class)" > "$tmp/summary.tsv"
 [[ -f "$manifest" ]] && cp -p "$manifest" "$tmp/wave_manifest.tsv"
 if command -v sacct >/dev/null && [[ -n "$job" ]]; then sacct -j "$job" --format=JobIDRaw,State,ExitCode,Elapsed,NodeList --parsable2 > "$tmp/sacct.tsv" 2>&1 || true; fi
 while IFS= read -r -d '' f; do
   case "$f" in *.fastq|*.fastq.gz|*.fq|*.fq.gz|*.bam|*.cram|*.chunk* ) continue;; esac
   rel=${f#"$work"/}; size=$(stat -c %s "$f" 2>/dev/null || echo 0)
   mkdir -p "$tmp/files/$(dirname "$rel")"
   if (( size > MAX_DIAGNOSTIC_FILE_BYTES )); then tail -n "$MAX_DIAGNOSTIC_LOG_LINES" "$f" > "$tmp/files/${rel}.tail"; printf '%s\t%s\t%s\n' "$f" "$size" "tail only" >> "$tmp/truncated_files.tsv"; else cp -p "$f" "$tmp/files/$rel"; fi
 done < <(find "$work" -type f \( -path '*/batch_lists/*' -o -path '*/batch_status/*' -o -name '*.log' -o -name '*.err' -o -name '.nextflow.log' -o -name 'trace*' -o -name 'report*' -o -name 'timeline*' -o -name '.command.err' -o -name '.command.log' \) -print0 2>/dev/null)
 [[ -f "${MANAGER_ROOT}/logs/${wave}.submit.log" ]] && tail -n "$MAX_DIAGNOSTIC_LOG_LINES" "${MANAGER_ROOT}/logs/${wave}.submit.log" > "$tmp/submit.log"
 touch "$tmp/ARCHIVE_COMPLETE"; rm -rf "$dest"; mv "$tmp" "$dest"
}
while IFS= read -r wave; do archive_one "$wave"; done < <(awk -F '\t' 'NR>1&&$9~/^(PARTIAL_COMPLETE|FAILED)$/{print $1}' "$WAVE_STATUS_FILE")
