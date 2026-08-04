#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ "$ENABLE_LOCAL_CLEANUP" == 1 ]] || { log "ENABLE_LOCAL_CLEANUP=0; no local deletion"; exit 0; }
mkdir -p "$ANALYSIS_ROOT"/{vcf,round2_coverage,numt_decoy_coverage,mtcn,receipts}
cleanup_one() {
 local sample="$1" dir task cram vcf cov2 covn mtcn tbi="" dest local_file relative_path remote_parent remote_basename remote_path listing
 dir="$LOCAL_RESULTS/$sample"
 task=$(awk -F '\t' -v s="$sample" 'NR>1&&$1==s{print $9}' "$STATUS_FILE"); dest="${DEST_ROOT%/}/$sample/"
 [[ -d "$dir" ]] || return 0
 cram=$(find_exact_one "$dir" "$sample.cram"); vcf=$(find_exact_one "$dir" "$sample.round2.original_coords.clean.final.split.vcf.gz"); cov2=$(find_exact_one "$dir" "$sample.round2.original_coords.per_base_coverage.tsv"); covn=$(find_exact_one "$dir" "$sample.numt_decoy.clean.realigned.per_base_coverage.tsv"); mtcn=$(find_exact_one "$dir" "$sample.round2.mtcn.tsv")
 require_nonempty "$cram" && require_nonempty "$vcf" && gzip -t "$vcf" >/dev/null 2>&1 && require_nonempty "$cov2" && require_nonempty "$covn" && require_nonempty "$mtcn" || { log "$sample final validation failed; refusing cleanup"; return 1; }
 [[ -s "${vcf}.tbi" ]] && tbi="${vcf}.tbi"
 if [[ "$DRY_RUN" == 1 ]]; then
   for local_file in "$cram" "$vcf" "$cov2" "$covn" "$mtcn"; do
     relative_path="${local_file#"$dir"/}"
     printf 'DRY RUN: globus ls %q\n' "${DEST_COLLECTION}:${DEST_ROOT%/}/${sample}/$(dirname "$relative_path")/"
   done
   printf 'DRY RUN: rm -rf -- %q\n' "$dir"
   return 0
 fi
 load_globus_module
 for local_file in "$cram" "$vcf" "$cov2" "$covn" "$mtcn"; do
   relative_path="${local_file#"$dir"/}"
   remote_parent=$(dirname "$relative_path")
   remote_basename=$(basename "$relative_path")
   remote_path="${DEST_ROOT%/}/${sample}/${remote_parent}/"
   listing=$(globus ls "${DEST_COLLECTION}:${remote_path}" 2>/dev/null) || {
     log "$sample destination directory not verified: $remote_path; refusing cleanup"
     log "$sample required filename: $remote_basename"
     return 1
   }
   if ! printf '%s\n' "$listing" | awk -v f="$remote_basename" '
     {
       gsub(/\r/, "")
       sub(/[[:space:]]+$/, "")
       sub(/\/$/, "")
       if ($0 == f) found=1
     }
     END { exit !found }
   '; then
     log "$sample destination missing core file: $remote_basename; refusing cleanup"
     log "$sample remote parent checked: $remote_path"
     log "$sample required filename: $remote_basename"
     log "$sample destination candidates (coverage, vcf, cram, or mtcn):"
     printf '%s\n' "$listing" | awk '
       {
         line=$0
         gsub(/\r/, "", line)
         sub(/[[:space:]]+$/, "", line)
         sub(/\/$/, "", line)
         lower=tolower(line)
         if (lower ~ /(coverage|vcf|cram|mtcn)/) print line
       }
     ' >&2
     return 1
   fi
 done
 local src target tmp; for pair in "$vcf:$ANALYSIS_ROOT/vcf" "$cov2:$ANALYSIS_ROOT/round2_coverage" "$covn:$ANALYSIS_ROOT/numt_decoy_coverage" "$mtcn:$ANALYSIS_ROOT/mtcn"; do src=${pair%%:*}; target=${pair#*:}/$(basename "$src"); tmp="${target}.tmp.$$"; cp -p "$src" "$tmp"; cmp -s "$src" "$tmp" || { rm -f "$tmp"; return 1; }; mv "$tmp" "$target"; done
 if [[ -n "$tbi" ]]; then target="$ANALYSIS_ROOT/vcf/$(basename "$tbi")"; tmp="${target}.tmp.$$"; cp -p "$tbi" "$tmp"; cmp -s "$tbi" "$tmp" || { rm -f "$tmp"; return 1; }; mv "$tmp" "$target"; fi
 receipt="$ANALYSIS_ROOT/receipts/$sample.transferred.tsv"; tmp="${receipt}.tmp.$$"; printf 'sample_id\tglobus_task_id\tdestination\tcleanup_time\n%s\t%s\t%s\t%s\n' "$sample" "$task" "$dest" "$(now_iso)" > "$tmp"; mv "$tmp" "$receipt"
 rm -rf -- "$dir"; with_state_lock update_sample_fields "$sample" "status=LOCAL_FINAL_RETAINED" "cleanup_status=COMPLETE" "notes=Workspace full copy verified; local final files retained"
}
while read -r s; do cleanup_one "$s" || true; done < <(get_samples_by_status '^TRANSFERRED_FULL$')
