#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ "$ENABLE_LOCAL_CLEANUP" == 1 ]] || { log "ENABLE_LOCAL_CLEANUP=0; no local deletion"; exit 0; }
mkdir -p "$ANALYSIS_ROOT"/{vcf,round2_coverage,numt_decoy_coverage,mtcn,receipts}
cleanup_one() {
 local sample="$1" dir task vcf cov2 covn mtcn tbi="" dest
 dir="$LOCAL_RESULTS/$sample"
 task=$(awk -F '\t' -v s="$sample" 'NR>1&&$1==s{print $9}' "$STATUS_FILE"); dest="${DEST_ROOT%/}/$sample/"
 [[ -d "$dir" ]] || return 0
 vcf=$(find_exact_one "$dir" "$sample.round2.original_coords.clean.final.split.vcf.gz"); cov2=$(find_exact_one "$dir" "$sample.round2.original_coords.per_base_coverage.tsv"); covn=$(find_exact_one "$dir" "$sample.numt_decoy.clean.realigned.per_base_coverage.tsv"); mtcn=$(find_exact_one "$dir" "$sample.round2.mtcn.tsv")
 require_nonempty "$vcf" && gzip -t "$vcf" >/dev/null 2>&1 && require_nonempty "$cov2" && require_nonempty "$covn" && require_nonempty "$mtcn" || { log "$sample final validation failed; refusing cleanup"; return 1; }
 [[ -s "${vcf}.tbi" ]] && tbi="${vcf}.tbi"
 if [[ "$DRY_RUN" == 1 ]]; then printf 'DRY RUN: globus ls %q\n' "${DEST_COLLECTION}:${dest}"; printf 'DRY RUN: rm -rf -- %q\n' "$dir"; return 0; fi
 command -v globus >/dev/null || die "globus CLI not found"
 local listing cram_name
 cram_name="$sample.cram"
 listing=$(globus ls "${DEST_COLLECTION}:${dest}" --recursive 2>/dev/null) || { log "$sample destination not verified"; return 1; }
 for required in "$cram_name" "$(basename "$vcf")" "$(basename "$cov2")" "$(basename "$covn")" "$(basename "$mtcn")"; do
   printf '%s\n' "$listing" | awk -v f="$required" '{sub(/\/$/,""); n=split($0,a,"/"); if(a[n]==f) found=1} END{exit !found}' || { log "$sample destination missing core file: $required; refusing cleanup"; return 1; }
 done
 local src target tmp; for pair in "$vcf:$ANALYSIS_ROOT/vcf" "$cov2:$ANALYSIS_ROOT/round2_coverage" "$covn:$ANALYSIS_ROOT/numt_decoy_coverage" "$mtcn:$ANALYSIS_ROOT/mtcn"; do src=${pair%%:*}; target=${pair#*:}/$(basename "$src"); tmp="${target}.tmp.$$"; cp -p "$src" "$tmp"; cmp -s "$src" "$tmp" || { rm -f "$tmp"; return 1; }; mv "$tmp" "$target"; done
 if [[ -n "$tbi" ]]; then target="$ANALYSIS_ROOT/vcf/$(basename "$tbi")"; tmp="${target}.tmp.$$"; cp -p "$tbi" "$tmp"; cmp -s "$tbi" "$tmp" || { rm -f "$tmp"; return 1; }; mv "$tmp" "$target"; fi
 receipt="$ANALYSIS_ROOT/receipts/$sample.transferred.tsv"; tmp="${receipt}.tmp.$$"; printf 'sample_id\tglobus_task_id\tdestination\tcleanup_time\n%s\t%s\t%s\t%s\n' "$sample" "$task" "$dest" "$(now_iso)" > "$tmp"; mv "$tmp" "$receipt"
 rm -rf -- "$dir"; with_state_lock update_sample_fields "$sample" "status=LOCAL_FINAL_RETAINED" "cleanup_status=COMPLETE" "notes=Workspace full copy verified; local final files retained"
}
while read -r s; do cleanup_one "$s" || true; done < <(get_samples_by_status '^TRANSFERRED_FULL$')
