#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
if command -v module >/dev/null 2>&1 && [[ -n "${SAMTOOLS_MODULE:-}" ]]; then module load "$SAMTOOLS_MODULE" >/dev/null 2>&1 || true; fi
upsert_validation() {
 local row="$1" sample="${row%%$'\t'*}" tmp="${VALIDATION_FILE}.tmp.$$"
 awk -F '\t' -v s="$sample" 'NR==1||$1!=s' "$VALIDATION_FILE" > "$tmp"; printf '%b\n' "$row" >> "$tmp"; mv "$tmp" "$VALIDATION_FILE"
}
while IFS=$'\t' read -r sample species hpc status job wave attempts error task workspace transfer cleanup updated oldnotes; do
 [[ "$sample" == sample_id ]] && continue
 case "$status" in TRANSFERRING|TRANSFERRED_FULL|LOCAL_FINAL_RETAINED) continue;; esac
 dir="${LOCAL_RESULTS}/${sample}"; cram="$dir/alignment/${sample}.cram"; crai="${cram}.crai"; [[ -s "$crai" ]] || crai="$dir/alignment/${sample}.crai"
 cram_ok=0; crai_ok=0; vcf_ok=0; cov2_ok=0; covn_ok=0; mtcn_ok=0; notes=()
 if [[ -s "$cram" ]]; then
   if command -v samtools >/dev/null 2>&1; then samtools quickcheck -v "$cram" >/dev/null 2>&1 && { cram_ok=1; touch "${cram}.complete"; } || notes+=(invalid_cram); else cram_ok=1; fi
 else notes+=(missing_cram); fi
 [[ -s "$crai" ]] && crai_ok=1 || notes+=(missing_crai)
 vcf=$(find_exact_one "$dir" "${sample}.round2.original_coords.clean.final.split.vcf.gz"); cov2=$(find_exact_one "$dir" "${sample}.round2.original_coords.per_base_coverage.tsv"); covn=$(find_exact_one "$dir" "${sample}.numt_decoy.clean.realigned.per_base_coverage.tsv"); mtcn=$(find_exact_one "$dir" "${sample}.round2.mtcn.tsv")
 if require_nonempty "$vcf" && gzip -t "$vcf" >/dev/null 2>&1; then vcf_ok=1; else notes+=(missing_or_invalid_vcf); fi
 require_nonempty "$cov2" && cov2_ok=1 || notes+=(missing_round2_coverage)
 require_nonempty "$covn" && covn_ok=1 || notes+=(missing_numt_coverage)
 require_nonempty "$mtcn" && mtcn_ok=1 || notes+=(missing_mtcn)
 overall=0; ((cram_ok&&crai_ok&&vcf_ok&&cov2_ok&&covn_ok&&mtcn_ok)) && overall=1
 note=$(IFS=';'; echo "${notes[*]:-validated}"); row="$sample\t$cram_ok\t$crai_ok\t$vcf_ok\t$cov2_ok\t$covn_ok\t$mtcn_ok\t$overall\t$(now_iso)\t$note"; with_state_lock upsert_validation "$row"
 if ((overall)); then
   case "$status" in PENDING|WAVE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_COMPLETE|PIPELINE_INCOMPLETE|PIPELINE_FAILED|TRANSFER_FAILED) with_state_lock update_sample_fields "$sample" "status=READY_TO_TRANSFER" "transfer_status=READY" "notes=all required outputs validated";; esac
 elif [[ "$status" =~ ^(WAVE_SUBMITTED|PIPELINE_RUNNING)$ ]] && ! wave_is_active "$wave"; then
   with_state_lock update_sample_fields "$sample" "status=PIPELINE_INCOMPLETE" "notes=$note"
 elif [[ "$status" =~ ^(PENDING|PIPELINE_INCOMPLETE|PIPELINE_FAILED)$ && -d "$dir" ]]; then with_state_lock update_sample_fields "$sample" "notes=$note"; fi
done < "$STATUS_FILE"
