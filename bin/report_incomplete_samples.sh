#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
REPORT_FILE="${MANAGER_ROOT}/state/pipeline_incomplete_report.tsv"
write_report() {
 local tmp="${REPORT_FILE}.tmp.$$" sample status wave attempts error updated dir cram crai vcf cov2 covn mtcn mc mci mv m2 mn mm gz qc slurm
 printf 'sample_id\tstatus\twave_id\tpipeline_attempts\tmissing_cram\tmissing_crai\tmissing_vcf\tmissing_round2_coverage\tmissing_numt_coverage\tmissing_mtcn\tvcf_gzip_valid\tcram_quickcheck_valid\tlast_slurm_state\tlast_pipeline_error\tlast_update\n' > "$tmp"
 while IFS=$'\t' read -r sample _ _ status _ wave attempts error _; do
  [[ "$sample" == sample_id ]] && continue
  case "$status" in PIPELINE_INCOMPLETE_REVIEW|PIPELINE_RETRY_READY|PIPELINE_RETRY_RUNNING|PIPELINE_FAILED) ;; *) continue;; esac
  dir="$LOCAL_RESULTS/$sample"; cram="$dir/alignment/$sample.cram"; crai="${cram}.crai"; [[ -s "$crai" ]] || crai="$dir/alignment/$sample.crai"
  vcf=$(find_exact_one "$dir" "$sample.round2.original_coords.clean.final.split.vcf.gz"); cov2=$(find_exact_one "$dir" "$sample.round2.original_coords.per_base_coverage.tsv"); covn=$(find_exact_one "$dir" "$sample.numt_decoy.clean.realigned.per_base_coverage.tsv"); mtcn=$(find_exact_one "$dir" "$sample.round2.mtcn.tsv")
  mc=1; require_nonempty "$cram" && mc=0; mci=1; require_nonempty "$crai" && mci=0; mv=1; require_nonempty "$vcf" && mv=0; m2=1; require_nonempty "$cov2" && m2=0; mn=1; require_nonempty "$covn" && mn=0; mm=1; require_nonempty "$mtcn" && mm=0
  gz=0; require_nonempty "$vcf" && gzip -t "$vcf" >/dev/null 2>&1 && gz=1
  qc=0; if require_nonempty "$cram"; then if command -v samtools >/dev/null 2>&1; then samtools quickcheck -v "$cram" >/dev/null 2>&1 && qc=1; elif [[ -e "${cram}.complete" ]]; then qc=1; fi; fi
  slurm=$(awk -F '\t' -v w="$wave" 'NR>1&&$1==w{print $6;exit}' "$WAVE_STATUS_FILE")
  updated=$(awk -F '\t' -v s="$sample" 'NR>1&&$1==s{print $13;exit}' "$STATUS_FILE")
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$sample" "$status" "$wave" "$attempts" "$mc" "$mci" "$mv" "$m2" "$mn" "$mm" "$gz" "$qc" "$slurm" "$error" "$updated" >> "$tmp"
 done < "$STATUS_FILE"
 mv "$tmp" "$REPORT_FILE"
}
with_state_lock write_report
log "Wrote $REPORT_FILE"
