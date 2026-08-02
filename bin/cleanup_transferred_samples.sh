#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"
ensure_state_files
[[ "$ENABLE_LOCAL_CLEANUP" == 1 ]] || { log "ENABLE_LOCAL_CLEANUP=0; no local deletion"; exit 0; }

mkdir -p "$ANALYSIS_ROOT"/{vcf,round2_coverage,numt_decoy_coverage,mtcn,receipts}

cleanup_one() {
    local sample="$1" dir="${LOCAL_RESULTS}/${sample}" vcf cov2 covn mtcn tbi="" receipt
    [[ -d "$dir" ]] || { log "$sample local directory already absent"; return 0; }
    vcf=$(find_exact_one "$dir" "${sample}.round2.original_coords.clean.final.split.vcf.gz")
    cov2=$(find_exact_one "$dir" "${sample}.round2.original_coords.per_base_coverage.tsv")
    covn=$(find_exact_one "$dir" "${sample}.numt_decoy.clean.realigned.per_base_coverage.tsv")
    mtcn=$(find_exact_one "$dir" "${sample}.round2.mtcn.tsv")
    require_nonempty "$vcf" || { log "$sample missing final VCF; not cleaning"; return 1; }
    require_nonempty "$cov2" || { log "$sample missing round2 coverage; not cleaning"; return 1; }
    require_nonempty "$covn" || { log "$sample missing NUMT coverage; not cleaning"; return 1; }
    require_nonempty "$mtcn" || { log "$sample missing mtCN; not cleaning"; return 1; }
    gzip -t "$vcf"
    [[ -s "${vcf}.tbi" ]] && tbi="${vcf}.tbi"

    cp -a "$vcf" "$ANALYSIS_ROOT/vcf/"
    [[ -n "$tbi" ]] && cp -a "$tbi" "$ANALYSIS_ROOT/vcf/"
    cp -a "$cov2" "$ANALYSIS_ROOT/round2_coverage/"
    cp -a "$covn" "$ANALYSIS_ROOT/numt_decoy_coverage/"
    cp -a "$mtcn" "$ANALYSIS_ROOT/mtcn/"

    cmp -s "$vcf" "$ANALYSIS_ROOT/vcf/$(basename "$vcf")" || return 1
    cmp -s "$cov2" "$ANALYSIS_ROOT/round2_coverage/$(basename "$cov2")" || return 1
    cmp -s "$covn" "$ANALYSIS_ROOT/numt_decoy_coverage/$(basename "$covn")" || return 1
    cmp -s "$mtcn" "$ANALYSIS_ROOT/mtcn/$(basename "$mtcn")" || return 1

    receipt="$ANALYSIS_ROOT/receipts/${sample}.transferred.tsv"
    printf 'sample_id\tglobus_task_id\tdestination\tcleanup_time\n%s\t%s\t%s\t%s\n' \
        "$sample" "$(awk -F '\t' -v s="$sample" 'NR>1&&$1==s{print $7}' "$STATUS_FILE")" \
        "${DEST_ROOT%/}/${sample}/" "$(now_iso)" > "$receipt"

    rm -rf -- "$dir"
    with_state_lock update_sample_row "$sample" LOCAL_FINAL_RETAINED "" "" "" "${DEST_ROOT%/}/${sample}/" "full Workspace copy retained; local final files retained under ANALYSIS_ROOT"
    log "Cleaned local intermediates for $sample"
}

while IFS= read -r sample; do cleanup_one "$sample" || true; done < <(get_samples_by_status '^TRANSFERRED_FULL$')
