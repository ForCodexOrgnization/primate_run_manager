#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"
ensure_state_files

module load "$SAMTOOLS_MODULE"

check_sample() {
    local sample="$1" dir="${LOCAL_RESULTS}/${sample}" missing=() cram crai marker vcf cov2 cov_numt mtcn
    [[ -d "$dir" ]] || return 2
    for d in alignment round_1 round_1_variant_calling_decoy round_2 round_2_variant_calling_original_coords; do
        [[ -d "$dir/$d" ]] || missing+=("folder:$d")
    done

    cram="$dir/alignment/${sample}.cram"
    if [[ -s "${cram}.crai" ]]; then crai="${cram}.crai"; else crai="$dir/alignment/${sample}.crai"; fi
    marker="${cram}.complete"
    if [[ -s "$cram" ]]; then
        if [[ ! -e "$marker" ]]; then
            if samtools quickcheck -v "$cram" >/dev/null 2>&1; then
                touch "$marker"
            else
                missing+=("invalid_cram")
            fi
        fi
    else
        missing+=("file:${sample}.cram")
    fi
    [[ -s "$crai" ]] || missing+=("file:${sample}.cram.crai")

    vcf=$(find_exact_one "$dir" "${sample}.round2.original_coords.clean.final.split.vcf.gz")
    cov2=$(find_exact_one "$dir" "${sample}.round2.original_coords.per_base_coverage.tsv")
    cov_numt=$(find_exact_one "$dir" "${sample}.numt_decoy.clean.realigned.per_base_coverage.tsv")
    mtcn=$(find_exact_one "$dir" "${sample}.round2.mtcn.tsv")

    require_nonempty "$vcf" || missing+=("file:round2_final_vcf")
    require_nonempty "$cov2" || missing+=("file:round2_coverage")
    require_nonempty "$cov_numt" || missing+=("file:numt_decoy_coverage")
    require_nonempty "$mtcn" || missing+=("file:round2_mtcn")
    if require_nonempty "$vcf"; then gzip -t "$vcf" >/dev/null 2>&1 || missing+=("invalid_gzip:round2_final_vcf"); fi

    if ((${#missing[@]}==0)); then
        printf 'COMPLETE\tNONE\n'
        return 0
    fi
    printf 'INCOMPLETE\t%s\n' "$(IFS=';'; echo "${missing[*]}")"
    return 1
}

while IFS=$'\t' read -r sample species hpc status _; do
    [[ "$sample" == "sample_id" ]] && continue
    case "$status" in
        LOCAL_FINAL_RETAINED|TRANSFERRING|TRANSFERRED_FULL) continue ;;
    esac
    result=$(check_sample "$sample" || true)
    state=${result%%$'\t'*}
    notes=${result#*$'\t'}
    if [[ "$state" == COMPLETE ]]; then
        with_state_lock update_sample_row "$sample" READY_TO_TRANSFER "" "" "" "" "all required outputs validated"
    elif [[ -d "${LOCAL_RESULTS}/${sample}" ]]; then
        with_state_lock update_sample_row "$sample" "$status" "" "" "" "" "$notes"
    fi
done < "$STATUS_FILE"
