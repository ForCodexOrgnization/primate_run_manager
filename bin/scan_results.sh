#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
interactive_full_scan_override_set=${ALLOW_INTERACTIVE_FULL_SCAN+x}
interactive_full_scan_override=${ALLOW_INTERACTIVE_FULL_SCAN:-0}
load_config "${1:-}"; ensure_state_files
[[ "$interactive_full_scan_override_set" == x ]] && ALLOW_INTERACTIVE_FULL_SCAN="$interactive_full_scan_override"
: "${SCAN_RESULTS_SCOPE:=full}" "${REQUIRE_SLURM_FOR_FULL_SCAN:=1}" "${ALLOW_INTERACTIVE_FULL_SCAN:=0}"
if [[ "$SCAN_RESULTS_SCOPE" == full && "$REQUIRE_SLURM_FOR_FULL_SCAN" == 1 && -z "${SLURM_JOB_ID:-}" && "$ALLOW_INTERACTIVE_FULL_SCAN" != 1 ]]; then
    cat >&2 <<'EOF'
Full sample validation must run through Slurm.
Use bin/submit_import_existing.sh CONFIG.
EOF
    exit 1
fi
: "${SAMTOOLS_QUICKCHECK_TIMEOUT_SECONDS:=600}"
[[ "$SAMTOOLS_QUICKCHECK_TIMEOUT_SECONDS" =~ ^[0-9]+$ ]] || die "SAMTOOLS_QUICKCHECK_TIMEOUT_SECONDS must be a non-negative integer"
if command -v module >/dev/null 2>&1 && [[ -n "${SAMTOOLS_MODULE:-}" ]]; then module load "$SAMTOOLS_MODULE" >/dev/null 2>&1 || die "Unable to load SAMtools module: $SAMTOOLS_MODULE"; fi

upsert_validation() {
 local row="$1" sample="${row%%$'\t'*}" tmp="${VALIDATION_FILE}.tmp.$$"
 awk -F '\t' -v s="$sample" 'NR==1||$1!=s' "$VALIDATION_FILE" > "$tmp"; printf '%b\n' "$row" >> "$tmp"; mv "$tmp" "$VALIDATION_FILE"
}
file_signature() {
    if [[ -n "${1:-}" && -f "$1" ]]; then
        printf '%s\t%s' "$(stat -c '%s' "$1")" "$(stat -c '%Y' "$1")"
    else
        printf '%s\t%s' -1 -1
    fi
}
promote_validated_sample() {
 local sample="$1" status="$2"
 case "$status" in
   PENDING|WAVE_SUBMITTED|PIPELINE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_COMPLETE|PIPELINE_INCOMPLETE_REVIEW|PIPELINE_RETRY_READY|PIPELINE_RETRY_RUNNING|PIPELINE_DEFERRED_RETRY|PIPELINE_DEFERRED_RUNNING|PIPELINE_DEFERRED_FAILED|PIPELINE_FAILED|TRANSFER_FAILED)
     with_state_lock update_sample_fields "$sample" "status=READY_TO_TRANSFER" "last_pipeline_error=" "transfer_status=READY" "notes=all required outputs validated";;
 esac
}

if [[ "$SCAN_RESULTS_SCOPE" == active ]]; then
 active_status_regex='^(WAVE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_COMPLETE|PIPELINE_RETRY_RUNNING|PIPELINE_DEFERRED_RUNNING)$'
 [[ "${FORCE_SCAN_INCOMPLETE_REVIEW:-0}" == 1 ]] && active_status_regex='^(WAVE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_COMPLETE|PIPELINE_RETRY_RUNNING|PIPELINE_DEFERRED_RUNNING|PIPELINE_INCOMPLETE_REVIEW)$'
 if [[ -n "${SCAN_SAMPLE_LIST:-}" ]]; then total=$(awk 'NF{n++}END{print n+0}' "$SCAN_SAMPLE_LIST")
 else total=$(awk -F '\t' -v r="$active_status_regex" -v markers="${PIPELINE_WORK_ROOT}/.sample_state" 'NR>1&&$4!~/^(TRANSFERRING|TRANSFERRED_FULL|LOCAL_FINAL_RETAINED|OUT_OF_SCOPE)$/&&($4~r||system("test -s \"" markers "/" $1 ".complete.tsv\"")==0){n++}END{print n+0}' "$STATUS_FILE"); fi
 (( total > 0 )) || { log "No active pipeline samples to validate"; exit 0; }
else
 total=$(awk -F '\t' 'NR>1 && $4!~/^(TRANSFERRING|TRANSFERRED_FULL|LOCAL_FINAL_RETAINED)$/{n++} END{print n+0}' "$STATUS_FILE")
fi
current=0
while IFS=$'\t' read -r sample species hpc status job wave attempts error task workspace transfer cleanup updated oldnotes; do
 [[ "$sample" == sample_id ]] && continue
 if [[ "$SCAN_RESULTS_SCOPE" == active ]]; then
   [[ "$status" =~ ^(TRANSFERRING|TRANSFERRED_FULL|LOCAL_FINAL_RETAINED|OUT_OF_SCOPE)$ ]] && continue
   if [[ -n "${SCAN_SAMPLE_LIST:-}" ]]; then grep -Fqx -- "$sample" "$SCAN_SAMPLE_LIST" || continue
   else [[ "$status" =~ $active_status_regex || -s "${PIPELINE_WORK_ROOT}/.sample_state/${sample}.complete.tsv" ]] || continue; fi
 else
   case "$status" in TRANSFERRING|TRANSFERRED_FULL|LOCAL_FINAL_RETAINED) continue;; esac
 fi
 current=$((current + 1)); log "Validating sample $sample ($current/$total)"
 dir="${LOCAL_RESULTS}/${sample}"; cram="$dir/alignment/${sample}.cram"; crai="${cram}.crai"; [[ -s "$crai" ]] || crai="$dir/alignment/${sample}.crai"
 vcf=$(find_exact_one "$dir" "${sample}.round2.original_coords.clean.final.split.vcf.gz"); cov2=$(find_exact_one "$dir" "${sample}.round2.original_coords.per_base_coverage.tsv"); covn=$(find_exact_one "$dir" "${sample}.numt_decoy.clean.realigned.per_base_coverage.tsv"); mtcn=$(find_exact_one "$dir" "${sample}.round2.mtcn.tsv")
 signature="$(file_signature "$cram")"$'\t'"$(file_signature "$crai")"$'\t'"$(file_signature "$vcf")"$'\t'"$(file_signature "$cov2")"$'\t'"$(file_signature "$covn")"$'\t'"$(file_signature "$mtcn")"
 cached=$(awk -F '\t' -v s="$sample" '$1==s&&$8==1{for(i=11;i<=22;i++)printf "%s%s",(i==11?"":"\t"),$i; exit}' "$VALIDATION_FILE")
 if [[ -n "$cached" && "$cached" == "$signature" ]]; then
   log "Using unchanged successful validation for $sample"
   log "COMPLETE"
   promote_validated_sample "$sample" "$status"
   continue
 fi
 cram_ok=0; crai_ok=0; vcf_ok=0; cov2_ok=0; covn_ok=0; mtcn_ok=0; notes=()
 if [[ -s "$cram" ]]; then
   if command -v samtools >/dev/null 2>&1; then
     quickcheck_rc=0
     if (( SAMTOOLS_QUICKCHECK_TIMEOUT_SECONDS > 0 )); then
       timeout "$SAMTOOLS_QUICKCHECK_TIMEOUT_SECONDS" samtools quickcheck -v "$cram" >/dev/null 2>&1 || quickcheck_rc=$?
     else
       samtools quickcheck -v "$cram" >/dev/null 2>&1 || quickcheck_rc=$?
     fi
     if (( quickcheck_rc == 0 )); then cram_ok=1; touch "${cram}.complete"
     elif (( quickcheck_rc == 124 )); then notes+=(cram_quickcheck_timeout)
     elif (( quickcheck_rc == 137 )); then
       transient_file="${MANAGER_ROOT}/state/validation_transient_errors.tsv"
       [[ -e "$transient_file" ]] || printf 'sample_id\tscan_time\terror\n' > "$transient_file"
       printf '%s\t%s\tcram_quickcheck_killed\n' "$sample" "$(now_iso)" >> "$transient_file"
       log "TRANSIENT: $sample cram_quickcheck_killed; validation cache and sample state unchanged"
       continue
     else notes+=(invalid_cram); fi
   elif [[ -f "${cram}.complete" ]]; then cram_ok=1
   else notes+=(cram_unverified_no_samtools_or_marker); fi
 else notes+=(missing_cram); fi
 [[ -s "$crai" ]] && crai_ok=1 || notes+=(missing_crai)
 if require_nonempty "$vcf" && gzip -t "$vcf" >/dev/null 2>&1; then vcf_ok=1; else notes+=(missing_or_invalid_vcf); fi
 require_nonempty "$cov2" && cov2_ok=1 || notes+=(missing_round2_coverage)
 require_nonempty "$covn" && covn_ok=1 || notes+=(missing_numt_coverage)
 require_nonempty "$mtcn" && mtcn_ok=1 || notes+=(missing_mtcn)
 overall=0; ((cram_ok&&crai_ok&&vcf_ok&&cov2_ok&&covn_ok&&mtcn_ok)) && overall=1
 note=$(IFS=';'; echo "${notes[*]:-validated}"); row="$sample\t$cram_ok\t$crai_ok\t$vcf_ok\t$cov2_ok\t$covn_ok\t$mtcn_ok\t$overall\t$(now_iso)\t$note\t$signature"; with_state_lock upsert_validation "$row"
 if ((overall)); then
   log "COMPLETE"
   promote_validated_sample "$sample" "$status"
 else
   log "INCOMPLETE: $note"
   if [[ "$status" =~ ^(WAVE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_RETRY_RUNNING)$ ]] && ! wave_is_active "$wave"; then with_state_lock update_sample_fields "$sample" "notes=$note"
   elif [[ "$status" =~ ^(PENDING|PIPELINE_INCOMPLETE_REVIEW|PIPELINE_RETRY_READY|PIPELINE_FAILED)$ && -d "$dir" ]]; then with_state_lock update_sample_fields "$sample" "notes=$note"; fi
 fi
done < "$STATUS_FILE"
