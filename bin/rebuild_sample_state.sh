#!/usr/bin/env bash
# Reconstruct sample state from evidence; never schedules, transfers, or deletes data.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/common.sh"

[[ $# == 2 && ( "$2" == --preview || "$2" == --apply ) ]] ||
    die "Usage: $0 CONFIG --preview|--apply"
mode=$2
load_config "$1"
[[ -s "$ASSIGNED_SAMPLE_LIST" ]] || die "Assigned sample list missing or empty"
awk -F '\t' 'NF!=2||$1==""||$2==""{exit 1}' "$ASSIGNED_SAMPLE_LIST" ||
    die "Sample list must contain exactly two non-empty TAB-separated columns"
[[ -z "$(cut -f1 "$ASSIGNED_SAMPLE_LIST" | sort | uniq -d)" ]] || die "Duplicate sample IDs in assigned list"

stamp=$(date -u +%Y%m%dT%H%M%SZ)
if [[ -n "${REBUILD_OUTPUT_DIR:-}" ]]; then out=$REBUILD_OUTPUT_DIR
elif [[ "$mode" == --apply ]]; then out="${MANAGER_ROOT}/state/rebuild_${stamp}"
else out=$(mktemp -d "${TMPDIR:-/tmp}/primate-state-preview.${stamp}.XXXXXX")
fi
mkdir -p "$out"
tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/primate-state-rebuild.XXXXXX")
trap 'rm -rf "$tmpdir"' EXIT

# Applying holds the manager's normal state lock for the complete snapshot and replace.
if [[ "$mode" == --apply ]]; then
    mkdir -p "${MANAGER_ROOT}/state/locks"
    exec 9>"${MANAGER_ROOT}/state/locks/state.lock"; flock -x 9
fi

old="$tmpdir/old.tsv"
if [[ -s "$STATUS_FILE" ]]; then cp "$STATUS_FILE" "$old"; else state_header > "$old"; fi
transfer="$tmpdir/transfer.tsv"
[[ -s "$TRANSFER_TASK_FILE" ]] && cp "$TRANSFER_TASK_FILE" "$transfer" || transfer_header > "$transfer"

# Build every historical sample -> array task association.  Query Slurm once for all tasks.
maps="$tmpdir/maps.tsv"; : > "$maps"
for f in "${MANAGER_ROOT}"/state/submission_task_map/*.tsv; do
    [[ -f "$f" ]] || continue
    awk -F '\t' 'NR>1&&$6=="SAMPLE"&&$8!=""&&$4!=""&&$5!=""{print $8"\t"$4"\t"$5}' "$f" >> "$maps"
done
for f in "${MANAGER_ROOT}"/state/array_sample_map/*.tsv; do
    [[ -f "$f" ]] || continue
    awk -F '\t' 'NR>1&&$4!=""&&$2!=""&&$3!=""{print $4"\t"$2"\t"$3}' "$f" >> "$maps"
done
sort -u "$maps" -o "$maps"
slurm="$tmpdir/slurm.tsv"; : > "$slurm"
if command -v sacct >/dev/null 2>&1 && [[ -s "$maps" ]]; then
    jobs=$(awk '{printf "%s%s_%s",(NR==1?"":","),$2,$3}' "$maps")
    sacct -n -j "$jobs" --format=JobIDRaw,State --parsable2 2>/dev/null |
      awk -F '|' 'NF>=2{sub(/ .*/,"",$2);sub(/\+$/, "", $2); if(!seen[$1]++)print $1"\t"$2}' > "$slurm" || true
fi

audit="$out/audit.tsv"
printf 'sample_id\tspecies\told_status\tderived_status\treason\tlocal_dir_exists\tlocal_validation_complete\treceipt_exists\tretained_outputs_complete\ttransfer_task_state\tslurm_job_id\tslurm_task_id\tslurm_state\tpreviously_attempted\n' > "$audit"
new="$tmpdir/sample_status.tsv"; state_header > "$new"
validation="$tmpdir/output_validation.tsv"; validation_header > "$validation"
for status in PENDING PIPELINE_SUBMITTED PIPELINE_RUNNING PIPELINE_DEFERRED_RETRY PIPELINE_INCOMPLETE_REVIEW READY_TO_TRANSFER TRANSFERRING TRANSFERRED_FULL LOCAL_FINAL_RETAINED REVIEW; do : > "$out/${status}.samples"; done

validate_local() {
    local s=$1 d="$LOCAL_RESULTS/$1" cram crai vcf c2 cn mt okc=0 okci=0 okv=0 ok2=0 okn=0 okm=0
    cram="$d/alignment/$s.cram"; crai="$cram.crai"; [[ -s "$crai" ]] || crai="$d/alignment/$s.crai"
    vcf=$(find_exact_one "$d" "$s.round2.original_coords.clean.final.split.vcf.gz")
    c2=$(find_exact_one "$d" "$s.round2.original_coords.per_base_coverage.tsv")
    cn=$(find_exact_one "$d" "$s.numt_decoy.clean.realigned.per_base_coverage.tsv")
    mt=$(find_exact_one "$d" "$s.round2.mtcn.tsv")
    if [[ -s "$cram" ]]; then
      if command -v samtools >/dev/null 2>&1; then timeout "${SAMTOOLS_QUICKCHECK_TIMEOUT_SECONDS:-600}" samtools quickcheck -v "$cram" >/dev/null 2>&1 && okc=1 || true
      elif [[ -f "$cram.complete" ]]; then okc=1; fi
    fi
    [[ -s "$crai" ]] && okci=1
    if require_nonempty "$vcf" && gzip -t "$vcf" >/dev/null 2>&1; then okv=1; fi
    require_nonempty "$c2" && ok2=1 || true; require_nonempty "$cn" && okn=1 || true; require_nonempty "$mt" && okm=1 || true
    local overall=0; ((okc&&okci&&okv&&ok2&&okn&&okm)) && overall=1
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\treconstructed\t-1\t-1\t-1\t-1\t-1\t-1\t-1\t-1\t-1\t-1\t-1\t-1\n' "$s" "$okc" "$okci" "$okv" "$ok2" "$okn" "$okm" "$overall" "$(now_iso)" >> "$validation"
    printf '%s' "$overall"
}

while IFS=$'\t' read -r sample species; do
    safe_sample_id "$sample" || die "unsafe sample ID: $sample"
    row=$(awk -F '\t' -v s="$sample" 'NR>1&&$1==s{r=$0}END{print r}' "$old")
    old_status=$(cut -f4 <<<"$row"); [[ -n "$old_status" ]] || old_status=ABSENT
    attempts=$(cut -f7 <<<"$row"); [[ "$attempts" =~ ^[0-9]+$ ]] || attempts=0
    old_wave=$(cut -f6 <<<"$row"); old_workspace=$(cut -f10 <<<"$row")
    dir=0; [[ -d "$LOCAL_RESULTS/$sample" ]] && dir=1
    complete=0; ((dir)) && complete=$(validate_local "$sample") || printf '%s\t0\t0\t0\t0\t0\t0\t0\t%s\tno_local_directory\t-1\t-1\t-1\t-1\t-1\t-1\t-1\t-1\t-1\t-1\t-1\t-1\n' "$sample" "$(now_iso)" >> "$validation"
    receipt=0; [[ -f "$ANALYSIS_ROOT/receipts/$sample.transferred.tsv" ]] && receipt=1
    retained=1
    for p in vcf/"$sample.round2.original_coords.clean.final.split.vcf.gz" round2_coverage/"$sample.round2.original_coords.per_base_coverage.tsv" numt_decoy_coverage/"$sample.numt_decoy.clean.realigned.per_base_coverage.tsv" mtcn/"$sample.round2.mtcn.tsv"; do [[ -s "$ANALYSIS_ROOT/$p" ]] || retained=0; done
    tstate=; taskid=; samplefile=
    while IFS=$'\t' read -r _ tid ts sf _; do [[ -n "$tid" ]] || continue; if [[ -s "$sf" ]] && awk -F '\t' -v s="$sample" '$1==s{f=1}END{exit !f}' "$sf"; then tstate=$ts; taskid=$tid; fi; done < <(awk -F '\t' 'NR>1{print $1"\t"$2"\t"$3"\t"$4"\t"$5}' "$transfer")
    sj= st= ss=; while IFS=$'\t' read -r ms mj mt; do [[ "$ms" == "$sample" ]] || continue; state=$(awk -F '\t' -v id="${mj}_${mt}" '$1==id{s=$2}END{print s}' "$slurm"); case "$state" in PENDING|RUNNING|CONFIGURING|COMPLETING|REQUEUED|REQUEUE_FED|REQUEUE_HOLD|RESIZING|SUSPENDED|SIGNALING|STAGE_OUT) sj=$mj; st=$mt; ss=$state;; esac; done < "$maps"
    attempted=0; [[ $(awk -F '\t' -v s="$sample" '$1==s{n++}END{print n+0}' "$maps") -gt 0 || $attempts -gt 0 || $dir -eq 1 ]] && attempted=1
    transfer_status= cleanup_status= notes= slurm_job= wave= globus= workspace=
    if [[ -n "$ss" ]]; then
      slurm_job=$sj; wave=$old_wave; workspace=$old_workspace; [[ "$ss" =~ ^(PENDING|CONFIGURING)$ ]] && derived=PIPELINE_SUBMITTED || derived=PIPELINE_RUNNING; reason="active Slurm array task ${sj}_${st} ($ss)"
    elif [[ "$tstate" == ACTIVE ]]; then derived=TRANSFERRING; reason="active transfer ledger task $taskid"; globus=$taskid; workspace=$old_workspace; transfer_status=ACTIVE
    elif ((receipt && retained)); then derived=LOCAL_FINAL_RETAINED; reason="transfer receipt and all retained core outputs exist"; transfer_status=SUCCEEDED; cleanup_status=COMPLETE
    elif [[ "$tstate" == SUCCEEDED && $dir -eq 1 && $receipt -eq 0 ]]; then derived=TRANSFERRED_FULL; reason="successful transfer ledger, local directory remains, no cleanup receipt"; globus=$taskid; transfer_status=SUCCEEDED
    elif ((complete)); then derived=READY_TO_TRANSFER; reason="formal local output validation complete"; transfer_status=READY
    elif ((receipt)) || [[ -n "$tstate" ]]; then derived=REVIEW; reason="transfer or receipt evidence exists but does not prove a higher-precedence complete state"; globus=$taskid; transfer_status=$tstate
    elif ((attempted)); then
      if (( attempts > MAX_DEFERRED_RETRIES )); then derived=PIPELINE_INCOMPLETE_REVIEW; reason="previous attempt evidence; retry limit exhausted"
      else derived=PIPELINE_DEFERRED_RETRY; reason="previous attempt evidence; formal outputs incomplete"; fi
    else derived=PENDING; reason="no active, transfer, receipt, local-result, or previous-attempt evidence"; fi
    printf '%s\n' "$sample" >> "$out/${derived}.samples"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$sample" "$species" "$old_status" "$derived" "$reason" "$dir" "$complete" "$receipt" "$retained" "${tstate:-NONE}" "$sj" "$st" "$ss" "$attempted" >> "$audit"
    notes="reconstructed: $reason"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t\t%s\t%s\t%s\t%s\t%s\t%s\n' "$sample" "$species" "$HPC_NAME" "$derived" "$slurm_job" "$wave" "$attempts" "$globus" "$workspace" "$transfer_status" "$cleanup_status" "$(now_iso)" "$notes" >> "$new"
done < "$ASSIGNED_SAMPLE_LIST"

if [[ "$mode" == --apply ]]; then
    mkdir -p "$(dirname "$STATUS_FILE")"
    if [[ -e "$STATUS_FILE" ]]; then cp -p "$STATUS_FILE" "${STATUS_FILE}.bak.${stamp}"; else state_header > "${STATUS_FILE}.bak.${stamp}"; fi
    if [[ -e "$VALIDATION_FILE" ]]; then cp -p "$VALIDATION_FILE" "${VALIDATION_FILE}.bak.${stamp}"; else validation_header > "${VALIDATION_FILE}.bak.${stamp}"; fi
    cp "$new" "${STATUS_FILE}.tmp.$$"; mv "${STATUS_FILE}.tmp.$$" "$STATUS_FILE"
    cp "$validation" "${VALIDATION_FILE}.tmp.$$"; mv "${VALIDATION_FILE}.tmp.$$" "$VALIDATION_FILE"
    flock -u 9
fi
log "Reconstruction $mode complete; audit and sample lists: $out"
