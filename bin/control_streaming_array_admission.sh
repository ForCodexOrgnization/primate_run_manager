#!/usr/bin/env bash
# Unified exact-element admission for all active streaming arrays.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"
ensure_state_files
[[ "$PIPELINE_MODE" == streaming_per_sample ]] || exit 0
command -v squeue >/dev/null 2>&1 && command -v scontrol >/dev/null 2>&1 || {
    log "WARNING: streaming admission tools unavailable"
    exit 0
}

ledger="$MANAGER_ROOT/state/streaming_array_holds.tsv"
legacy="$MANAGER_ROOT/state/streaming_array_disk_holds.tsv"
exec 7>"$MANAGER_ROOT/state/locks/streaming_admission.lock"
flock -x 7
[[ -e "$ledger" ]] || printf 'array_job_id\tarray_task_id\thold_reason\theld_at\n' > "$ledger"

# Adopt manager-owned disk holds written by the previous release. Other
# JobHeldUser/JobHeldAdmin elements are never claimed merely because they exist.
if [[ -s "$legacy" ]]; then
    while IFS=$'\t' read -r job task held_at _; do
        [[ "$job" == array_job_id ]] && continue
        awk -F '\t' -v j="$job" -v t="$task" \
            'NR>1 && $1==j && $2==t && $3=="DISK_PRESSURE" { found=1 } END { exit !found }' "$ledger" ||
            printf '%s\t%s\tDISK_PRESSURE\t%s\n' "$job" "$task" "$held_at" >> "$ledger"
    done < "$legacy"
fi

jobs=$(awk -F '\t' 'NR>1 && $9~/^(CREATED|SUBMITTED|RUNNING)$/ && $4~/^[0-9]+$/ { print $4 }' \
    "$WAVE_STATUS_FILE" | sort -u | paste -sd,)
rows=$(mktemp); desired=$(mktemp); previous=$(mktemp)
trap 'rm -f "$rows" "$desired" "$previous"' EXIT
cp "$ledger" "$previous"
if [[ -n "$jobs" ]]; then
    squeue --noheader --array --jobs="$jobs" --format='%F|%K|%T|%r' 2>/dev/null |
        awk -F '|' '$1~/^[0-9]+$/ && $2~/^[0-9]+$/ { print }' > "$rows"
fi

running=$(awk -F '|' '$3~/^(RUNNING|CONFIGURING|COMPLETING)$/ { n++ } END { print n+0 }' "$rows")
free=$((SAMPLE_CHAIN_CONCURRENCY - running)); (( free < 0 )) && free=0
used="${WORK_DISK_USED_PERCENT_OVERRIDE:-$(work_disk_used_percent)}"

declare -A resume_keys=()
shopt -s nullglob
for marker in "$PIPELINE_WORK_ROOT"/.sample_state/*.requeue.tsv; do
    sample=${marker##*/}; sample=${sample%.requeue.tsv}
    marker_values=$(awk -F '\t' '
        NR==1 { for (i=1;i<=NF;i++) h[$i]=i; next }
        $h["reason"]=="TIMEOUT_SIGNAL" && $h["resume_eligible"]==1 {
            print $h["array_job_id"] "|" $h["array_task_id"]
        }' "$marker")
    [[ "$marker_values" =~ ^[0-9]+\|[0-9]+$ ]] || continue
    marker_job=${marker_values%%|*}; marker_task=${marker_values#*|}
    # submission_task_map is authoritative for exact element ownership.
    if awk -F '\t' -v j="$marker_job" -v t="$marker_task" -v sample="$sample"         'NR>1 && $4==j && $5==t && $6=="SAMPLE" && $8==sample { valid=1 } END { exit !valid }'         "$MANAGER_ROOT"/state/submission_task_map/*.tsv 2>/dev/null; then
        resume_keys["$marker_values"]=1
    fi
done
shopt -u nullglob
is_resume() { [[ -n "${resume_keys[$1|$2]:-}" ]]; }

declare -a resumes=() fresh=()
while IFS='|' read -r job task state pending_reason; do
    [[ "$state" == PENDING ]] || continue
    if is_resume "$job" "$task"; then
        resumes+=("$job|$task|$pending_reason")
    else
        fresh+=("$job|$task|$pending_reason")
    fi
done < "$rows"

# Allocate the available budget in resume-first order. Resume priority only
# displaces the corresponding number of fresh tasks; unused slots remain fresh.
admitted=0
resume_admitted=0
timestamp=$(now_iso)
declare -A desired_elements=()
for item in "${resumes[@]}" "${fresh[@]}"; do
    [[ -n "$item" ]] || continue
    IFS='|' read -r job task pending_reason <<< "$item"
    candidate_is_resume=0; is_resume "$job" "$task" && candidate_is_resume=1
    if (( used >= WORK_CRITICAL_PERCENT )); then
        printf '%s\t%s\tDISK_PRESSURE\t%s\n' "$job" "$task" "$timestamp" >> "$desired"; desired_elements["$job|$task"]=1
    elif (( admitted < free )); then
        admitted=$((admitted + 1))
        (( candidate_is_resume == 0 )) || resume_admitted=$((resume_admitted + 1))
    else
        printf '%s\t%s\tGLOBAL_CONCURRENCY\t%s\n' "$job" "$task" "$timestamp" >> "$desired"; desired_elements["$job|$task"]=1
        # This reason documents only fresh work displaced by an admitted resume.
        if (( candidate_is_resume == 0 && resume_admitted > 0 )); then
            printf '%s\t%s\tRESUME_PRIORITY\t%s\n' "$job" "$task" "$timestamp" >> "$desired"
            resume_admitted=$((resume_admitted - 1))
        fi
    fi
done

# Manager-created arrays are submitted --hold and pre-recorded with
# INITIAL_SUBMISSION ownership. Unrelated held elements remain untouched.
declare -A previous_owned=() current_reason=()
while IFS=$'\t' read -r job task _; do
    [[ "$job" == array_job_id ]] || previous_owned["$job|$task"]=1
done < "$previous"
while IFS='|' read -r job task _state reason; do current_reason["$job|$task"]=$reason; done < "$rows"
for key in "${!desired_elements[@]}"; do
    job=${key%%|*}; task=${key#*|}; reason=${current_reason[$key]:-}
    [[ "$reason" == JobHeldUser && -n "${previous_owned[$key]:-}" ]] && continue
    [[ "$reason" == JobHeldUser || "$reason" == JobHeldAdmin ]] && continue
    scontrol hold "${job}_${task}"
done
for key in "${!previous_owned[@]}"; do
    [[ -n "${desired_elements[$key]:-}" ]] && continue
    job=${key%%|*}; task=${key#*|}
    [[ "${current_reason[$key]:-}" == JobHeldUser ]] && scontrol release "${job}_${task}" || true
done

{ printf 'array_job_id\tarray_task_id\thold_reason\theld_at\n'; sort -u "$desired"; } > "$ledger"
{ printf 'array_job_id\tarray_task_id\theld_at\thold_reason\n'; awk -F '\t' '$3=="DISK_PRESSURE" { print $1 FS $2 FS $4 FS "critical work filesystem" }' "$ledger"; } > "$legacy"
pending=$(awk -F '|' '$3=="PENDING" { n++ } END { print n+0 }' "$rows")
held=$(awk -F '|' '$3=="PENDING" && ($4=="JobHeldUser" || $4=="JobHeldAdmin") { n++ } END { print n+0 }' "$rows")
printf 'resume_waiting\trunning\tpending\theld\tupdated_at\n%s\t%s\t%s\t%s\t%s\n' \
    "${#resumes[@]}" "$running" "$pending" "$held" "$(now_iso)" > "$MANAGER_ROOT/state/streaming_admission_status.tsv"
log "Streaming admission: running=$running free=$free admitted=$admitted resume_waiting=${#resumes[@]} disk_used=${used}%"
