#!/usr/bin/env bash
# Unified exact-element admission for all active streaming arrays.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"; ensure_state_files
[[ "$PIPELINE_MODE" == streaming_per_sample ]] || exit 0
command -v squeue >/dev/null 2>&1 && command -v scontrol >/dev/null 2>&1 || { log "WARNING: streaming admission tools unavailable"; exit 0; }
ledger="$MANAGER_ROOT/state/streaming_array_holds.tsv"; legacy="$MANAGER_ROOT/state/streaming_array_disk_holds.tsv"
exec 7>"$MANAGER_ROOT/state/locks/streaming_admission.lock"; flock -x 7
[[ -e "$ledger" ]] || printf 'array_job_id\tarray_task_id\thold_reason\theld_at\n' > "$ledger"
if [[ -s "$legacy" ]]; then while IFS=$'\t' read -r j t at _; do
 [[ "$j" == array_job_id ]] && continue
 awk -F'\t' -v j="$j" -v t="$t" 'NR>1&&$1==j&&$2==t&&$3=="DISK_PRESSURE"{x=1}END{exit !x}' "$ledger" || printf '%s\t%s\tDISK_PRESSURE\t%s\n' "$j" "$t" "$at" >> "$ledger"
done < "$legacy"; fi
jobs=$(awk -F'\t' 'NR>1&&$9~/^(CREATED|SUBMITTED|RUNNING)$/&&$4~/^[0-9]+$/{print $4}' "$WAVE_STATUS_FILE"|sort -u|paste -sd,)
rows=$(mktemp); wanted=$(mktemp); old=$(mktemp); trap 'rm -f "$rows" "$wanted" "$old"' EXIT; cp "$ledger" "$old"
[[ -z "$jobs" ]] || squeue --noheader --array --jobs="$jobs" --format='%F|%K|%T|%r' 2>/dev/null | awk -F'|' '$1~/^[0-9]+$/&&$2~/^[0-9]+$/{print}' > "$rows"
running=$(awk -F'|' '$3~/^(RUNNING|CONFIGURING|COMPLETING)$/{n++}END{print n+0}' "$rows")
free=$((SAMPLE_CHAIN_CONCURRENCY-running)); ((free<0))&&free=0
used="${WORK_DISK_USED_PERCENT_OVERRIDE:-$(work_disk_used_percent)}"
is_resume(){ local j=$1 t=$2 row sample marker
 row=$(awk -F'\t' -v j="$j" -v t="$t" 'NR>1&&$4==j&&$5==t&&$6=="SAMPLE"{x=$0}END{print x}' "$MANAGER_ROOT"/state/submission_task_map/*.tsv 2>/dev/null||true); [[ -n "$row" ]]||return 1
 sample=$(cut -f8<<<"$row"); marker="$PIPELINE_WORK_ROOT/.sample_state/$sample.requeue.tsv"; [[ -s "$marker" ]]||return 1
 awk -F'\t' -v j="$j" -v t="$t" 'NR==1{for(i=1;i<=NF;i++)h[$i]=i;next}$h["reason"]=="TIMEOUT_SIGNAL"&&$h["resume_eligible"]==1&&$h["array_job_id"]==j&&$h["array_task_id"]==t{ok=1}END{exit !ok}' "$marker"
}
declare -a resumes=() fresh=()
while IFS='|' read -r j t st why; do [[ "$st" == PENDING ]]||continue; if is_resume "$j" "$t";then resumes+=("$j|$t|$why");else fresh+=("$j|$t|$why");fi; done < "$rows"
admitted=0
for item in "${resumes[@]}" "${fresh[@]}"; do [[ -n "$item" ]]||continue; IFS='|' read -r j t why<<<"$item"; reasons=()
 ((used>=WORK_CRITICAL_PERCENT))&&reasons+=(DISK_PRESSURE)
 if ((admitted<free&&used<WORK_CRITICAL_PERCENT));then admitted=$((admitted+1));else reasons+=(GLOBAL_CONCURRENCY);fi
 if ((${#resumes[@]}>0))&&! is_resume "$j" "$t";then reasons+=(RESUME_PRIORITY);fi
 for reason in "${reasons[@]}";do printf '%s\t%s\t%s\t%s\n' "$j" "$t" "$reason" "$(now_iso)" >> "$wanted";done
done
awk -F'\t' '{k=$1"\t"$2;if(!seen[k]++)print $1"\t"$2}' "$wanted" | while IFS=$'\t' read -r j t;do
 why=$(awk -F'|' -v j="$j" -v t="$t" '$1==j&&$2==t{print $4;exit}' "$rows"); owned=$(awk -F'\t' -v j="$j" -v t="$t" 'NR>1&&$1==j&&$2==t{print 1;exit}' "$old")
 [[ "$why" == JobHeldUser && -n "$owned" ]]&&continue; [[ "$why" == JobHeldUser || "$why" == JobHeldAdmin ]]&&continue; scontrol hold "${j}_${t}"
done
awk -F'\t' 'NR>1{print $1"\t"$2}' "$old"|sort -u|while IFS=$'\t' read -r j t;do
 awk -F'\t' -v j="$j" -v t="$t" '$1==j&&$2==t{x=1}END{exit !x}' "$wanted"&&continue
 why=$(awk -F'|' -v j="$j" -v t="$t" '$1==j&&$2==t{print $4;exit}' "$rows"); [[ "$why" == JobHeldUser ]]&&scontrol release "${j}_${t}"||true
done
{ printf 'array_job_id\tarray_task_id\thold_reason\theld_at\n'; sort -u "$wanted"; } > "$ledger"
{ printf 'array_job_id\tarray_task_id\theld_at\thold_reason\n'; awk -F'\t' '$3=="DISK_PRESSURE"{print $1"\t"$2"\t"$4"\tcritical work filesystem"}' "$ledger"; } > "$legacy"
printf 'resume_waiting\trunning\tpending\theld\tupdated_at\n%s\t%s\t%s\t%s\t%s\n' "${#resumes[@]}" "$running" "$(awk -F'|' '$3=="PENDING"{n++}END{print n+0}' "$rows")" "$(awk -F'|' '$3=="PENDING"&&($4=="JobHeldUser"||$4=="JobHeldAdmin"){n++}END{print n+0}' "$rows")" "$(now_iso)" > "$MANAGER_ROOT/state/streaming_admission_status.tsv"
log "Streaming admission: running=$running free=$free admitted=$admitted resume_waiting=${#resumes[@]} disk_used=${used}%"
