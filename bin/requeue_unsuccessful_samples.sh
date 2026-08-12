#!/usr/bin/env bash
# Conservatively prepare reconciled, unsuccessful samples for a future submission.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/common.sh"

[[ $# == 2 && ( "$2" == --dry-run || "$2" == --apply ) ]] ||
    die "Usage: $0 CONFIG --dry-run|--apply"
mode=$2
load_config "$1"
[[ "$PIPELINE_MODE" == streaming_per_sample ]] ||
    die "Requeue is supported only for PIPELINE_MODE=streaming_per_sample"
[[ -s "$STATUS_FILE" ]] || die "Sample status file missing or empty: $STATUS_FILE"
[[ -s "$WAVE_STATUS_FILE" ]] || die "Wave status file missing or empty: $WAVE_STATUS_FILE"

check_reconciled() {
    local active submitted running
    active=$(active_submission_count)
    (( active == 0 )) || die "Refusing to requeue: $active active streaming submission(s) remain"
    running=$(awk -F '\t' 'NR>1&&$4~/^(PIPELINE_RUNNING|PIPELINE_DEFERRED_RUNNING)$/{n++}END{print n+0}' "$STATUS_FILE")
    (( running == 0 )) || die "Refusing to requeue: $running running sample(s) remain"
    submitted=$(awk -F '\t' 'NR>1&&$4=="PIPELINE_SUBMITTED"{n++}END{print n+0}' "$STATUS_FILE")
    (( submitted == 0 )) || die "Refusing to requeue: $submitted stale PIPELINE_SUBMITTED sample(s) remain; run reconciliation first"
}

build_preview() {
    local output=$1
    awk -F '\t' -v OFS='\t' '
      FNR==NR {
        if (FNR==1) {for(i=1;i<=NF;i++) vh[$i]=i; next}
        if (vh["sample_id"] && vh["overall_complete"] && $vh["overall_complete"]==1)
          complete[$vh["sample_id"]]=1
        next
      }
      NR==FNR+1 { }
      FILENAME != ARGV[1] && FNR==1 {for(i=1;i<=NF;i++) sh[$i]=i; next}
      FILENAME != ARGV[1] && $sh["status"]~/^(PENDING|PIPELINE_DEFERRED_RETRY|PIPELINE_FAILED|PIPELINE_INCOMPLETE_REVIEW)$/ && !complete[$sh["sample_id"]] {
        print $sh["sample_id"],$sh["status"],$sh["pipeline_attempts"],$sh["last_pipeline_error"],"PENDING"
      }
    ' "$VALIDATION_FILE" "$STATUS_FILE" > "$output"
}

preview=$(mktemp "${TMPDIR:-/tmp}/primate-requeue-preview.XXXXXX")
trap 'rm -f "$preview"' EXIT

if [[ "$mode" == --apply ]]; then
    # Hold the normal state lock across all safety checks, snapshot, and replace.
    exec 9>"${MANAGER_ROOT}/state/locks/state.lock"
    flock -x 9
fi
check_reconciled
build_preview "$preview"

printf 'sample_id\tcurrent_status\tpipeline_attempts\tlast_pipeline_error\tproposed_status\n'
cat "$preview"
printf '\nSummary by current status:\n'
awk -F '\t' '{n[$2]++} END{for(s in n) print s"\t"n[s]}' "$preview" | sort
printf 'TOTAL\t%s\n' "$(wc -l < "$preview")"

if [[ "$mode" == --dry-run ]]; then
    log "Dry run only; sample state was not changed"
    exit 0
fi

stamp=$(date -u +%Y%m%dT%H%M%SZ)
backup="${STATUS_FILE}.bak.${stamp}"
cp -p "$STATUS_FILE" "$backup"
tmp="${STATUS_FILE}.tmp.$$"
awk -F '\t' -v OFS='\t' -v selected="$preview" -v ts="$(now_iso)" '
  BEGIN {while((getline line < selected)>0){split(line,a,"\t"); pick[a[1]]=1} close(selected)}
  NR==1 {for(i=1;i<=NF;i++)h[$i]=i; print; next}
  $h["sample_id"] in pick {
    $h["status"]="PENDING"; $h["slurm_job_id"]=""; $h["wave_id"]=""
    $h["last_pipeline_error"]=""; $h["transfer_status"]=""
    $h["notes"]="requeued after completed-submission reconciliation"
    $h["last_update"]=ts
  }
  {print}
' "$STATUS_FILE" > "$tmp"
mv "$tmp" "$STATUS_FILE"
flock -u 9
log "Requeued $(wc -l < "$preview") sample(s); backup: $backup"
