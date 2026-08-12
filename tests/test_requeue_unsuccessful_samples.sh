#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test_helper.sh"
new_env
cat >> "$T/config.sh" <<EOF
PIPELINE_MODE=streaming_per_sample
PIPELINE_WORK_ROOT=$T/work
EOF
mkdir -p "$T/manager/state/locks"
source "$REPO/lib/common.sh"; load_config "$T/config.sh"
state_header > "$STATUS_FILE"
printf 'eligible\tsp\tTEST\tPIPELINE_FAILED\t99\toldwave\t4\told error\t\t/path\tFAILED\t\told\told note\n' >> "$STATUS_FILE"
printf 'complete\tsp\tTEST\tPIPELINE_FAILED\t99\toldwave\t3\tstale\t\t/path\t\t\told\tstale\n' >> "$STATUS_FILE"
printf 'success\tsp\tTEST\tREADY_TO_TRANSFER\t\t\t2\t\t\t/path\tREADY\t\told\tgood\n' >> "$STATUS_FILE"
printf 'pending\tsp\tTEST\tPENDING\t88\toldwave\t1\told\t\t/path\tFAILED\t\told\told\n' >> "$STATUS_FILE"
wave_header > "$WAVE_STATUS_FILE"
validation_header > "$VALIDATION_FILE"
printf 'complete\t1\t1\t1\t1\t1\t1\t1\tnow\tok\n' >> "$VALIDATION_FILE"

before=$(sha256sum "$STATUS_FILE")
out=$("$REPO/bin/requeue_unsuccessful_samples.sh" "$T/config.sh" --dry-run)
[[ "$before" == "$(sha256sum "$STATUS_FILE")" ]]
[[ "$out" == *$'eligible\tPIPELINE_FAILED\t4\told error\tPENDING'* ]]
[[ "$out" == *$'pending\tPENDING\t1\told\tPENDING'* ]]
[[ "$out" != *$'complete\tPIPELINE_FAILED'* && "$out" != *$'success\tREADY_TO_TRANSFER'* ]]

"$REPO/bin/requeue_unsuccessful_samples.sh" "$T/config.sh" --apply >/dev/null
assert awk -F '\t' '$1=="eligible"&&$4=="PENDING"&&$5==""&&$6==""&&$7==4&&$8==""&&$11==""&&$14=="requeued after completed-submission reconciliation"{ok=1}END{exit !ok}' "$STATUS_FILE"
assert awk -F '\t' '$1=="complete"&&$4=="PIPELINE_FAILED"&&$8=="stale"{ok=1}END{exit !ok}' "$STATUS_FILE"
assert awk -F '\t' '$1=="success"&&$4=="READY_TO_TRANSFER"{ok=1}END{exit !ok}' "$STATUS_FILE"
compgen -G "$STATUS_FILE.bak.*" >/dev/null

# Every reconciliation guard refuses before creating another backup.
backups=$(find "$T/manager/state" -name 'sample_status.tsv.bak.*' | wc -l)
awk -F '\t' -v OFS='\t' 'NR==1{print;next}$1=="eligible"{$4="PIPELINE_SUBMITTED"}{print}' "$STATUS_FILE" > "$T/x"; mv "$T/x" "$STATUS_FILE"
! "$REPO/bin/requeue_unsuccessful_samples.sh" "$T/config.sh" --apply >/dev/null 2>&1
[[ $(find "$T/manager/state" -name 'sample_status.tsv.bak.*' | wc -l) -eq "$backups" ]]
echo 'Requeue unsuccessful sample tests passed.'
