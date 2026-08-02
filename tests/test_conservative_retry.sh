#!/usr/bin/env bash
source "$(dirname "$0")/test_helper.sh"; new_env
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
mkdir -p "$T/results/s1/alignment"; printf partial > "$T/results/s1/alignment/s1.cram"
"$REPO/bin/import_existing_results.sh" "$T/config.sh" >/dev/null
assert awk -F '\t' '$1=="s1"&&$4=="PIPELINE_INCOMPLETE_REVIEW"{ok=1}END{exit !ok}' "$T/manager/state/sample_status.tsv"
assert test -e "$T/results/s1/alignment/s1.cram"
# Review samples are excluded; only ordinary pending samples enter the first wave.
"$REPO/bin/submit_next_batch.sh" "$T/config.sh" >/dev/null
manifest=$(find "$T/manager/manifests/pipeline_waves" -name '*.samples.tsv' | head -n1)
assert sh -c "! grep -q '^s1' '$manifest'"
# Close the test wave, approve the historical sample, and verify approval preserves output.
awk -F '\t' -v OFS='\t' 'NR==1{print;next}{$9="COMPLETE";print}' "$T/manager/state/wave_status.tsv" > "$T/x"; mv "$T/x" "$T/manager/state/wave_status.tsv"
printf 's1\n' > "$T/approve.txt"; "$REPO/bin/approve_retry_samples.sh" "$T/config.sh" "$T/approve.txt" >/dev/null
assert test -e "$T/results/s1/alignment/s1.cram"
assert awk -F '\t' '$1=="s1"&&$4=="PIPELINE_RETRY_READY"{ok=1}END{exit !ok}' "$T/manager/state/sample_status.tsv"
"$REPO/bin/report_incomplete_samples.sh" "$T/config.sh" >/dev/null
assert grep -q $'^sample_id\tstatus\twave_id\tpipeline_attempts\tmissing_cram' "$T/manager/state/pipeline_incomplete_report.tsv"
assert grep -q $'^s1\tPIPELINE_RETRY_READY' "$T/manager/state/pipeline_incomplete_report.tsv"
"$REPO/bin/submit_next_batch.sh" "$T/config.sh" >/dev/null
assert awk -F '\t' '$1=="s1"&&$4=="PIPELINE_RETRY_RUNNING"&&$7==1{ok=1}END{exit !ok}' "$T/manager/state/sample_status.tsv"
