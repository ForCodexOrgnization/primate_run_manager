#!/usr/bin/env bash
source "$(dirname "$0")/test_helper.sh"; new_env
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
"$REPO/bin/submit_next_batch.sh" "$T/config.sh" >/dev/null
assert test "$(wc -l < "$T/invocations")" -eq 1
manifest=$(find "$T/manager/manifests/submissions" -name '*.samples.tsv')
# All eligible samples are submitted; PIPELINE_WAVE_SIZE=2 is ignored.
assert test "$(wc -l < "$manifest")" -eq 3
assert grep -q $'^5\t1\t.*/submissions/batch_.*\tNextflow/test$' "$T/invocations"
assert grep -q $'\t123456\t' "$T/manager/state/wave_status.tsv"
assert test "$(awk -F '\t' '$4=="PIPELINE_SUBMITTED"{n++}END{print n+0}' "$T/manager/state/sample_status.tsv")" -eq 3
map=$(find "$T/manager/state/submission_task_map" -name '*.tsv')
assert test "$(awk -F '\t' 'NR>1&&!seen[$5]++{n++}END{print n}' "$map")" -eq 1
assert test "$(awk -F '\t' 'NR==2{print $5}' "$map")" -eq 0
# An active submission prevents a second independently throttled array.
"$REPO/bin/submit_next_batch.sh" "$T/config.sh" >/dev/null
assert test "$(wc -l < "$T/invocations")" -eq 1
