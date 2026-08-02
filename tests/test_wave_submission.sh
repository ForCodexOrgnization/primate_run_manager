#!/usr/bin/env bash
source "$(dirname "$0")/test_helper.sh"; new_env
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
"$REPO/bin/submit_next_batch.sh" "$T/config.sh" >/dev/null
assert test "$(wc -l < "$T/invocations")" -eq 1
manifest=$(find "$T/manager/manifests/pipeline_waves" -name '*.samples.tsv')
assert test "$(wc -l < "$manifest")" -eq 2
assert grep -q $'^5\t1\t.*/wave_' "$T/invocations"
assert grep -q $'\t123456\t' "$T/manager/state/wave_status.tsv"
assert test "$(awk -F '\t' '$4=="WAVE_SUBMITTED"{n++}END{print n+0}' "$T/manager/state/sample_status.tsv")" -eq 2
# An active wave prevents duplicate selection.
"$REPO/bin/submit_next_batch.sh" "$T/config.sh" >/dev/null
assert test "$(wc -l < "$T/invocations")" -eq 1
# Dry run generates a manifest without invoking launcher.
sed -i 's/DRY_RUN=0/DRY_RUN=1/;s/MAX_ACTIVE_PIPELINE_WAVES=1/MAX_ACTIVE_PIPELINE_WAVES=2/' "$T/config.sh"
"$REPO/bin/submit_next_batch.sh" "$T/config.sh" >/dev/null
assert test "$(wc -l < "$T/invocations")" -eq 1
