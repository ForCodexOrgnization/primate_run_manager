#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test_helper.sh"
new_env
mkdir -p "$T"/refs/{global,mt,nuclear}
mkdir -p "$T/numt_detection"
touch "$T/preprocessing.nf" "$T/numt_detection/numt_end2end.nf" \
  "$T/primate_pipeline_numt_decoy_round1.nf" \
  "$T/primate_pipeline_round2_consensus_NUMT.nf"
printf '// test workflow\n' > "$T/preprocessing.nf"
printf '// test workflow\n' > "$T/numt_detection/numt_end2end.nf"
printf '// test workflow\n' > "$T/primate_pipeline_numt_decoy_round1.nf"
printf '// test workflow\n' > "$T/primate_pipeline_round2_consensus_NUMT.nf"
cat >> "$T/config.sh" <<EOF
PIPELINE_MODE=streaming_per_sample
GLOBAL_REF_DIR=$T/refs/global
REF_DIR=$T/refs/mt
NUCLEAR_ONLY_REF_DIR=$T/refs/nuclear
SAMTOOLS_MODULE=SAMtools/test
STREAM_SMOKE_TEST=1
SAMPLE_CHAIN_CONCURRENCY=1
EOF
cat > "$T/launch_pipeline_streaming_per_sample.sh" <<'EOF'
#!/usr/bin/env bash
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
  "$SAMTOOLS_MODULE" "$STREAM_SMOKE_TEST" "$MAX_CONCURRENT" "$FULL_SAMPLE_LIST" \
  "$PIPELINE_REPO_DIR" "$PRE_OUTPUT_DIR" "$ROUND_OUTPUT_DIR" "$ROUND1_OUTDIR" \
  "$NF_BASE_WORK_DIR" "$NF_CONFIG_FILE" "$NEXTFLOW_MODULE" "$GLOBAL_REF_DIR:$REF_DIR:$NUCLEAR_ONLY_REF_DIR" \
  > "$TEST_ROOT/stream_invocation"
echo 'Submitted batch job 123456'
EOF
chmod +x "$T/launch_pipeline_streaming_per_sample.sh"
sed -i "s|^PIPELINE_LAUNCHER=.*|PIPELINE_LAUNCHER=$T/launch_pipeline_streaming_per_sample.sh|" "$T/config.sh"

"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
submit_output=$("$REPO/bin/submit_next_batch.sh" "$T/config.sh" 2>&1)
map=$(find "$T/manager/state/array_sample_map" -name '*.tsv' -type f)
[[ $(awk -F '\t' 'NR==2{printf "%s:%s",$3,$4} NR==3{printf ",%s:%s",$3,$4}' "$map") == '1:s1,2:s2' ]]
[[ $(cut -f1-3 "$T/stream_invocation") == $'SAMtools/test\t1\t1' ]]
pipeline_repo_dir=$(cut -f5 "$T/stream_invocation")
[[ "$pipeline_repo_dir" == "$(realpath -m "$T")" ]]
[[ "$pipeline_repo_dir" == /* ]]
[[ "$submit_output" == *"INFO: pipeline_repo_dir=$(realpath -m "$T")"* ]]
[[ $(cut -f6-12 "$T/stream_invocation") == "$T/results"$'\t'"$T/results"$'\t'"$T/results"$'\t'"$T/work"$'\t'"$T/test.config"$'\tNextflow/test\t'"$T/refs/global:$T/refs/mt:$T/refs/nuclear" ]]

# Every repository artifact is validated before a wave or launcher invocation exists.
for workflow in launch_pipeline_streaming_per_sample.sh preprocessing.nf \
  numt_detection/numt_end2end.nf primate_pipeline_numt_decoy_round1.nf \
  primate_pipeline_round2_consensus_NUMT.nf; do
  missing_root=$(mktemp -d)
  cp -a "$T/." "$missing_root/"
  rm -f "$missing_root/$workflow" "$missing_root/manager/state/wave_sequence"
  find "$missing_root/manager/manifests/pipeline_waves" -type f -delete
  : > "$missing_root/invocations"
  sed -e "s|^MANAGER_ROOT=.*|MANAGER_ROOT=$missing_root/manager|" \
      -e "s|^PIPELINE_REPO=.*|PIPELINE_REPO=$missing_root|" \
      -e "s|^PIPELINE_LAUNCHER=.*|PIPELINE_LAUNCHER=$missing_root/launch_pipeline_streaming_per_sample.sh|" \
      "$T/config.sh" > "$missing_root/config.sh"
  before_status=$(sha256sum "$missing_root/manager/state/sample_status.tsv" "$missing_root/manager/state/wave_status.tsv")
  if output=$(TEST_ROOT="$missing_root" "$REPO/bin/submit_next_batch.sh" "$missing_root/config.sh" 2>&1); then
    echo "$workflow unexpectedly accepted" >&2; exit 1
  fi
  [[ "$output" == *"PIPELINE_REPO missing required workflow: $missing_root/$workflow"* ]]
  [[ "$before_status" == "$(sha256sum "$missing_root/manager/state/sample_status.tsv" "$missing_root/manager/state/wave_status.tsv")" ]]
  [[ ! -e "$missing_root/manager/state/wave_sequence" ]]
  [[ -z $(find "$missing_root/manager/manifests/pipeline_waves" -type f -print -quit) ]]
  [[ ! -s "$missing_root/invocations" ]]
done

# Each required reference produces a specific early validation error.
for variable in GLOBAL_REF_DIR REF_DIR NUCLEAR_ONLY_REF_DIR; do
  bad="$T/${variable}.config"
  sed "s|^${variable}=.*|${variable}=|" "$T/config.sh" > "$bad"
  if output=$(bash -c 'source "$1/lib/common.sh"; load_config "$2"; validate_config' _ "$REPO" "$bad" 2>&1); then
    echo "$variable unexpectedly accepted" >&2; exit 1
  fi
  [[ "$output" == *"$variable is required"* ]]
done

# Retry limits count the initial manager submission, not worker-local retries.
source "$REPO/lib/common.sh"
MAX_DEFERRED_RETRIES=2
[[ $(deferred_terminal_status NORMAL 99) == PIPELINE_DEFERRED_RETRY ]]
[[ $(deferred_terminal_status DEFERRED_RETRY 2) == PIPELINE_DEFERRED_RETRY ]]
[[ $(deferred_terminal_status DEFERRED_RETRY 3) == PIPELINE_DEFERRED_FAILED ]]

# Wave-root cleanup entry points are hard no-ops for the shared work root.
mkdir -p "$T/work/sentinel"; touch "$T/work/sentinel/keep"
"$REPO/bin/cleanup_terminal_deferred_wave_workdirs.sh" "$T/config.sh"
"$REPO/bin/cleanup_orphan_wave_workdirs.sh" "$T/config.sh" --apply
[[ -f "$T/work/sentinel/keep" ]]
echo 'Streaming manager tests passed.'
