#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test_helper.sh"
new_env
mkdir -p "$T"/refs/{global,mt,nuclear}
cat >> "$T/config.sh" <<EOF
PIPELINE_MODE=streaming_per_sample
GLOBAL_REF_DIR=$T/refs/global
REF_DIR=$T/refs/mt
NUCLEAR_ONLY_REF_DIR=$T/refs/nuclear
SAMTOOLS_MODULE=SAMtools/test
STREAM_SMOKE_TEST=1
SAMPLE_CHAIN_CONCURRENCY=1
EOF
cat > "$T/launcher.sh" <<'EOF'
#!/usr/bin/env bash
printf '%s\t%s\t%s\t%s\n' "$SAMTOOLS_MODULE" "$STREAM_SMOKE_TEST" "$MAX_CONCURRENT" "$FULL_SAMPLE_LIST" > "$TEST_ROOT/stream_invocation"
echo 'Submitted batch job 123456'
EOF
chmod +x "$T/launcher.sh"

"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
"$REPO/bin/submit_next_batch.sh" "$T/config.sh" >/dev/null
map=$(find "$T/manager/state/array_sample_map" -name '*.tsv' -type f)
[[ $(awk -F '\t' 'NR==2{printf "%s:%s",$3,$4} NR==3{printf ",%s:%s",$3,$4}' "$map") == '1:s1,2:s2' ]]
[[ $(cut -f1-3 "$T/stream_invocation") == $'SAMtools/test\t1\t1' ]]

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
