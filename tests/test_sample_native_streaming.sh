#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test_helper.sh"
new_env
mkdir -p "$T"/refs/{global,mt,nuclear} "$T/numt_detection"
for f in preprocessing.nf numt_detection/numt_end2end.nf primate_pipeline_numt_decoy_round1.nf primate_pipeline_round2_consensus_NUMT.nf; do printf '// mock\n' > "$T/$f"; done
{
  for i in $(seq -w 1 100); do printf 'sample%s\tsp\n' "$i"; done
  printf 'manual\tsp\nready\tsp\n'
} > "$T/samples/list.tsv"
cat >> "$T/config.sh" <<EOF2
PIPELINE_MODE=streaming_per_sample
GLOBAL_REF_DIR=$T/refs/global
REF_DIR=$T/refs/mt
NUCLEAR_ONLY_REF_DIR=$T/refs/nuclear
SAMTOOLS_MODULE=mock
SAMPLE_CHAIN_CONCURRENCY=10
PIPELINE_WAVE_SIZE=50
MAX_ACTIVE_PIPELINE_WAVES=1
EOF2
cat > "$T/launch_pipeline_streaming_per_sample.sh" <<'LAUNCH'
#!/usr/bin/env bash
printf '%s\t%s\n' "$MAX_CONCURRENT" "$(wc -l < "$FULL_SAMPLE_LIST")" >> "$TEST_ROOT/invocations"
cp "$FULL_SAMPLE_LIST" "$TEST_ROOT/submitted.samples"
echo 'Submitted batch job 555'
LAUNCH
chmod +x "$T/launch_pipeline_streaming_per_sample.sh"
sed -i "s|^PIPELINE_LAUNCHER=.*|PIPELINE_LAUNCHER=$T/launch_pipeline_streaming_per_sample.sh|" "$T/config.sh"
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
# Manual-review and retained states are never eligible.
awk -F '\t' -v OFS='\t' 'NR==1{print;next}$1=="manual"{$4="PIPELINE_INCOMPLETE_REVIEW"}$1=="ready"{$4="LOCAL_FINAL_RETAINED"}{print}' "$T/manager/state/sample_status.tsv" > "$T/s"; mv "$T/s" "$T/manager/state/sample_status.tsv"
"$REPO/bin/submit_next_batch.sh" "$T/config.sh" >/dev/null
[[ $(cat "$T/invocations") == $'10\t100' ]]
[[ $(wc -l < "$T/submitted.samples") -eq 100 ]]
[[ $(awk -F '\t' 'NR>1&&$4=="PIPELINE_SUBMITTED"{n++}END{print n+0}' "$T/manager/state/sample_status.tsv") -eq 100 ]]
[[ $(awk -F '\t' '$1=="manual"{print $4}' "$T/manager/state/sample_status.tsv") == PIPELINE_INCOMPLETE_REVIEW ]]
[[ $(awk -F '\t' '$1=="ready"{print $4}' "$T/manager/state/sample_status.tsv") == LOCAL_FINAL_RETAINED ]]
# A second array is forbidden while the first owns the %10 global budget.
"$REPO/bin/submit_next_batch.sh" "$T/config.sh" >/dev/null
[[ $(wc -l < "$T/invocations") -eq 1 ]]
map=$(find "$T/manager/state/array_sample_map" -type f -name '*.tsv')
[[ $(awk -F '\t' 'NR==2{print $2"_"$3":"$4":"$7}' "$map") == 555_1:sample001:NORMAL ]]
# Independent lifecycle: validated task 1 advances while exact task 2 runs.
printf 'sample001\t1\t1\t1\t1\t1\t1\t1\t2026-01-01T00:00:00Z\tok\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n' >> "$T/manager/state/output_validation.tsv"
cat > "$T/mockbin/sacct" <<'SACCT'
#!/usr/bin/env bash
case " $* " in *" 555_2 "*) printf '555_2|RUNNING|\n';; *) printf '555_1|COMPLETED|\n';; esac
SACCT
chmod +x "$T/mockbin/sacct"
"$REPO/bin/ingest_sample_markers.sh" "$T/config.sh"
[[ $(awk -F '\t' '$1=="sample001"{print $4}' "$T/manager/state/sample_status.tsv") == READY_TO_TRANSFER ]]
[[ $(awk -F '\t' '$1=="sample002"{print $4}' "$T/manager/state/sample_status.tsv") == PIPELINE_RUNNING ]]
echo 'Sample-native streaming tests passed.'
