#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test_helper.sh"
new_env
cat >> "$T/config.sh" <<EOF
PIPELINE_MODE=streaming_per_sample
EOF
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null

set_status() {
  local sample=$1 status=$2
  awk -F '\t' -v OFS='\t' -v s="$sample" -v status="$status" \
    'NR==1{print;next}$1==s{$4=status;$11="";$14=""}{print}' \
    "$T/manager/state/sample_status.tsv" > "$T/status"
  mv "$T/status" "$T/manager/state/sample_status.tsv"
}

make_valid_outputs() {
  local sample=$1 dir="$T/results/$1"
  mkdir -p "$dir/alignment" "$dir/out"
  printf x > "$dir/alignment/$sample.cram"
  printf x > "$dir/alignment/$sample.cram.crai"
  touch "$dir/alignment/$sample.cram.complete"
  printf ok | gzip > "$dir/out/$sample.round2.original_coords.clean.final.split.vcf.gz"
  printf x > "$dir/out/$sample.round2.original_coords.per_base_coverage.tsv"
  printf x > "$dir/out/$sample.numt_decoy.clean.realigned.per_base_coverage.tsv"
  printf x > "$dir/out/$sample.round2.mtcn.tsv"
}

assert_ready() {
  local sample=$1
  assert awk -F '\t' -v s="$sample" '$1==s&&$4=="READY_TO_TRANSFER"&&$11=="READY"&&$14=="all required outputs validated"{ok=1}END{exit !ok}' "$T/manager/state/sample_status.tsv"
}

# Submitted/PENDING work is excluded; only executing lifecycle states validate.
for spec in s1:PIPELINE_SUBMITTED s2:PIPELINE_RUNNING s3:PIPELINE_DEFERRED_RUNNING; do
  sample=${spec%%:*}; status=${spec#*:}
  set_status "$sample" "$status"
  make_valid_outputs "$sample"
done
"$REPO/bin/scan_active_results.sh" "$T/config.sh" >/dev/null
assert awk -F '\t' '$1=="s1"&&$4=="PIPELINE_SUBMITTED"{ok=1}END{exit !ok}' "$T/manager/state/sample_status.tsv"
assert_ready s2
assert_ready s3

# A completion marker only triggers the same validator; valid outputs remain required.
set_status s1 PIPELINE_SUBMITTED
awk -F '\t' 'NR==1||$1!="s1"' "$T/manager/state/output_validation.tsv" > "$T/validation"
mv "$T/validation" "$T/manager/state/output_validation.tsv"
mkdir -p "$T/work/.sample_state"
printf 'sample_id\tcompleted_at\ns1\tnow\n' > "$T/work/.sample_state/s1.complete.tsv"
"$REPO/bin/ingest_sample_markers.sh" "$T/config.sh" >/dev/null
assert_ready s1

# Incomplete formal outputs never advance, even when a completion marker exists.
rm -rf "$T/results/s1" "$T/results/s2"
set_status s1 PIPELINE_SUBMITTED
set_status s2 PIPELINE_SUBMITTED
awk -F '\t' 'NR==1||($1!="s1"&&$1!="s2")' "$T/manager/state/output_validation.tsv" > "$T/validation"
mv "$T/validation" "$T/manager/state/output_validation.tsv"
mkdir -p "$T/results/s1/alignment"
printf x > "$T/results/s1/alignment/s1.cram"
rm -f "$T/work/.sample_state/s1.complete.tsv"
printf 'sample_id\tcompleted_at\n' > "$T/work/.sample_state/s2.complete.tsv"
printf 's2\tnow\n' >> "$T/work/.sample_state/s2.complete.tsv"
"$REPO/bin/scan_active_results.sh" "$T/config.sh" >/dev/null
assert awk -F '\t' '$1=="s1"&&$4=="PIPELINE_SUBMITTED"{ok=1}END{exit !ok}' "$T/manager/state/sample_status.tsv"
"$REPO/bin/ingest_sample_markers.sh" "$T/config.sh" >/dev/null
assert awk -F '\t' '$1=="s2"&&$4=="PIPELINE_SUBMITTED"{ok=1}END{exit !ok}' "$T/manager/state/sample_status.tsv"

# Transfer-terminal and pending samples remain outside the explicit active whitelist.
for status in TRANSFERRING TRANSFERRED_FULL LOCAL_FINAL_RETAINED PENDING; do
  set_status s3 "$status"
  awk -F '\t' '$1!="s3"' "$T/manager/state/output_validation.tsv" > "$T/validation"
  mv "$T/validation" "$T/manager/state/output_validation.tsv"
  "$REPO/bin/scan_active_results.sh" "$T/config.sh" >/dev/null
  assert awk -F '\t' -v s="$status" '$1=="s3"&&$4==s{ok=1}END{exit !ok}' "$T/manager/state/sample_status.tsv"
  assert awk -F '\t' '$1=="s3"{found=1}END{exit found}' "$T/manager/state/output_validation.tsv"
done

echo 'Active result scanning tests passed.'
