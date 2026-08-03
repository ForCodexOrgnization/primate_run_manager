#!/usr/bin/env bash
source "$(dirname "$0")/test_helper.sh"; new_env
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
s=s1; d="$T/results/$s"; mkdir -p "$d/alignment" "$d/out"; printf x > "$d/alignment/$s.cram"; printf x > "$d/alignment/$s.cram.crai"; printf ok | gzip > "$d/out/$s.round2.original_coords.clean.final.split.vcf.gz"; printf x > "$d/out/$s.round2.original_coords.per_base_coverage.tsv"; printf x > "$d/out/$s.numt_decoy.clean.realigned.per_base_coverage.tsv"
ALLOW_INTERACTIVE_FULL_SCAN=1 "$REPO/bin/scan_results.sh" "$T/config.sh"; assert grep -q $'^s1\t0\t1' "$T/manager/state/output_validation.tsv"
touch "$d/alignment/$s.cram.complete"
ALLOW_INTERACTIVE_FULL_SCAN=1 "$REPO/bin/scan_results.sh" "$T/config.sh"; assert grep -q $'^s1\t1\t1\t1\t1\t1\t0\t0' "$T/manager/state/output_validation.tsv"
printf x > "$d/out/$s.round2.mtcn.tsv"; ALLOW_INTERACTIVE_FULL_SCAN=1 "$REPO/bin/scan_results.sh" "$T/config.sh"; assert awk -F '\t' '$1=="s1"&&$4=="READY_TO_TRANSFER"{ok=1}END{exit !ok}' "$T/manager/state/sample_status.tsv"
printf broken > "$d/out/$s.round2.original_coords.clean.final.split.vcf.gz"; ALLOW_INTERACTIVE_FULL_SCAN=1 "$REPO/bin/scan_results.sh" "$T/config.sh"; assert grep -q $'^s1\t1\t1\t0' "$T/manager/state/output_validation.tsv"
# A failed Slurm wave is resolved per sample: one valid sample completes independently.
s=s2; d="$T/results/$s"; mkdir -p "$d/alignment" "$d/out"; printf x > "$d/alignment/$s.cram"; printf x > "$d/alignment/$s.cram.crai"; printf ok | gzip > "$d/out/$s.round2.original_coords.clean.final.split.vcf.gz"; printf x > "$d/out/$s.round2.original_coords.per_base_coverage.tsv"; printf x > "$d/out/$s.numt_decoy.clean.realigned.per_base_coverage.tsv"; printf x > "$d/out/$s.round2.mtcn.tsv"
touch "$d/alignment/$s.cram.complete"
awk -F '\t' -v OFS='\t' 'NR==1{print;next}$1=="s1"||$1=="s2"{$4="WAVE_SUBMITTED";$5="77";$6="wave_failed"}{print}' "$T/manager/state/sample_status.tsv" > "$T/x"; mv "$T/x" "$T/manager/state/sample_status.tsv"
printf 'wave_failed\tmanifest\t2\t77\tnow\tRUNNING\t0\t0\tRUNNING\tnow\t\n' >> "$T/manager/state/wave_status.tsv"
cat > "$T/mockbin/sacct" <<'S'
#!/usr/bin/env bash
printf '77_0|FAILED|\n'
printf '88_0|FAILED|\n'
S
chmod +x "$T/mockbin/sacct"
"$REPO/bin/update_wave_states.sh" "$T/config.sh"
assert awk -F '\t' '$1=="s1"&&$4=="PIPELINE_RETRY_READY"{bad=1}$1=="s2"&&$4=="READY_TO_TRANSFER"{good=1}END{exit !(bad&&good)}' "$T/manager/state/sample_status.tsv"
assert awk -F '\t' '$1=="wave_failed"&&$9=="PARTIAL_COMPLETE"{ok=1}END{exit !ok}' "$T/manager/state/wave_status.tsv"
# An incomplete sample at the configured attempt limit becomes terminally failed.
awk -F '\t' -v OFS='\t' 'NR==1{print;next}$1=="s3"{$4="PIPELINE_RETRY_RUNNING";$5="88";$6="wave_exhausted";$7=2}{print}' "$T/manager/state/sample_status.tsv" > "$T/x"; mv "$T/x" "$T/manager/state/sample_status.tsv"
printf 'wave_exhausted\tmanifest\t1\t88\tnow\tRUNNING\t0\t0\tRUNNING\tnow\t\n' >> "$T/manager/state/wave_status.tsv"
"$REPO/bin/update_wave_states.sh" "$T/config.sh"
assert awk -F '\t' '$1=="s3"&&$4=="PIPELINE_FAILED"&&$7==2{ok=1}END{exit !ok}' "$T/manager/state/sample_status.tsv"
