#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test_helper.sh"
new_env
printf 's1\tsp1\ns2\tsp2\ns3\tsp3\ns4\tsp4\n' > "$T/samples/list.tsv"
bash "$REPO/bin/initialize_samples.sh" "$T/config.sh"

# s1 has stale state but cleanup evidence is authoritative.
awk -F '\t' -v OFS='\t' 'NR==1{print;next}$1=="s1"{$4="READY_TO_TRANSFER"}{print}' "$T/manager/state/sample_status.tsv" > "$T/status"; mv "$T/status" "$T/manager/state/sample_status.tsv"
mkdir -p "$T/analysis"/{receipts,vcf,round2_coverage,numt_decoy_coverage,mtcn}
touch "$T/analysis/receipts/s1.transferred.tsv"
printf x > "$T/analysis/vcf/s1.round2.original_coords.clean.final.split.vcf.gz"
printf x > "$T/analysis/round2_coverage/s1.round2.original_coords.per_base_coverage.tsv"
printf x > "$T/analysis/numt_decoy_coverage/s1.numt_decoy.clean.realigned.per_base_coverage.tsv"
printf x > "$T/analysis/mtcn/s1.round2.mtcn.tsv"

# s3 was attempted and is incomplete. s4 is an active legacy streaming task.
mkdir -p "$T/results/s3" "$T/manager/state/array_sample_map"
cat > "$T/manager/state/array_sample_map/legacy.tsv" <<'EOF'
submission_id	array_job_id	array_task_id	sample_id	reference_name	sample_work_root	phase
old	900	7	s4	ref	/work/s4	NORMAL
EOF
cat > "$T/mockbin/sacct" <<'EOF'
#!/usr/bin/env bash
printf '900_7|RUNNING|\n'
EOF
chmod +x "$T/mockbin/sacct"

out="$T/audit-output"
before=$(sha256sum "$T/manager/state/sample_status.tsv" | cut -d' ' -f1)
REBUILD_OUTPUT_DIR="$out" bash "$REPO/bin/rebuild_sample_state.sh" "$T/config.sh" --preview
[[ $(sha256sum "$T/manager/state/sample_status.tsv" | cut -d' ' -f1) == "$before" ]]
assert awk -F '\t' '$1=="s1"&&$4=="LOCAL_FINAL_RETAINED"&&$8==1&&$9==1{ok=1}END{exit !ok}' "$out/audit.tsv"
assert awk -F '\t' '$1=="s2"&&$4=="PENDING"&&$14==0{ok=1}END{exit !ok}' "$out/audit.tsv"
assert awk -F '\t' '$1=="s3"&&$4=="PIPELINE_DEFERRED_RETRY"&&$14==1{ok=1}END{exit !ok}' "$out/audit.tsv"
assert awk -F '\t' '$1=="s4"&&$4=="PIPELINE_RUNNING"&&$11==900&&$12==7&&$13=="RUNNING"{ok=1}END{exit !ok}' "$out/audit.tsv"

REBUILD_OUTPUT_DIR="$T/applied" bash "$REPO/bin/rebuild_sample_state.sh" "$T/config.sh" --apply
[[ $(awk -F '\t' '$1=="s1"{print $4}' "$T/manager/state/sample_status.tsv") == LOCAL_FINAL_RETAINED ]]
[[ $(awk -F '\t' '$1=="s4"{print $4}' "$T/manager/state/sample_status.tsv") == PIPELINE_RUNNING ]]
compgen -G "$T/manager/state/sample_status.tsv.bak.*" >/dev/null
compgen -G "$T/manager/state/output_validation.tsv.bak.*" >/dev/null
echo "test_rebuild_sample_state: PASS"
