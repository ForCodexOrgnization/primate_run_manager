#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test_helper.sh"
new_env
mkdir -p "$T"/refs/{global,mt,nuclear} "$T/numt_detection"
for workflow in preprocessing.nf numt_detection/numt_end2end.nf primate_pipeline_numt_decoy_round1.nf primate_pipeline_round2_consensus_NUMT.nf; do
  printf '// mock\n' > "$T/$workflow"
done
printf '#!/usr/bin/env bash\n' > "$T/launch_pipeline_streaming_per_sample.sh"
cat > "$T/stream-launcher.sh" <<'LAUNCH'
#!/usr/bin/env bash
echo 'Submitted batch job 777'
LAUNCH
chmod +x "$T/stream-launcher.sh"
cat >> "$T/config.sh" <<EOF
PIPELINE_MODE=streaming_per_sample
SAMPLE_CHAIN_CONCURRENCY=2
SAMTOOLS_MODULE=mock
PIPELINE_LAUNCHER=$T/stream-launcher.sh
GLOBAL_REF_DIR=$T/refs/global
REF_DIR=$T/refs/mt
NUCLEAR_ONLY_REF_DIR=$T/refs/nuclear
EOF
printf 'config\n' > "$T/test.config"
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null

# Model a terminal normal submission whose accounting records have aged out,
# alongside deferred work that must become schedulable.
awk -F '\t' -v OFS='\t' 'NR==1{print;next} $1=="s1"{$4="PIPELINE_SUBMITTED";$6="old";$7=1} $1=="s2"{$4="PIPELINE_DEFERRED_RETRY";$7=1} $1=="s3"{$4="LOCAL_FINAL_RETAINED"} {print}' \
  "$T/manager/state/sample_status.tsv" > "$T/status"; mv "$T/status" "$T/manager/state/sample_status.tsv"
source "$REPO/lib/common.sh"; load_config "$T/config.sh"; ensure_state_files
append_wave_row $'old\tmanifest\t1\t555\tcreated\t\t0\t1\tCOMPLETE\tupdated\taudit only; phase=NORMAL'
cat > "$T/manager/state/submission_task_map/old.tsv" <<'MAP'
submission_id	pipeline_mode	phase	slurm_array_job_id	array_task_id	task_type	task_name	sample_id	reference_name	sample_work_root	batch_work_root
old	streaming_per_sample	NORMAL	555	1	SAMPLE	s1	s1	sp1	/work/s1	
MAP
cat > "$T/mockbin/sacct" <<'SACCT'
#!/usr/bin/env bash
exit 0
SACCT
chmod +x "$T/mockbin/sacct"

"$REPO/bin/ingest_sample_markers.sh" "$T/config.sh"
[[ $(awk -F '\t' '$1=="s1"{print $4}' "$T/manager/state/sample_status.tsv") == PIPELINE_DEFERRED_RETRY ]]
determine_manager_phase
[[ $(manager_phase) == DEFERRED_RETRY ]]
[[ $(awk -F '\t' 'NR==2{print $4}' "$MANAGER_PHASE_FILE") -eq 0 ]]

# The scheduler must now select deferred rows rather than searching PENDING.
"$REPO/bin/submit_next_batch.sh" "$T/config.sh" >/dev/null
[[ $(awk -F '\t' 'NR>1&&$4=="PIPELINE_DEFERRED_RUNNING"{n++}END{print n+0}' "$T/manager/state/sample_status.tsv") -eq 2 ]]

# A genuinely new submission remains active while Slurm accounting catches up.
sed -i 's/\tCOMPLETE\t/\tSUBMITTED\t/' "$T/manager/state/wave_status.tsv"
awk -F '\t' -v OFS='\t' 'NR==1{print;next} $1=="s1"{$4="PIPELINE_SUBMITTED";$6="old"} {print}' \
  "$T/manager/state/sample_status.tsv" > "$T/status"; mv "$T/status" "$T/manager/state/sample_status.tsv"
"$REPO/bin/ingest_sample_markers.sh" "$T/config.sh"
[[ $(awk -F '\t' '$1=="s1"{print $4}' "$T/manager/state/sample_status.tsv") == PIPELINE_SUBMITTED ]]
determine_manager_phase
[[ $(manager_phase) == NORMAL ]]
echo 'Stale streaming submission reconciliation tests passed.'
