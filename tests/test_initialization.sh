#!/usr/bin/env bash
source "$(dirname "$0")/test_helper.sh"; new_env
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
assert test "$(wc -l < "$T/manager/state/sample_status.tsv")" -eq 4
# Exercise 10-column migration and preservation.
printf 'sample_id\tspecies\thpc\tstatus\tslurm_job_id\tpipeline_batch\tglobus_task_id\tworkspace_path\tlast_update\tnotes\ns1\tsp1\tTEST\tSUBMITTED\t9\told\t\t\tdate\tkeep\n' > "$T/manager/state/sample_status.tsv"
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
assert grep -q $'s1\tsp1\tTEST\tWAVE_SUBMITTED\t9\told\t1' "$T/manager/state/sample_status.tsv"
assert test "$(find "$T/manager/state" -name 'sample_status.tsv.bak.*' | wc -l)" -eq 1
