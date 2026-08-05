#!/usr/bin/env bash
source "$(dirname "$0")/test_helper.sh"; new_env
printf 'config\n' > "$T/test.config"
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
"$REPO/bin/submit_next_batch.sh" "$T/config.sh" >/dev/null 2>"$T/submit1.err"
wave=$(awk -F '\t' 'NR==2{print $1}' "$T/manager/state/wave_status.tsv")
work=$(awk -F '\t' 'NR==2{print $12}' "$T/manager/state/wave_status.tsv")
assert test -d "$work"
assert awk -F '\t' 'NR==1&&$12=="work_root"&&$16=="resume_eligible"&&$19=="pipeline_git_commit"{ok=1}END{exit !ok}' "$T/manager/state/wave_status.tsv"
# REQUEUED+/array element remains active and does not increment attempts or create another wave.
cat > "$T/mockbin/sacct" <<'S'
#!/usr/bin/env bash
printf '123456_0|REQUEUED+\n'
S
chmod +x "$T/mockbin/sacct"
"$REPO/bin/update_wave_states.sh" "$T/config.sh" >/dev/null
assert awk -F '\t' -v w="$wave" 'NR>1&&$1==w&&$9~/^(SUBMITTED|RUNNING)$/&&$6=="REQUEUED"{ok=1}END{exit !ok}' "$T/manager/state/wave_status.tsv"
assert test "$(awk -F '\t' '$4~/^(WAVE_SUBMITTED|PIPELINE_RUNNING)$/&&$7==1{n++}END{print n+0}' "$T/manager/state/sample_status.tsv")" -eq 2
"$REPO/bin/submit_next_batch.sh" "$T/config.sh" >/dev/null 2>"$T/submit_requeued.err"
assert test "$(wc -l < "$T/invocations")" -eq 1
# TIMEOUT is infrastructure/resume eligible and later retry with same full manifest reuses work root.
cat > "$T/mockbin/sacct" <<'S'
#!/usr/bin/env bash
printf '123456_0|TIMEOUT\n'
S
chmod +x "$T/mockbin/sacct"
"$REPO/bin/update_wave_states.sh" "$T/config.sh" >/dev/null || true
assert awk -F '\t' -v w="$wave" 'NR>1&&$1==w&&$15=="INFRASTRUCTURE"&&$16==1{ok=1}END{exit !ok}' "$T/manager/state/wave_status.tsv"
assert test "$(awk -F '\t' '$4=="PIPELINE_RETRY_READY"{n++}END{print n+0}' "$T/manager/state/sample_status.tsv")" -eq 2
"$REPO/bin/submit_next_batch.sh" "$T/config.sh" >/dev/null 2>"$T/submit2.err"
assert grep -q "INFO: retry_mode=resume" "$T/submit2.err"
assert awk -F '\t' -v old="$wave" -v work="$work" 'NR>1&&$13==old&&$12==work{ok=1}END{exit !ok}' "$T/manager/state/wave_status.tsv"
assert grep -q $'5\t1\t'"$work"$'\tNextflow/test' "$T/invocations"
# FAILED is not resume eligible.
new_env; printf 'config\n' > "$T/test.config"; "$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null; "$REPO/bin/submit_next_batch.sh" "$T/config.sh" >/dev/null 2>&1
cat > "$T/mockbin/sacct" <<'S'
#!/usr/bin/env bash
printf '123456_0|FAILED\n'
S
chmod +x "$T/mockbin/sacct"; "$REPO/bin/update_wave_states.sh" "$T/config.sh" >/dev/null || true
assert awk -F '\t' 'NR==2&&$15=="PIPELINE"&&$16==0{ok=1}END{exit !ok}' "$T/manager/state/wave_status.tsv"
# old schema migration is backed up and expands safely.
new_env; mkdir -p "$T/manager/state"; printf 'wave_id\tsample_manifest\tsample_count\tpipeline_job_id\tsubmit_time\tslurm_state\tcomplete_count\tincomplete_count\tstatus\tlast_update\tnotes\nold\tm\t1\tj\tt\tRUNNING\t0\t0\tRUNNING\tt\tn\n' > "$T/manager/state/wave_status.tsv"
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
assert awk -F '\t' 'NR==1{exit !(NF==21)}' "$T/manager/state/wave_status.tsv"
assert compgen -G "$T/manager/state/wave_status.tsv.bak.*" >/dev/null
