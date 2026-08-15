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
# Submission rows aggregate exact task states, so an active REQUEUED element is
# represented by the deliberately generic ACTIVE audit state.
assert awk -F '\t' -v w="$wave" 'NR>1&&$1==w&&$9=="RUNNING"&&$6=="ACTIVE"{ok=1}END{exit !ok}' "$T/manager/state/wave_status.tsv"
assert test "$(awk -F '\t' '$4~/^(WAVE_SUBMITTED|PIPELINE_SUBMITTED|PIPELINE_RUNNING)$/&&$7==1{n++}END{print n+0}' "$T/manager/state/sample_status.tsv")" -eq 3
"$REPO/bin/submit_next_batch.sh" "$T/config.sh" >/dev/null 2>"$T/submit_requeued.err"
assert test "$(wc -l < "$T/invocations")" -eq 1
# TIMEOUT is terminal: it is deferred and later receives a fresh work root.
cat > "$T/mockbin/sacct" <<'S'
#!/usr/bin/env bash
printf '123456_0|TIMEOUT\n'
S
chmod +x "$T/mockbin/sacct"
"$REPO/bin/update_wave_states.sh" "$T/config.sh" >/dev/null || true
"$REPO/bin/ingest_batch_tasks.sh" "$T/config.sh" >/dev/null || true
# Task-native reconciliation deliberately retries exact failed work fresh; the
# old wave-level resume classification is no longer populated.
assert awk -F '\t' '$4=="PIPELINE_DEFERRED_RETRY"&&$8=="TERMINAL_BATCH_TIMEOUT_INCOMPLETE"{n++}END{exit !(n==3)}' "$T/manager/state/sample_status.tsv"
assert test "$(awk -F '\t' '$4=="PIPELINE_DEFERRED_RETRY"{n++}END{print n+0}' "$T/manager/state/sample_status.tsv")" -eq 3
# Drain the remaining new sample before deferred work is eligible.
awk -F '\t' -v OFS='\t' '$1=="s3"{$4="READY_TO_TRANSFER"}{print}' "$T/manager/state/sample_status.tsv" > "$T/state.tmp"; mv "$T/state.tmp" "$T/manager/state/sample_status.tsv"
"$REPO/bin/submit_next_batch.sh" "$T/config.sh" >/dev/null 2>"$T/submit2.err"
assert awk -F '\t' -v work="$work" 'NR>2&&$12!=work&&$11~/phase=DEFERRED_RETRY/{ok=1}END{exit !ok}' "$T/manager/state/wave_status.tsv"
assert grep -q $'5\t1\t.*/submissions/batch_.*\tNextflow/test' "$T/invocations"
# FAILED is not resume eligible.
new_env; printf 'config\n' > "$T/test.config"; "$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null; "$REPO/bin/submit_next_batch.sh" "$T/config.sh" >/dev/null 2>&1
cat > "$T/mockbin/sacct" <<'S'
#!/usr/bin/env bash
printf '123456_0|FAILED\n'
S
chmod +x "$T/mockbin/sacct"; "$REPO/bin/update_wave_states.sh" "$T/config.sh" >/dev/null || true
"$REPO/bin/ingest_batch_tasks.sh" "$T/config.sh" >/dev/null || true
assert awk -F '\t' '$4=="PIPELINE_DEFERRED_RETRY"&&$8=="TERMINAL_BATCH_FAILED_INCOMPLETE"{n++}END{exit !(n==3)}' "$T/manager/state/sample_status.tsv"
# old schema migration is backed up and expands safely.
new_env; mkdir -p "$T/manager/state"; printf 'wave_id\tsample_manifest\tsample_count\tpipeline_job_id\tsubmit_time\tslurm_state\tcomplete_count\tincomplete_count\tstatus\tlast_update\tnotes\nold\tm\t1\tj\tt\tRUNNING\t0\t0\tRUNNING\tt\tn\n' > "$T/manager/state/wave_status.tsv"
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
assert awk -F '\t' 'NR==1{exit !(NF==21)}' "$T/manager/state/wave_status.tsv"
assert compgen -G "$T/manager/state/wave_status.tsv.bak.*" >/dev/null
