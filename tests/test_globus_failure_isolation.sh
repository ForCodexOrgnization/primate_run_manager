#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test_helper.sh"; new_env
sed -i 's/ENABLE_TRANSFER=0/ENABLE_TRANSFER=1/; s/ENABLE_PIPELINE_SUBMIT=1/ENABLE_PIPELINE_SUBMIT=0/; s/PATH_CHECK_REQUIRED=1/PATH_CHECK_REQUIRED=0/' "$T/config.sh"
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
printf 'batch_id\ttask_id\tstatus\tsample_file\tsubmit_time\tlast_update\tnotes\nb1\ttask1\tACTIVE\t%s\tnow\tnow\tsubmitted\n' "$T/samples/list.tsv" > "$T/manager/state/transfer_tasks.tsv"
cat > "$T/mockbin/globus" <<'GLOBUS'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$TEST_ROOT/globus.calls"
if [[ "$1 $2" == 'task show' ]]; then printf 'authentication required\n' >&2; exit 4; fi
exit 0
GLOBUS
chmod +x "$T/mockbin/globus"
if "$REPO/bin/check_globus_tasks.sh" "$T/config.sh" >"$T/check.out" 2>"$T/check.err"; then echo 'auth check unexpectedly healthy' >&2; exit 1; fi
assert awk -F '\t' 'NR==2&&$1=="AUTH_REQUIRED"&&$2=="task_show"&&$3==4{ok=1}END{exit !ok}' "$T/manager/state/globus_health.tsv"
assert awk -F '\t' 'NR==2&&$3=="ACTIVE"{ok=1}END{exit !ok}' "$T/manager/state/transfer_tasks.tsv"
assert grep -q 'conservatively preserving ACTIVE' "$T/check.err"

# Manager isolates the same auth error and still reaches non-transfer work and
# a successful cycle status.
"$REPO/bin/manager_cycle.sh" "$T/config.sh" >"$T/cycle.out" 2>"$T/cycle.err"
assert grep -q 'continuing manager cycle' "$T/cycle.err"
assert awk -F '\t' 'NR==2&&$1=="SUCCESS"&&$2==0{ok=1}END{exit !ok}' "$T/manager/state/manager_cycle_status.tsv"
assert awk -F '\t' 'NR==2&&$3=="ACTIVE"{ok=1}END{exit !ok}' "$T/manager/state/transfer_tasks.tsv"

# UNKNOWN is also visible and conservative.
sed -i 's/exit 4/exit 7/' "$T/mockbin/globus"
"$REPO/bin/check_globus_tasks.sh" "$T/config.sh" >/dev/null 2>"$T/unknown.err" || true
assert awk -F '\t' 'NR==2&&$1=="UNKNOWN"&&$3==7{ok=1}END{exit !ok}' "$T/manager/state/globus_health.tsv"
assert grep -q 'status UNKNOWN' "$T/unknown.err"
assert awk -F '\t' 'NR==2&&$3=="ACTIVE"{ok=1}END{exit !ok}' "$T/manager/state/transfer_tasks.tsv"
# An already-READY batch is submitted before the expensive validator starts.
new_env
sed -i 's/ENABLE_TRANSFER=0/ENABLE_TRANSFER=1/; s/ENABLE_PIPELINE_SUBMIT=1/ENABLE_PIPELINE_SUBMIT=0/; s/PATH_CHECK_REQUIRED=1/PATH_CHECK_REQUIRED=0/' "$T/config.sh"
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
for s in s1 s2 s3; do
  d="$T/results/$s"; mkdir -p "$d/alignment" "$d/out"
  printf x > "$d/alignment/$s.cram"; printf x > "$d/alignment/$s.cram.crai"
  printf ok | gzip > "$d/out/$s.round2.original_coords.clean.final.split.vcf.gz"
  printf x > "$d/out/$s.round2.original_coords.per_base_coverage.tsv"
  printf x > "$d/out/$s.numt_decoy.clean.realigned.per_base_coverage.tsv"
  printf x > "$d/out/$s.round2.mtcn.tsv"
done
awk -F '\t' -v OFS='\t' 'NR==1{print;next}$1~/^s[12]$/{$4="READY_TO_TRANSFER"}$1=="s3"{$4="PIPELINE_RUNNING"}{print}' "$T/manager/state/sample_status.tsv" > "$T/state"; mv "$T/state" "$T/manager/state/sample_status.tsv"
cat > "$T/mockbin/globus" <<'GLOBUS'
#!/usr/bin/env bash
case "$1 $2" in
  'task show') echo ACTIVE;;
  'transfer src:/source') echo transfer >> "$TEST_ROOT/order"; echo task-fast;;
esac
GLOBUS
cat > "$T/mockbin/samtools" <<'SAMTOOLS'
#!/usr/bin/env bash
echo validation >> "$TEST_ROOT/order"
SAMTOOLS
chmod +x "$T/mockbin/globus" "$T/mockbin/samtools"
"$REPO/bin/manager_cycle.sh" "$T/config.sh" >/dev/null 2>"$T/fast.err"
assert test "$(sed -n '1p' "$T/order")" = transfer
assert grep -q '^validation$' "$T/order"
echo 'Globus failure isolation tests passed.'
