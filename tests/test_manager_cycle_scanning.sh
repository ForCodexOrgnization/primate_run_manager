#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/test_helper.sh"
new_env
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
sed -i 's/ENABLE_PIPELINE_SUBMIT=1/ENABLE_PIPELINE_SUBMIT=0/' "$T/config.sh"

cat > "$T/mockbin/samtools" <<'SAMTOOLS'
#!/usr/bin/env bash
printf '%s\n' "${@: -1}" >> "$TEST_ROOT/samtools.invocations"
exit "${SAMTOOLS_EXIT:-0}"
SAMTOOLS
chmod +x "$T/mockbin/samtools"

# The default manager path is incremental. With no active samples it invokes no
# samtools process and succeeds outside Slurm (a full scan would be blocked).
"$REPO/bin/manager_cycle.sh" "$T/config.sh" >"$T/manager.out" 2>"$T/manager.err"
assert test ! -e "$T/samtools.invocations"
assert grep -q 'No active pipeline samples to validate' "$T/manager.err"

make_outputs() {
    local sample=$1 dir="$T/results/$1"
    mkdir -p "$dir/alignment" "$dir/out"
    printf 'cram\n' > "$dir/alignment/$sample.cram"
    printf 'index\n' > "$dir/alignment/$sample.cram.crai"
    printf ok | gzip > "$dir/out/$sample.round2.original_coords.clean.final.split.vcf.gz"
    printf x > "$dir/out/$sample.round2.original_coords.per_base_coverage.tsv"
    printf x > "$dir/out/$sample.numt_decoy.clean.realigned.per_base_coverage.tsv"
    printf x > "$dir/out/$sample.round2.mtcn.tsv"
}
make_outputs s1
make_outputs s2
awk -F '\t' -v OFS='\t' 'NR==1{print;next}$1=="s1"{$4="PIPELINE_RUNNING"}$1=="s2"{$4="PENDING"}{print}' \
    "$T/manager/state/sample_status.tsv" > "$T/status"; mv "$T/status" "$T/manager/state/sample_status.tsv"
"$REPO/bin/manager_cycle.sh" "$T/config.sh" >/dev/null 2>"$T/active.err"
assert test "$(wc -l < "$T/samtools.invocations")" -eq 1
assert grep -q '/s1.cram$' "$T/samtools.invocations"
if grep -q '/s2.cram$' "$T/samtools.invocations"; then echo 'inactive sample was scanned' >&2; exit 1; fi

# Full scans are protected outside Slurm and allowed inside it.
if "$REPO/bin/scan_results.sh" "$T/config.sh" >"$T/full.out" 2>"$T/full.err"; then
    echo 'interactive full scan unexpectedly succeeded' >&2; exit 1
fi
assert grep -q 'Full sample validation must run through Slurm.' "$T/full.err"
SLURM_JOB_ID=123 "$REPO/bin/scan_results.sh" "$T/config.sh" >/dev/null 2>"$T/slurm-scan.err"

# SIGKILL-style quickcheck status is transient: preserve cache and sample state.
awk -F '\t' -v OFS='\t' 'NR==1{print;next}$1=="s1"{$4="PIPELINE_RUNNING"}{print}' \
    "$T/manager/state/sample_status.tsv" > "$T/status"; mv "$T/status" "$T/manager/state/sample_status.tsv"
cache_before=$(awk -F '\t' '$1=="s1"' "$T/manager/state/output_validation.tsv")
printf changed >> "$T/results/s1/alignment/s1.cram"
SAMTOOLS_EXIT=137 "$REPO/bin/scan_active_results.sh" "$T/config.sh" >/dev/null 2>"$T/killed.err"
cache_after=$(awk -F '\t' '$1=="s1"' "$T/manager/state/output_validation.tsv")
assert test "$cache_after" = "$cache_before"
assert awk -F '\t' '$1=="s1"&&$4=="PIPELINE_RUNNING"{ok=1}END{exit !ok}' "$T/manager/state/sample_status.tsv"
assert awk -F '\t' '$1=="s1"&&$3=="cram_quickcheck_killed"{ok=1}END{exit !ok}' "$T/manager/state/validation_transient_errors.tsv"
assert grep -q 'cram_quickcheck_killed' "$T/killed.err"

# Submission uses sbatch with the wrapper and an absolute config path.
cat > "$T/mockbin/sbatch" <<'SBATCH'
#!/usr/bin/env bash
printf '%s\n' "$*" > "$TEST_ROOT/manager-sbatch.args"
printf '24680;cluster\n'
SBATCH
chmod +x "$T/mockbin/sbatch"
"$REPO/bin/submit_manager_cycle.sh" "$T/config.sh" > "$T/manager-submission.out"
assert grep -q 'Submitted manager cycle Slurm job 24680' "$T/manager-submission.out"
assert grep -q "$REPO/run_manager_cycle.slurm $T/config.sh" "$T/manager-sbatch.args"

# A Slurm spool copy resolves manager_cycle.sh strictly through MANAGER_ROOT.
mkdir -p "$T/manager-spool" "$T/mock-cycle-manager/bin" "$T/mock-cycle-manager/logs"
cp "$REPO/run_manager_cycle.slurm" "$T/manager-spool/slurm_script"
cat > "$T/mock-cycle-manager/bin/manager_cycle.sh" <<'CYCLE'
#!/usr/bin/env bash
printf '%s\t%s\n' "$0" "$1" > "$TEST_ROOT/manager-spool.invocation"
CYCLE
chmod +x "$T/mock-cycle-manager/bin/manager_cycle.sh"
cat > "$T/manager-spool-config.sh" <<CONFIG
MANAGER_ROOT="$T/mock-cycle-manager"
RUNTIME_LOG_DIR="$T/mock-cycle-manager/logs"
CONFIG
(cd "$T/manager-spool" && SLURM_JOB_ID=24680 SLURM_SUBMIT_DIR="$T" bash ./slurm_script manager-spool-config.sh)
assert grep -qx "$T/mock-cycle-manager/bin/manager_cycle.sh"$'\t'"$T/manager-spool-config.sh" "$T/manager-spool.invocation"

echo 'Manager cycle scanning tests passed.'
