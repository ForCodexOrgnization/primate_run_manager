#!/usr/bin/env bash
source "$(dirname "$0")/test_helper.sh"; new_env

# Initialization remains a lightweight login-node operation.
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
assert test "$(wc -l < "$T/manager/state/sample_status.tsv")" -eq 4

# Historical validation is refused outside Slurm unless explicitly overridden.
if "$REPO/bin/import_existing_results.sh" "$T/config.sh" >"$T/refused.out" 2>"$T/refused.err"; then
    echo "interactive historical import unexpectedly succeeded" >&2; exit 1
fi
assert grep -q 'Historical import performs CRAM validation and must run through Slurm.' "$T/refused.err"

make_outputs() {
    local sample=$1 dir="$T/results/$1"
    mkdir -p "$dir/alignment" "$dir/out"
    printf 'cram-%s\n' "$sample" > "$dir/alignment/$sample.cram"
    printf 'index\n' > "$dir/alignment/$sample.cram.crai"
    printf ok | gzip > "$dir/out/$sample.round2.original_coords.clean.final.split.vcf.gz"
    printf coverage > "$dir/out/$sample.round2.original_coords.per_base_coverage.tsv"
    printf coverage > "$dir/out/$sample.numt_decoy.clean.realigned.per_base_coverage.tsv"
    printf mtcn > "$dir/out/$sample.round2.mtcn.tsv"
}
make_outputs s1; make_outputs s2
before=$(sha256sum "$T/results/s1/alignment/s1.cram")
cat > "$T/mockbin/samtools" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$3" >> "$TEST_ROOT/quickchecks"
[[ "$3" == *s1.cram ]] && sleep 2
exit 0
EOF
chmod +x "$T/mockbin/samtools"
printf '\nSAMTOOLS_QUICKCHECK_TIMEOUT_SECONDS=1\n' >> "$T/config.sh"

# The override permits controlled test/admin execution; timeout is per-sample.
ALLOW_INTERACTIVE_IMPORT=1 "$REPO/bin/import_existing_results.sh" "$T/config.sh" >"$T/import.out" 2>"$T/import.err"
assert awk -F '\t' '$1=="s1"&&$8==0&&$10=="cram_quickcheck_timeout"{ok=1}END{exit !ok}' "$T/manager/state/output_validation.tsv"
assert awk -F '\t' '$1=="s2"&&$8==1{ok=1}END{exit !ok}' "$T/manager/state/output_validation.tsv"
assert grep -q 'Validating sample s1 (1/3)' "$T/import.err"
assert grep -q 'INCOMPLETE: cram_quickcheck_timeout' "$T/import.err"
assert grep -q 'Validating sample s2 (2/3)' "$T/import.err"
assert grep -q 'COMPLETE' "$T/import.err"
assert test "$(sha256sum "$T/results/s1/alignment/s1.cram")" = "$before"

# A second Slurm import skips the unchanged successful sample and remains idempotent.
checks_before=$(grep -c 's2.cram' "$T/quickchecks")
SLURM_JOB_ID=42 bash "$REPO/run_import_existing.slurm" "$T/config.sh" >"$T/slurm.out" 2>"$T/slurm.err"
assert test "$(grep -c 's2.cram' "$T/quickchecks")" -eq "$checks_before"
assert test "$(awk -F '\t' '$1=="s2"{n++}END{print n+0}' "$T/manager/state/output_validation.tsv")" -eq 1
assert test "$(sha256sum "$T/results/s1/alignment/s1.cram")" = "$before"

# Submission uses sbatch, the wrapper, and the absolute config path.
cat > "$T/mockbin/sbatch" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" > "$TEST_ROOT/sbatch.args"
printf '98765;cluster\n'
EOF
chmod +x "$T/mockbin/sbatch"
"$REPO/bin/submit_import_existing.sh" "$T/config.sh" > "$T/submission.out"
assert grep -q 'Submitted historical import Slurm job 98765' "$T/submission.out"
assert grep -q "$REPO/run_import_existing.slurm $T/config.sh" "$T/sbatch.args"
