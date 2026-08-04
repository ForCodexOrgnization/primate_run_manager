#!/usr/bin/env bash
source "$(dirname "$0")/test_helper.sh"

# Bouchet's reviewed transfer automation is enabled, while pipeline submission
# and imported-incomplete retries remain conservative.
assert grep -q '^ENABLE_PIPELINE_SUBMIT=0$' "$REPO/config/bouchet.sh"
assert grep -q '^ENABLE_TRANSFER=1$' "$REPO/config/bouchet.sh"
assert grep -q '^ENABLE_LOCAL_CLEANUP=1$' "$REPO/config/bouchet.sh"
assert grep -q '^AUTO_RETRY_IMPORTED_INCOMPLETE=0$' "$REPO/config/bouchet.sh"

# The unconfigured second-cluster template remains inert.
assert grep -q '^ENABLE_PIPELINE_SUBMIT=0$' "$REPO/config/hpc2.template.sh"
assert grep -q '^ENABLE_TRANSFER=0$' "$REPO/config/hpc2.template.sh"
assert grep -q '^ENABLE_LOCAL_CLEANUP=0$' "$REPO/config/hpc2.template.sh"
assert grep -q '^DRY_RUN=1$' "$REPO/config/hpc2.template.sh"

# A required path check blocks Globus before it creates a batch manifest.
new_env; "$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
awk -F '\t' -v OFS='\t' 'NR==1{print;next}$1=="s1"{$4="READY_TO_TRANSFER"}{print}' "$T/manager/state/sample_status.tsv" > "$T/x"; mv "$T/x" "$T/manager/state/sample_status.tsv"
sed -i 's/ENABLE_TRANSFER=0/ENABLE_TRANSFER=1/;s/DRY_RUN=0/DRY_RUN=1/;s/TRANSFER_BATCH_SIZE=2/TRANSFER_BATCH_SIZE=1/' "$T/config.sh"
if "$REPO/bin/submit_globus_batch.sh" "$T/config.sh" 2>/dev/null; then echo "transfer was not blocked" >&2; exit 1; fi
assert test "$(find "$T/manager/manifests/transfer_batches" -type f | wc -l)" -eq 0

# Stale path markers and samples absent from the source view are also blocked.
printf 'LOCAL_RESULTS=/stale/results\nSOURCE_ROOT_LOCAL_VIEW=/stale/source\n' > "$T/manager/state/path_check.passed"
if "$REPO/bin/submit_globus_batch.sh" "$T/config.sh" 2>/dev/null; then echo "stale marker was accepted" >&2; exit 1; fi
printf 'LOCAL_RESULTS=%s\nSOURCE_ROOT_LOCAL_VIEW=%s\n' "$T/results" "$T/results" > "$T/manager/state/path_check.passed"
if "$REPO/bin/submit_globus_batch.sh" "$T/config.sh" 2>/dev/null; then echo "missing source sample was accepted" >&2; exit 1; fi
assert test "$(find "$T/manager/manifests/transfer_batches" -type f | wc -l)" -eq 0

# Identical resolved paths inspect only ready samples, stop at the configured
# limit, and never checksum or traverse unrelated sample directories.
mkdir -p "$T/results"/{s1,s2,unrelated}/out
printf 'vcf' > "$T/results/s1/out/s1.round2.original_coords.clean.final.split.vcf.gz"
printf 'mtcn' > "$T/results/s1/out/s1.round2.mtcn.tsv"
printf 'vcf' > "$T/results/s2/out/s2.round2.original_coords.clean.final.split.vcf.gz"
printf 'vcf' > "$T/results/unrelated/out/unrelated.round2.original_coords.clean.final.split.vcf.gz"
awk -F '\t' -v OFS='\t' 'NR==1{print;next}$1~/^s[12]$/{$4="READY_TO_TRANSFER"}{print}' "$T/manager/state/sample_status.tsv" > "$T/x"; mv "$T/x" "$T/manager/state/sample_status.tsv"
sed -i 's/PATH_CHECK_MAX_FILES=5/PATH_CHECK_MAX_FILES=2/' "$T/config.sh"
sed -i 's/ENABLE_TRANSFER=1/ENABLE_TRANSFER=0/' "$T/config.sh"
mkdir -p "$T/path_alias"; ln -s "$T/results" "$T/path_alias/results"
sed -i "s|SOURCE_ROOT_LOCAL_VIEW=$T/results|SOURCE_ROOT_LOCAL_VIEW=$T/path_alias/results|" "$T/config.sh"
cat > "$T/mockbin/find" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$TEST_ROOT/find.calls"
exec /usr/bin/find "$@"
EOF
cat > "$T/mockbin/sha256sum" <<'EOF'
#!/usr/bin/env bash
echo 'sha256sum must not run in same-realpath mode' >&2
exit 99
EOF
chmod +x "$T/mockbin/find" "$T/mockbin/sha256sum"
path_log=$("$REPO/bin/check_paths.sh" "$T/config.sh" 2>&1)
assert grep -q '^checks_performed=same_realpath$' "$T/manager/state/path_check.passed"
assert grep -q 's1.round2.original_coords.clean.final.split.vcf.gz' "$T/manager/state/path_check.passed"
assert grep -q 's1.round2.mtcn.tsv' "$T/manager/state/path_check.passed"
assert test "$(wc -l < "$T/find.calls")" -eq 2
if grep -Eq '/(s2|unrelated)( |$)' "$T/find.calls"; then echo "search continued after the file limit or traversed an unrelated directory" >&2; exit 1; fi
assert test "$(grep -c '^file=' "$T/manager/state/path_check.passed")" -eq 2
assert grep -q 'both resolve to the same POSIX location' <<< "$path_log"

# Distinct resolved paths retain existence, size, and SHA-256 comparison.
mkdir -p "$T/source_view/s1/out"
cp "$T/results/s1/out/s1.round2.original_coords.clean.final.split.vcf.gz" "$T/source_view/s1/out/"
mkdir -p "$T/source_view/s2/out"
cp "$T/results/s2/out/s2.round2.original_coords.clean.final.split.vcf.gz" "$T/source_view/s2/out/"
sed -i "s|SOURCE_ROOT_LOCAL_VIEW=$T/path_alias/results|SOURCE_ROOT_LOCAL_VIEW=$T/source_view|" "$T/config.sh"
rm "$T/mockbin/sha256sum" "$T/mockbin/find"
"$REPO/bin/check_paths.sh" "$T/config.sh" >/dev/null
assert grep -q '^checks_performed=existence,size,sha256$' "$T/manager/state/path_check.passed"

# Every configured infrastructure directory is excluded from capacity counts.
rm -rf "$T/results/s2" "$T/results/unrelated"
mkdir -p "$T/results"/{sample_a,cache,logs,lost+found,numt_discovery,numt_besthit}
printf '\nLOCAL_RESULTS_EXCLUDE_DIRS="numt_discovery numt_besthit logs lost+found cache"\n' >> "$T/config.sh"
count=$(bash -c 'source "$1/lib/common.sh"; load_config "$2"; local_sample_dir_count' _ "$REPO" "$T/config.sh")
assert test "$count" -eq 2

# Assignment overlap is rejected, including when the files have TSV headers.
printf 'sample_id\tspecies\na\tsp\nb\tsp\n' > "$T/one.tsv"
printf 'sample_id\tspecies\nb\tsp\nc\tsp\n' > "$T/two.tsv"
if "$REPO/bin/check_cross_hpc_assignments.sh" "$T/one.tsv" "$T/two.tsv" 2>/dev/null; then echo "overlap was accepted" >&2; exit 1; fi
printf 'sample_id\tspecies\nd\tsp\n' > "$T/two.tsv"
assert "$REPO/bin/check_cross_hpc_assignments.sh" "$T/one.tsv" "$T/two.tsv"

# Disk thresholds must be monotonic.
sed -i 's/STOP_SUBMIT_PERCENT=100/STOP_SUBMIT_PERCENT=91/;s/FORCE_TRANSFER_PERCENT=100/FORCE_TRANSFER_PERCENT=90/' "$T/config.sh"
if bash -c 'source "$1/lib/common.sh"; load_config "$2"; validate_config' _ "$REPO" "$T/config.sh" 2>/dev/null; then echo "invalid thresholds were accepted" >&2; exit 1; fi
