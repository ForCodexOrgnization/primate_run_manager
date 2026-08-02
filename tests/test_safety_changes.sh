#!/usr/bin/env bash
source "$(dirname "$0")/test_helper.sh"

# Mixed array states keep the wave and every sample active.
new_env; "$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
awk -F '\t' -v OFS='\t' 'NR==1{print;next}$1~/^s[12]$/{$4="WAVE_SUBMITTED";$5="42";$6="mixed"}{print}' "$T/manager/state/sample_status.tsv" > "$T/x"; mv "$T/x" "$T/manager/state/sample_status.tsv"
printf 'mixed\tm\t2\t42\tnow\tRUNNING\t0\t0\tRUNNING\tnow\t\n' >> "$T/manager/state/wave_status.tsv"
cat > "$T/mockbin/sacct" <<'EOF'
#!/usr/bin/env bash
printf '42|COMPLETED|\n42_0|COMPLETED|\n42_0.batch|FAILED|\n42_1|RUNNING|\n42_1.extern|COMPLETED|\n'
EOF
chmod +x "$T/mockbin/sacct"; "$REPO/bin/update_wave_states.sh" "$T/config.sh"
assert awk -F '\t' '$1=="mixed"&&$9=="RUNNING"{ok=1}END{exit !ok}' "$T/manager/state/wave_status.tsv"
assert test "$(awk -F '\t' '$6=="mixed"&&$4~/^(WAVE_SUBMITTED|PIPELINE_RUNNING)$/{n++}END{print n+0}' "$T/manager/state/sample_status.tsv")" -eq 2

# The active transfer task limit blocks submission and honors configured sync level.
new_env; "$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
awk -F '\t' -v OFS='\t' 'NR==1{print;next}$1=="s1"{$4="READY_TO_TRANSFER"}{print}' "$T/manager/state/sample_status.tsv" > "$T/x"; mv "$T/x" "$T/manager/state/sample_status.tsv"
printf 'old1\tt1\tACTIVE\ta\tn\tn\tn\nold2\tt2\tACTIVE\ta\tn\tn\tn\n' >> "$T/manager/state/transfer_tasks.tsv"
sed -i 's/ENABLE_TRANSFER=0/ENABLE_TRANSFER=1/;s/DRY_RUN=0/DRY_RUN=1/;s/GLOBUS_SYNC_LEVEL=checksum/GLOBUS_SYNC_LEVEL=mtime/;s/TRANSFER_BATCH_SIZE=2/TRANSFER_BATCH_SIZE=1/' "$T/config.sh"
out=$("$REPO/bin/submit_globus_batch.sh" "$T/config.sh" 2>&1); assert test "$(find "$T/manager/manifests/transfer_batches" -type f | wc -l)" -eq 0
sed -i 's/MAX_ACTIVE_TRANSFER_TASKS=2/MAX_ACTIVE_TRANSFER_TASKS=3/' "$T/config.sh"; out=$("$REPO/bin/submit_globus_batch.sh" "$T/config.sh"); assert grep -q -- '--sync-level mtime' <<< "$out"

# Historical inventory reports unknown first-level directories and excludes infrastructure.
new_env; mkdir -p "$T/results"/{s1,mystery,logs,lost+found,numt_discovery,numt_besthit}
"$REPO/bin/import_existing_results.sh" "$T/config.sh" >/dev/null
assert grep -q $'^s1\t' "$T/manager/state/existing_local_samples.tsv"
assert grep -q $'^mystery\t' "$T/manager/state/unrecognized_local_directories.tsv"
assert test "$(wc -l < "$T/manager/state/unrecognized_local_directories.tsv")" -eq 2

# Cleanup never selects incomplete samples, and refuses an incomplete destination.
new_env; "$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null; s=s1; d="$T/results/$s/out"; mkdir -p "$d"
printf ok | gzip > "$d/$s.round2.original_coords.clean.final.split.vcf.gz"; printf c > "$d/$s.round2.original_coords.per_base_coverage.tsv"; printf n > "$d/$s.numt_decoy.clean.realigned.per_base_coverage.tsv"; printf m > "$d/$s.round2.mtcn.tsv"
sed -i 's/ENABLE_LOCAL_CLEANUP=0/ENABLE_LOCAL_CLEANUP=1/' "$T/config.sh"
cat > "$T/mockbin/globus" <<'EOF'
#!/usr/bin/env bash
printf 'alignment/s1.cram\n'
EOF
chmod +x "$T/mockbin/globus"; "$REPO/bin/cleanup_transferred_samples.sh" "$T/config.sh"; assert test -d "$T/results/s1"
awk -F '\t' -v OFS='\t' 'NR==1{print;next}$1=="s1"{$4="TRANSFERRED_FULL"}{print}' "$T/manager/state/sample_status.tsv" > "$T/x"; mv "$T/x" "$T/manager/state/sample_status.tsv"
"$REPO/bin/cleanup_transferred_samples.sh" "$T/config.sh"; assert test -d "$T/results/s1"
