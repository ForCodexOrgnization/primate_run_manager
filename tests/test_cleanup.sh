#!/usr/bin/env bash
source "$(dirname "$0")/test_helper.sh"; new_env
"$REPO/bin/initialize_samples.sh" "$T/config.sh" >/dev/null
mkdir -p "$T/results/s1/out"; touch "$T/results/s1/keep"
"$REPO/bin/cleanup_transferred_samples.sh" "$T/config.sh" >/dev/null; assert test -e "$T/results/s1/keep"
s=s1; d="$T/results/$s/out"; printf ok | gzip > "$d/$s.round2.original_coords.clean.final.split.vcf.gz"; printf i > "$d/$s.round2.original_coords.clean.final.split.vcf.gz.tbi"; printf c > "$d/$s.round2.original_coords.per_base_coverage.tsv"; printf n > "$d/$s.numt_decoy.clean.realigned.per_base_coverage.tsv"; printf m > "$d/$s.round2.mtcn.tsv"
awk -F '\t' -v OFS='\t' 'NR==1{print;next}$1=="s1"{$4="TRANSFERRED_FULL";$9="task1";$11="SUCCEEDED"}{print}' "$T/manager/state/sample_status.tsv" > "$T/x"; mv "$T/x" "$T/manager/state/sample_status.tsv"
cat > "$T/mockbin/globus" <<'G'
#!/usr/bin/env bash
[[ "$1" == ls ]] && exit 0
exit 1
G
chmod +x "$T/mockbin/globus"; sed -i 's/ENABLE_LOCAL_CLEANUP=0/ENABLE_LOCAL_CLEANUP=1/' "$T/config.sh"
"$REPO/bin/cleanup_transferred_samples.sh" "$T/config.sh"
assert test ! -d "$T/results/s1"; assert test -s "$T/analysis/vcf/$s.round2.original_coords.clean.final.split.vcf.gz"; assert test -s "$T/analysis/vcf/$s.round2.original_coords.clean.final.split.vcf.gz.tbi"; assert test -s "$T/analysis/round2_coverage/$s.round2.original_coords.per_base_coverage.tsv"; assert test -s "$T/analysis/numt_decoy_coverage/$s.numt_decoy.clean.realigned.per_base_coverage.tsv"; assert test -s "$T/analysis/mtcn/$s.round2.mtcn.tsv"
# Unsafe roots are rejected.
sed -i "s|LOCAL_RESULTS=$T/results|LOCAL_RESULTS=/|" "$T/config.sh"; if bash -c "source '$REPO/lib/common.sh'; load_config '$T/config.sh'; validate_config" 2>/dev/null; then exit 1; fi
