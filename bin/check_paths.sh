#!/usr/bin/env bash
# Verify that the pipeline-visible and Globus-visible POSIX paths expose the
# same result bytes. This check deliberately does not assume path aliasing.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"
marker="$MANAGER_ROOT/state/path_check.passed"; rm -f "$marker"
validate_config
[[ -d "$LOCAL_RESULTS" ]] || die "LOCAL_RESULTS does not exist: $LOCAL_RESULTS"
[[ -d "$SOURCE_ROOT_LOCAL_VIEW" ]] || die "SOURCE_ROOT_LOCAL_VIEW does not exist: $SOURCE_ROOT_LOCAL_VIEW"

local_results_realpath=$(realpath "$LOCAL_RESULTS")
source_view_realpath=$(realpath "$SOURCE_ROOT_LOCAL_VIEW")

checked=0; mismatches=0; checked_files=()
patterns=(
  '*.round2.original_coords.clean.final.split.vcf.gz'
  '*.round2.mtcn.tsv'
  '*.round2.original_coords.per_base_coverage.tsv'
  '*.numt_decoy.clean.realigned.per_base_coverage.tsv'
)
[[ "$PATH_CHECK_INCLUDE_CRAM" == 1 ]] && patterns+=('*.cram')

if [[ "$local_results_realpath" == "$source_view_realpath" ]]; then
  for pattern in "${patterns[@]}"; do
    while IFS= read -r -d '' local_file; do
      checked_files+=("${local_file#"$LOCAL_RESULTS"/}")
      checked=$((checked+1)); (( checked >= PATH_CHECK_MAX_FILES )) && break 2
    done < <(find "$LOCAL_RESULTS" -type f -name "$pattern" -print0 | sort -z)
  done
  (( checked > 0 )) || die "No recognized sample files available in shared deployment path: $local_results_realpath"
  tmp="${marker}.tmp.$$"
  {
    printf 'timestamp=%s\nLOCAL_RESULTS=%s\nSOURCE_ROOT_LOCAL_VIEW=%s\nfiles_checked=%s\n' "$(now_iso)" "$LOCAL_RESULTS" "$SOURCE_ROOT_LOCAL_VIEW" "$checked"
    printf 'checks_performed=same_realpath\n'
    printf 'file=%s\n' "${checked_files[@]}"
  } > "$tmp"
  mv "$tmp" "$marker"
  log "LOCAL_RESULTS and SOURCE_ROOT_LOCAL_VIEW both resolve to the same POSIX location: $local_results_realpath; found $checked recognized sample files and wrote $marker"
  exit 0
fi

for pattern in "${patterns[@]}"; do
  while IFS= read -r -d '' local_file; do
    rel=${local_file#"$LOCAL_RESULTS"/}; visible_file="$SOURCE_ROOT_LOCAL_VIEW/$rel"
    checked_files+=("$rel")
    if [[ ! -f "$visible_file" ]]; then log "Missing from SOURCE_ROOT_LOCAL_VIEW: $rel"; mismatches=$((mismatches+1))
    elif [[ $(stat -c %s "$local_file") != $(stat -c %s "$visible_file") ]]; then log "Size mismatch: $rel"; mismatches=$((mismatches+1))
    elif [[ $(sha256sum "$local_file" | awk '{print $1}') != $(sha256sum "$visible_file" | awk '{print $1}') ]]; then log "Checksum mismatch: $rel"; mismatches=$((mismatches+1))
    else log "Matched: $rel"; fi
    checked=$((checked+1)); (( checked >= PATH_CHECK_MAX_FILES )) && break 2
  done < <(find "$LOCAL_RESULTS" -type f -name "$pattern" -print0 | sort -z)
done
(( checked > 0 )) || die "No recognized sample files available to compare"
(( mismatches == 0 )) || die "$mismatches of $checked compared files differ between deployment paths"
tmp="${marker}.tmp.$$"
{
  printf 'timestamp=%s\nLOCAL_RESULTS=%s\nSOURCE_ROOT_LOCAL_VIEW=%s\nfiles_checked=%s\n' "$(now_iso)" "$LOCAL_RESULTS" "$SOURCE_ROOT_LOCAL_VIEW" "$checked"
  printf 'checks_performed=existence,size,sha256\n'
  printf 'file=%s\n' "${checked_files[@]}"
} > "$tmp"
mv "$tmp" "$marker"
log "Deployment paths agree for $checked recognized sample files; wrote $marker"
