#!/usr/bin/env bash
# Verify that the pipeline-visible and Globus-visible POSIX paths expose the
# same result bytes.  This check deliberately does not assume path aliasing.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "${SCRIPT_DIR}/../lib/common.sh"
load_config "${1:-}"
[[ -d "$LOCAL_RESULTS" ]] || die "LOCAL_RESULTS does not exist: $LOCAL_RESULTS"
[[ -n "${SOURCE_ROOT_LOCAL_VIEW:-}" ]] || die "SOURCE_ROOT_LOCAL_VIEW is not configured"
[[ -d "$SOURCE_ROOT_LOCAL_VIEW" ]] || die "SOURCE_ROOT_LOCAL_VIEW does not exist: $SOURCE_ROOT_LOCAL_VIEW"

checked=0; mismatches=0
while IFS=$'\t' read -r sample _; do
  [[ "$sample" == sample_id || -z "$sample" || ! -d "$LOCAL_RESULTS/$sample" ]] && continue
  while IFS= read -r -d '' local_file; do
    rel=${local_file#"$LOCAL_RESULTS"/}; visible_file="$SOURCE_ROOT_LOCAL_VIEW/$rel"
    if [[ ! -f "$visible_file" ]]; then log "Missing from SOURCE_ROOT_LOCAL_VIEW: $rel"; mismatches=$((mismatches+1))
    elif [[ $(stat -c %s "$local_file") != $(stat -c %s "$visible_file") ]]; then log "Size mismatch: $rel"; mismatches=$((mismatches+1))
    elif [[ $(sha256sum "$local_file" | awk '{print $1}') != $(sha256sum "$visible_file" | awk '{print $1}') ]]; then log "Checksum mismatch: $rel"; mismatches=$((mismatches+1))
    else log "Matched: $rel"; fi
    checked=$((checked+1)); (( checked >= 3 )) && break 2
  done < <(find "$LOCAL_RESULTS/$sample" -type f \( -name "$sample.cram" -o -name "$sample.round2.original_coords.clean.final.split.vcf.gz" -o -name "$sample.round2.mtcn.tsv" \) -print0)
done < "$ASSIGNED_SAMPLE_LIST"
(( checked > 0 )) || die "No recognized sample files available to compare"
(( mismatches == 0 )) || die "$mismatches of $checked compared files differ between deployment paths"
log "Deployment paths agree for $checked recognized sample files"
