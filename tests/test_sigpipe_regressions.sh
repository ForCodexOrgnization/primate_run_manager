#!/usr/bin/env bash
set -euo pipefail

REPO=$(cd "$(dirname "$0")/.." && pwd)
# shellcheck source=../lib/common.sh
source "$REPO/lib/common.sh"

# Reproduce Bouchet's ordering with enough trailing output to fill a pipe.  The
# parser must consume it all, preserve the first value, and return successfully.
scontrol() {
    [[ "$*" == "show config" ]]
    printf 'ClusterName = test\nMaxArraySize = 100001\nSchedulerType = sched/backfill\n'
    local i
    for ((i=0; i<10000; i++)); do
        printf 'TrailingConfig%05d = %0100d\n' "$i" "$i"
    done
}
max=$(scontrol show config | parse_slurm_max_array_size)
[[ "$max" == 100001 ]]

# Bounded READY_TO_TRANSFER selection reads the complete file rather than
# closing a status producer after the requested number of rows.
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
STATUS_FILE="$tmp/sample_status.tsv"
state_header > "$STATUS_FILE"
for ((i=1; i<=1000; i++)); do
    printf 'sample%04d\tsp\thpc\tREADY_TO_TRANSFER\t\t\t0\t\t\t\t\t\t\t\n' "$i"
done >> "$STATUS_FILE"
mapfile -t selected < <(get_samples_by_status_limit '^READY_TO_TRANSFER$' 25)
[[ ${#selected[@]} -eq 25 ]]
[[ "${selected[0]}" == sample0001 ]]
[[ "${selected[24]}" == sample0025 ]]

printf 'SIGPIPE regression tests passed.\n'
