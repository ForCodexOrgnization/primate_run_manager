#!/usr/bin/env bash
set -euo pipefail
[[ $# -eq 3 ]] || { echo "Usage: $0 all_samples.txt hpc1.txt hpc2.txt" >&2; exit 1; }
awk 'NR%2==1' "$1" > "$2"
awk 'NR%2==0' "$1" > "$3"
echo "HPC1: $(wc -l < "$2") samples"
echo "HPC2: $(wc -l < "$3") samples"
