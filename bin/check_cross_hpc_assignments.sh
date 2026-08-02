#!/usr/bin/env bash
set -euo pipefail
[[ $# -ge 2 ]] || { echo "Usage: $0 assigned_samples_1.tsv assigned_samples_2.tsv [assigned_samples_N.tsv ...]" >&2; exit 2; }
for file in "$@"; do [[ -s "$file" ]] || { echo "Missing or empty assignment file: $file" >&2; exit 2; }; done
awk -F '\t' '
  FNR == 1 { file_index++ }
  $1 == "" || $1 == "sample_id" { next }
  !seen_in_file[file_index, $1]++ {
    if ($1 in first_file && first_file[$1] != FILENAME) {
      printf "Sample %s is assigned in both %s and %s\n", $1, first_file[$1], FILENAME > "/dev/stderr"
      overlap=1
    } else first_file[$1]=FILENAME
  }
  END { exit overlap ? 1 : 0 }
' "$@"
