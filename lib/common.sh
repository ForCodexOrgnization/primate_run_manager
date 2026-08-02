#!/usr/bin/env bash
set -euo pipefail

now_iso() { date -u +'%Y-%m-%dT%H:%M:%SZ'; }
log() { printf '[%s] %s\n' "$(now_iso)" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

load_config() {
    local cfg="${1:-${RUN_MANAGER_CONFIG:-}}"
    [[ -n "$cfg" ]] || die "Pass a config file or set RUN_MANAGER_CONFIG"
    [[ -s "$cfg" ]] || die "Config missing or empty: $cfg"
    # shellcheck disable=SC1090
    source "$cfg"
    : "${MANAGER_ROOT:?}" "${HPC_NAME:?}" "${ASSIGNED_SAMPLE_LIST:?}"
    STATUS_FILE="${MANAGER_ROOT}/state/sample_status.tsv"
    TRANSFER_TASK_FILE="${MANAGER_ROOT}/state/transfer_tasks.tsv"
    mkdir -p "${MANAGER_ROOT}"/{state/locks,state/receipts,manifests/pipeline_batches,manifests/transfer_batches,logs,samples}
}

with_state_lock() {
    local lock_file="${MANAGER_ROOT}/state/locks/state.lock"
    exec 9>"$lock_file"
    flock -x 9
    "$@"
    flock -u 9
}

state_header() {
    printf 'sample_id\tspecies\thpc\tstatus\tslurm_job_id\tpipeline_batch\tglobus_task_id\tworkspace_path\tlast_update\tnotes\n'
}

transfer_header() {
    printf 'batch_id\ttask_id\tstatus\tsample_file\tsubmit_time\tlast_update\tnotes\n'
}

ensure_state_files() {
    [[ -e "$STATUS_FILE" ]] || state_header > "$STATUS_FILE"
    [[ -e "$TRANSFER_TASK_FILE" ]] || transfer_header > "$TRANSFER_TASK_FILE"
}

update_sample_row() {
    local sample="$1" status="$2" job_id="${3:-}" pbatch="${4:-}" task_id="${5:-}" workspace="${6:-}" notes="${7:-}"
    local tmp="${STATUS_FILE}.tmp.$$"
    awk -F '\t' -v OFS='\t' -v s="$sample" -v st="$status" -v j="$job_id" -v pb="$pbatch" -v t="$task_id" -v w="$workspace" -v ts="$(now_iso)" -v n="$notes" '
        NR==1 {print; next}
        $1==s {
            $4=st
            if (j!="") $5=j
            if (pb!="") $6=pb
            if (t!="") $7=t
            if (w!="") $8=w
            $9=ts
            if (n!="") $10=n
        }
        {print}
    ' "$STATUS_FILE" > "$tmp"
    mv "$tmp" "$STATUS_FILE"
}

get_samples_by_status() {
    local regex="$1"
    awk -F '\t' -v r="$regex" 'NR>1 && $4 ~ r {print $1}' "$STATUS_FILE"
}

sample_species() {
    local sample="$1"
    awk -F '\t' -v s="$sample" 'NR>1 && $1==s {print $2; exit}' "$STATUS_FILE"
}

disk_used_percent() {
    df -P "$DISK_CHECK_PATH" | awk 'NR==2 {gsub(/%/,"",$5); print $5}'
}

local_sample_dir_count() {
    find "$LOCAL_RESULTS" -mindepth 1 -maxdepth 1 -type d \
      ! -name numt_discovery ! -name numt_besthit | wc -l | tr -d '[:space:]'
}

find_exact_one() {
    local root="$1" name="$2"
    find "$root" -type f -name "$name" -print -quit 2>/dev/null || true
}

require_nonempty() {
    [[ -n "${1:-}" && -s "$1" ]]
}
