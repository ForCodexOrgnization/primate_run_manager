#!/usr/bin/env bash
set -euo pipefail

now_iso() { date -u +'%Y-%m-%dT%H:%M:%SZ'; }
log() { printf '[%s] %s\n' "$(now_iso)" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

state_header() { printf 'sample_id\tspecies\thpc\tstatus\tslurm_job_id\twave_id\tpipeline_attempts\tlast_pipeline_error\tglobus_task_id\tworkspace_path\ttransfer_status\tcleanup_status\tlast_update\tnotes\n'; }
wave_header() { printf 'wave_id\tsample_manifest\tsample_count\tpipeline_job_id\tsubmit_time\tslurm_state\tcomplete_count\tincomplete_count\tstatus\tlast_update\tnotes\n'; }
transfer_header() { printf 'batch_id\ttask_id\tstatus\tsample_file\tsubmit_time\tlast_update\tnotes\n'; }
validation_header() { printf 'sample_id\tcram_ok\tcrai_ok\tvcf_ok\tround2_coverage_ok\tnumt_coverage_ok\tmtcn_ok\toverall_complete\tscan_time\tnotes\n'; }

load_config() {
    local cfg="${1:-${RUN_MANAGER_CONFIG:-}}"
    [[ -n "$cfg" && -s "$cfg" ]] || die "Config missing or empty: ${cfg:-<unset>}"
    # shellcheck disable=SC1090
    source "$cfg"
    : "${MANAGER_ROOT:?}" "${HPC_NAME:?}" "${ASSIGNED_SAMPLE_LIST:?}"
    STATUS_FILE="${MANAGER_ROOT}/state/sample_status.tsv"
    WAVE_STATUS_FILE="${MANAGER_ROOT}/state/wave_status.tsv"
    TRANSFER_TASK_FILE="${MANAGER_ROOT}/state/transfer_tasks.tsv"
    VALIDATION_FILE="${MANAGER_ROOT}/state/output_validation.tsv"
    mkdir -p "${MANAGER_ROOT}"/{state/locks,state/receipts,manifests/pipeline_waves,manifests/transfer_batches,logs,samples}
}

validate_config() {
    local v
    [[ -x "$PIPELINE_LAUNCHER" ]] || die "PIPELINE_LAUNCHER missing or not executable: $PIPELINE_LAUNCHER"
    [[ -s "$ASSIGNED_SAMPLE_LIST" ]] || die "ASSIGNED_SAMPLE_LIST missing or empty: $ASSIGNED_SAMPLE_LIST"
    for v in LOCAL_RESULTS ANALYSIS_ROOT PIPELINE_WORK_ROOT; do
        [[ -n "${!v:-}" && "${!v}" != / ]] || die "$v must be non-empty and must not be /"
    done
    [[ "$LOCAL_RESULTS" != "$ANALYSIS_ROOT" ]] || die "LOCAL_RESULTS and ANALYSIS_ROOT must differ"
    [[ -n "${SOURCE_ROOT:-}" && -n "${DEST_ROOT:-}" ]] || die "SOURCE_ROOT and DEST_ROOT must be non-empty"
    for v in PIPELINE_WAVE_SIZE PIPELINE_BATCH_SIZE CHAIN_CONCURRENT_BATCHES NUMT_CONCURRENT MAX_ACTIVE_PIPELINE_WAVES MAX_PIPELINE_RETRIES AUTO_RETRY_IMPORTED_INCOMPLETE TRANSFER_BATCH_SIZE STOP_SUBMIT_PERCENT MAX_LOCAL_SAMPLE_DIRS CLEAN_ON_SUCCESS ENABLE_PIPELINE_SUBMIT ENABLE_TRANSFER ENABLE_LOCAL_CLEANUP DRY_RUN; do
        [[ "${!v:-}" =~ ^[0-9]+$ ]] || die "$v must be an integer"
    done
    if [[ "$ENABLE_PIPELINE_SUBMIT" == 1 ]]; then command -v sbatch >/dev/null || die "sbatch not found"; fi
    if [[ "$ENABLE_TRANSFER" == 1 ]]; then command -v globus >/dev/null || die "globus not found"; fi
}

with_state_lock() { local lock_file="${MANAGER_ROOT}/state/locks/state.lock"; exec 9>"$lock_file"; flock -x 9; "$@"; local rc=$?; flock -u 9; return "$rc"; }

migrate_state_file() {
    [[ -e "$STATUS_FILE" ]] || { state_header > "$STATUS_FILE"; return; }
    local fields backup tmp
    fields=$(awk -F '\t' 'NR==1{print NF}' "$STATUS_FILE")
    if [[ "$fields" == 14 ]]; then
        if awk -F '\t' 'NR>1&&$4=="PIPELINE_INCOMPLETE"{found=1} END{exit !found}' "$STATUS_FILE"; then
            backup="${STATUS_FILE}.bak.$(date -u +%Y%m%dT%H%M%SZ).$$"; cp -p "$STATUS_FILE" "$backup"; tmp="${STATUS_FILE}.tmp.$$"
            awk -F '\t' -v OFS='\t' 'NR>1&&$4=="PIPELINE_INCOMPLETE"{$4="PIPELINE_INCOMPLETE_REVIEW";$14=($14==""?"legacy incomplete requires review":$14"; legacy incomplete requires review")}{print}' "$STATUS_FILE" > "$tmp"
            mv "$tmp" "$STATUS_FILE"; log "Conservatively migrated legacy incomplete states; backup: $backup"
        fi
        return
    fi
    [[ "$fields" == 10 ]] || die "Unsupported sample state schema ($fields columns): $STATUS_FILE"
    backup="${STATUS_FILE}.bak.$(date -u +%Y%m%dT%H%M%SZ).$$"; cp -p "$STATUS_FILE" "$backup"
    tmp="${STATUS_FILE}.tmp.$$"
    { state_header; awk -F '\t' -v OFS='\t' 'NR>1 { attempts=($4=="PENDING"?0:1); status=$4; if(status=="SUBMITTED")status="WAVE_SUBMITTED"; if(status=="PIPELINE_INCOMPLETE")status="PIPELINE_INCOMPLETE_REVIEW"; print $1,$2,$3,status,$5,$6,attempts,"",$7,$8,"","",$9,$10 }' "$STATUS_FILE"; } > "$tmp"
    mv "$tmp" "$STATUS_FILE"; log "Migrated state; backup: $backup"
}

ensure_wave_state_file() { [[ -e "$WAVE_STATUS_FILE" ]] || wave_header > "$WAVE_STATUS_FILE"; }
ensure_state_files() {
    with_state_lock migrate_state_file
    [[ -e "$TRANSFER_TASK_FILE" ]] || transfer_header > "$TRANSFER_TASK_FILE"
    [[ -e "$VALIDATION_FILE" ]] || validation_header > "$VALIDATION_FILE"
    ensure_wave_state_file
}

# update_sample_fields SAMPLE field=value ...; callers must hold state lock.
update_sample_fields() {
    local sample="$1"; shift; local tmp="${STATUS_FILE}.tmp.$$" spec="" item
    for item in "$@"; do spec+="${item}"$'\034'; done
    awk -F '\t' -v OFS='\t' -v s="$sample" -v spec="$spec" -v ts="$(now_iso)" '
      BEGIN {n=split(spec,a,"\034"); for(i=1;i<=n;i++){p=index(a[i],"="); if(p){k=substr(a[i],1,p-1);v[k]=substr(a[i],p+1)}}}
      NR==1 {for(i=1;i<=NF;i++) h[$i]=i; print; next}
      $1==s {for(k in v) if(h[k]) $h[k]=v[k]; $h["last_update"]=ts} {print}' "$STATUS_FILE" > "$tmp"
    mv "$tmp" "$STATUS_FILE"
}

append_wave_row() { local tmp="${WAVE_STATUS_FILE}.tmp.$$"; cat "$WAVE_STATUS_FILE" > "$tmp"; printf '%b\n' "$1" >> "$tmp"; mv "$tmp" "$WAVE_STATUS_FILE"; }
update_wave_row() {
    local wave="$1"; shift; local tmp="${WAVE_STATUS_FILE}.tmp.$$" spec="" item
    for item in "$@"; do spec+="${item}"$'\034'; done
    awk -F '\t' -v OFS='\t' -v w="$wave" -v spec="$spec" -v ts="$(now_iso)" 'BEGIN{n=split(spec,a,"\034");for(i=1;i<=n;i++){p=index(a[i],"=");if(p)v[substr(a[i],1,p-1)]=substr(a[i],p+1)}} NR==1{for(i=1;i<=NF;i++)h[$i]=i;print;next} $1==w{for(k in v)if(h[k])$h[k]=v[k];$h["last_update"]=ts}{print}' "$WAVE_STATUS_FILE" > "$tmp"; mv "$tmp" "$WAVE_STATUS_FILE"
}
active_wave_count() { awk -F '\t' 'NR>1 && $9 ~ /^(CREATED|SUBMITTED|RUNNING)$/ {n++} END{print n+0}' "$WAVE_STATUS_FILE"; }
samples_in_wave() { local wave="${1:-}"; awk -F '\t' -v w="$wave" 'NR>1 && $6!="" && (w==""||$6==w) && $4 ~ /^(WAVE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_RETRY_RUNNING)$/ {print $1}' "$STATUS_FILE"; }
wave_is_active() { local w="$1"; awk -F '\t' -v w="$w" 'NR>1&&$1==w&&$9~/^(CREATED|SUBMITTED|RUNNING)$/{ok=1} END{exit !ok}' "$WAVE_STATUS_FILE"; }
get_samples_by_status() { local regex="$1"; awk -F '\t' -v r="$regex" 'NR>1 && $4 ~ r {print $1}' "$STATUS_FILE"; }
sample_species() { local sample="$1"; awk -F '\t' -v s="$sample" 'NR>1&&$1==s{print $2;exit}' "$STATUS_FILE"; }
disk_used_percent() { df -P "$DISK_CHECK_PATH" | awk 'NR==2{gsub(/%/,"",$5);print $5}'; }
local_sample_dir_count() { [[ -d "$LOCAL_RESULTS" ]] || { echo 0; return; }; find "$LOCAL_RESULTS" -mindepth 1 -maxdepth 1 -type d ! -name numt_discovery ! -name numt_besthit | wc -l | tr -d '[:space:]'; }
find_exact_one() { find "$1" -type f -name "$2" -print -quit 2>/dev/null || true; }
require_nonempty() { [[ -n "${1:-}" && -s "$1" ]]; }
