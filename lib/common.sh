#!/usr/bin/env bash
set -euo pipefail

now_iso() { date -u +'%Y-%m-%dT%H:%M:%SZ'; }
log() { printf '[%s] %s\n' "$(now_iso)" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

load_globus_module() {
    command -v globus >/dev/null 2>&1 && return 0

    if [[ -n "${GLOBUS_MODULE:-}" ]] && command -v module >/dev/null 2>&1; then
        if module load "$GLOBUS_MODULE"; then
            log "Loaded Globus module: $GLOBUS_MODULE"
        fi
    fi

    command -v globus >/dev/null 2>&1 ||
        die "Globus CLI not found. Configure GLOBUS_MODULE or install globus-cli."
}

state_header() { printf 'sample_id\tspecies\thpc\tstatus\tslurm_job_id\twave_id\tpipeline_attempts\tlast_pipeline_error\tglobus_task_id\tworkspace_path\ttransfer_status\tcleanup_status\tlast_update\tnotes\n'; }
wave_header() { printf 'wave_id\tsample_manifest\tsample_count\tpipeline_job_id\tsubmit_time\tslurm_state\tcomplete_count\tincomplete_count\tstatus\tlast_update\tnotes\twork_root\tretry_of_wave_id\toriginal_wave_id\tfailure_class\tresume_eligible\tpipeline_manifest_sha256\tpipeline_config_sha256\tpipeline_git_commit\tbatch_lists_dir\tbatch_manifest_sha256'; [[ "${PIPELINE_MODE:-legacy_batch}" == streaming_per_sample ]] && printf '\twork_layout'; printf '\n'; }
transfer_header() { printf 'batch_id\ttask_id\tstatus\tsample_file\tsubmit_time\tlast_update\tnotes\n'; }
validation_header() { printf 'sample_id\tcram_ok\tcrai_ok\tvcf_ok\tround2_coverage_ok\tnumt_coverage_ok\tmtcn_ok\toverall_complete\tscan_time\tnotes\tcram_size\tcram_mtime\tcrai_size\tcrai_mtime\tvcf_size\tvcf_mtime\tround2_coverage_size\tround2_coverage_mtime\tnumt_coverage_size\tnumt_coverage_mtime\tmtcn_size\tmtcn_mtime\n'; }

load_config() {
    local cfg="${1:-${RUN_MANAGER_CONFIG:-}}"
    [[ -n "$cfg" && -s "$cfg" ]] || die "Config missing or empty: ${cfg:-<unset>}"
    # shellcheck disable=SC1090
    source "$cfg"
    : "${MANAGER_ROOT:?}" "${HPC_NAME:?}" "${ASSIGNED_SAMPLE_LIST:?}"
    : "${REQUIRE_SLURM_FOR_EXISTING_IMPORT:=1}"
    : "${ALLOW_INTERACTIVE_IMPORT:=0}"
    : "${SAMTOOLS_QUICKCHECK_TIMEOUT_SECONDS:=600}"
    : "${ENABLE_FULL_SCAN_IN_MANAGER_CYCLE:=0}"
    : "${ENABLE_INCREMENTAL_SCAN_IN_MANAGER_CYCLE:=1}"
    : "${REQUIRE_SLURM_FOR_FULL_SCAN:=1}"
    : "${ALLOW_INTERACTIVE_FULL_SCAN:=0}"
    : "${ENABLE_INFRASTRUCTURE_RESUME:=1}"
    : "${RESUME_ELIGIBLE_SLURM_STATES:=TIMEOUT PREEMPTED NODE_FAIL BOOT_FAIL}"
    : "${REQUIRE_RESUME_FINGERPRINT_MATCH:=1}"
    : "${PIPELINE_QUEUE_POLICY:=new_first}" "${DEFER_RETRY_UNTIL_PENDING_EMPTY:=1}"
    : "${ENABLE_DEFERRED_RETRY:=1}" "${MAX_DEFERRED_RETRIES:=2}"
    : "${CLEAN_TERMINAL_DEFERRED_WORK:=1}" "${ARCHIVE_FAILURE_DIAGNOSTICS:=1}"
    : "${WORK_DISK_CHECK_PATH:=$PIPELINE_WORK_ROOT}" "${WORK_STOP_SUBMIT_PERCENT:=75}"
    : "${WORK_EMERGENCY_CLEAN_PERCENT:=82}" "${WORK_CRITICAL_PERCENT:=90}"
    : "${WORK_ARRAY_RELEASE_PERCENT:=$WORK_STOP_SUBMIT_PERCENT}"
    : "${DISK_PRESSURE_POLL_SECONDS:=60}"
    : "${ENABLE_ORPHAN_WORK_CLEANUP:=1}" "${ORPHAN_WORK_RETENTION_HOURS:=48}"
    : "${MAX_DIAGNOSTIC_LOG_LINES:=5000}" "${MAX_DIAGNOSTIC_FILE_BYTES:=10485760}"
    : "${PIPELINE_MODE:=batch}" "${SAMPLE_CHAIN_CONCURRENCY:=10}" "${STREAMING_SUBMISSION_WINDOW:=0}"
    : "${IMMEDIATE_SAMPLE_RETRIES:=1}" "${IMMEDIATE_RETRY_DELAY_SECONDS:=60}"
    : "${CLEAN_VALIDATED_STAGE_WORK:=1}" "${REMOVE_SAMPLE_ROOT_ON_SUCCESS:=1}"
    : "${FAILED_CACHE_CLEAN_TRIGGER_PERCENT:=70}" "${FAILED_CACHE_CLEAN_TARGET_PERCENT:=65}"
    : "${STREAM_PARTITION:=day}"
    : "${STREAM_SMOKE_TEST:=0}"
    case "$PIPELINE_MODE" in
      legacy_batch) PIPELINE_MODE=batch ;;
      streaming_per_sample|batch) ;;
      *) die "PIPELINE_MODE must be streaming_per_sample or batch (legacy_batch is an alias)" ;;
    esac
    if [[ -n "${PIPELINE_LAUNCHER:-}" ]]; then
        : "${STREAMING_PIPELINE_LAUNCHER:=$PIPELINE_LAUNCHER}"
        : "${BATCH_PIPELINE_LAUNCHER:=$PIPELINE_LAUNCHER}"
    else
        : "${STREAMING_PIPELINE_LAUNCHER:=${PIPELINE_REPO:-}/launch_pipeline_streaming_per_sample.sh}"
        : "${BATCH_PIPELINE_LAUNCHER:=${PIPELINE_REPO:-}/launch_pipeline_all_per_batch.sh}"
    fi
    if [[ "$PIPELINE_MODE" == streaming_per_sample ]]; then
        PIPELINE_LAUNCHER="$STREAMING_PIPELINE_LAUNCHER"
    else
        PIPELINE_LAUNCHER="$BATCH_PIPELINE_LAUNCHER"
    fi
    STATUS_FILE="${MANAGER_ROOT}/state/sample_status.tsv"
    WAVE_STATUS_FILE="${MANAGER_ROOT}/state/wave_status.tsv"
    TRANSFER_TASK_FILE="${MANAGER_ROOT}/state/transfer_tasks.tsv"
    VALIDATION_FILE="${MANAGER_ROOT}/state/output_validation.tsv"
    MANAGER_PHASE_FILE="${MANAGER_ROOT}/state/manager_phase.tsv"
    WAVE_FAILURE_FILE="${MANAGER_ROOT}/state/wave_failures.tsv"
    GLOBUS_HEALTH_FILE="${MANAGER_ROOT}/state/globus_health.tsv"
    MANAGER_CYCLE_STATUS_FILE="${MANAGER_ROOT}/state/manager_cycle_status.tsv"
    MANAGER_RUNTIME_ROOT="${MANAGER_RUNTIME_ROOT:-${RUNTIME_ROOT:-$MANAGER_ROOT}}"
    RUNTIME_LOG_DIR="${RUNTIME_LOG_DIR:-${MANAGER_RUNTIME_ROOT}/logs}"
    mkdir -p "${MANAGER_ROOT}"/{state/locks,state/array_sample_map,state/submission_task_map,state/receipts/deferred_wave_work_cleanup,state/receipts/failed_sample_work_cleanup,state/receipts/stale_sample_work_cleanup,state/receipts/sample_scope_reconciliation,state/failure_diagnostics/samples,manifests/pipeline_waves,manifests/submissions,manifests/transfer_batches,logs,samples} "$RUNTIME_LOG_DIR"
    [[ "$PIPELINE_MODE" != streaming_per_sample ]] || mkdir -p "$PIPELINE_WORK_ROOT"/{.sample_state,.locks,.manifests}
}

record_globus_health() {
    local health="$1" operation="$2" rc="${3:-0}" detail="${4:-}" tmp="${GLOBUS_HEALTH_FILE}.tmp.$$"
    detail=${detail//$'\n'/; }; detail=${detail//$'\t'/ }
    printf 'health\toperation\texit_code\tchecked_at\tdetail\n%s\t%s\t%s\t%s\t%s\n' \
      "$health" "$operation" "$rc" "$(now_iso)" "$detail" > "$tmp"
    mv "$tmp" "$GLOBUS_HEALTH_FILE"
}

validate_config() {
    local v
    [[ -s "$ASSIGNED_SAMPLE_LIST" ]] || die "ASSIGNED_SAMPLE_LIST missing or empty: $ASSIGNED_SAMPLE_LIST"
    for v in LOCAL_RESULTS ANALYSIS_ROOT PIPELINE_WORK_ROOT SOURCE_ROOT_LOCAL_VIEW; do
        [[ -n "${!v:-}" && "${!v}" != / ]] || die "$v must be non-empty and must not be /"
    done
    [[ "$LOCAL_RESULTS" != "$ANALYSIS_ROOT" ]] || die "LOCAL_RESULTS and ANALYSIS_ROOT must differ"
    [[ -n "${SOURCE_ROOT:-}" && -n "${DEST_ROOT:-}" ]] || die "SOURCE_ROOT and DEST_ROOT must be non-empty"
    if [[ "$PIPELINE_MODE" == streaming_per_sample ]]; then
        [[ -n "${PIPELINE_REPO:-}" ]] || die "PIPELINE_REPO is required for PIPELINE_MODE=streaming_per_sample"
        [[ -d "$PIPELINE_REPO" ]] || die "PIPELINE_REPO directory does not exist: $PIPELINE_REPO"
        local workflow
        for workflow in \
            launch_pipeline_streaming_per_sample.sh \
            preprocessing.nf \
            numt_detection/numt_end2end.nf \
            primate_pipeline_numt_decoy_round1.nf \
            primate_pipeline_round2_consensus_NUMT.nf; do
            [[ -s "$PIPELINE_REPO/$workflow" ]] ||
                die "PIPELINE_REPO missing required workflow: $PIPELINE_REPO/$workflow"
        done
        for v in GLOBAL_REF_DIR REF_DIR NUCLEAR_ONLY_REF_DIR; do
            [[ -n "${!v:-}" ]] || die "$v is required for PIPELINE_MODE=streaming_per_sample"
            [[ -d "${!v}" ]] || die "$v directory does not exist: ${!v}"
        done
        [[ -n "${NEXTFLOW_MODULE:-}" ]] || command -v nextflow >/dev/null 2>&1 ||
            die "NEXTFLOW_MODULE is required unless nextflow is already available"
        [[ -n "${SAMTOOLS_MODULE:-}" ]] || command -v samtools >/dev/null 2>&1 ||
            die "SAMTOOLS_MODULE is required unless samtools is already available"
    else
        [[ -n "${PIPELINE_REPO:-}" && -d "$PIPELINE_REPO" ]] || die "PIPELINE_REPO directory does not exist: ${PIPELINE_REPO:-<unset>}"
        [[ -s "$BATCH_PIPELINE_LAUNCHER" ]] || die "batch launcher missing: $BATCH_PIPELINE_LAUNCHER"
    fi
    [[ -x "$PIPELINE_LAUNCHER" ]] || die "PIPELINE_LAUNCHER missing or not executable: $PIPELINE_LAUNCHER"
    for v in PIPELINE_WAVE_SIZE SAMPLE_CHAIN_CONCURRENCY STREAMING_SUBMISSION_WINDOW IMMEDIATE_SAMPLE_RETRIES IMMEDIATE_RETRY_DELAY_SECONDS CLEAN_VALIDATED_STAGE_WORK REMOVE_SAMPLE_ROOT_ON_SUCCESS STREAM_SMOKE_TEST FAILED_CACHE_CLEAN_TRIGGER_PERCENT FAILED_CACHE_CLEAN_TARGET_PERCENT MAX_ACTIVE_PIPELINE_WAVES MAX_PIPELINE_RETRIES AUTO_RETRY_IMPORTED_INCOMPLETE TRANSFER_BATCH_SIZE MAX_ACTIVE_TRANSFER_TASKS STOP_SUBMIT_PERCENT FORCE_TRANSFER_PERCENT EMERGENCY_PERCENT MAX_LOCAL_SAMPLE_DIRS CLEAN_ON_SUCCESS ENABLE_PIPELINE_SUBMIT ENABLE_TRANSFER ENABLE_LOCAL_CLEANUP DRY_RUN PATH_CHECK_REQUIRED PATH_CHECK_INCLUDE_CRAM PATH_CHECK_MAX_FILES; do
        [[ "${!v:-}" =~ ^[0-9]+$ ]] || die "$v must be an integer"
    done
    if [[ "$PIPELINE_MODE" == batch ]]; then
      for v in PIPELINE_BATCH_SIZE CHAIN_CONCURRENT_BATCHES NUMT_CONCURRENT; do [[ "${!v:-}" =~ ^[0-9]+$ ]] || die "$v must be an integer"; done
    fi
    for v in REQUIRE_SLURM_FOR_EXISTING_IMPORT ALLOW_INTERACTIVE_IMPORT SAMTOOLS_QUICKCHECK_TIMEOUT_SECONDS ENABLE_FULL_SCAN_IN_MANAGER_CYCLE ENABLE_INCREMENTAL_SCAN_IN_MANAGER_CYCLE REQUIRE_SLURM_FOR_FULL_SCAN ALLOW_INTERACTIVE_FULL_SCAN ENABLE_INFRASTRUCTURE_RESUME REQUIRE_RESUME_FINGERPRINT_MATCH ENABLE_DEFERRED_RETRY MAX_DEFERRED_RETRIES CLEAN_TERMINAL_DEFERRED_WORK ARCHIVE_FAILURE_DIAGNOSTICS WORK_STOP_SUBMIT_PERCENT WORK_EMERGENCY_CLEAN_PERCENT WORK_CRITICAL_PERCENT WORK_ARRAY_RELEASE_PERCENT DISK_PRESSURE_POLL_SECONDS ENABLE_ORPHAN_WORK_CLEANUP ORPHAN_WORK_RETENTION_HOURS MAX_DIAGNOSTIC_LOG_LINES MAX_DIAGNOSTIC_FILE_BYTES; do
        [[ "${!v:-}" =~ ^[0-9]+$ ]] || die "$v must be an integer"
    done
    for v in REQUIRE_SLURM_FOR_EXISTING_IMPORT ALLOW_INTERACTIVE_IMPORT ENABLE_FULL_SCAN_IN_MANAGER_CYCLE ENABLE_INCREMENTAL_SCAN_IN_MANAGER_CYCLE REQUIRE_SLURM_FOR_FULL_SCAN ALLOW_INTERACTIVE_FULL_SCAN ENABLE_INFRASTRUCTURE_RESUME REQUIRE_RESUME_FINGERPRINT_MATCH; do
        [[ "${!v}" == 0 || "${!v}" == 1 ]] || die "$v must be 0 or 1"
    done
    for v in ENABLE_PIPELINE_SUBMIT ENABLE_TRANSFER ENABLE_LOCAL_CLEANUP DRY_RUN PATH_CHECK_REQUIRED PATH_CHECK_INCLUDE_CRAM STREAM_SMOKE_TEST; do
        [[ "${!v}" == 0 || "${!v}" == 1 ]] || die "$v must be 0 or 1"
    done
    (( PATH_CHECK_MAX_FILES > 0 )) || die "PATH_CHECK_MAX_FILES must be greater than zero"
    for v in STOP_SUBMIT_PERCENT FORCE_TRANSFER_PERCENT EMERGENCY_PERCENT; do
        (( ${!v} <= 100 )) || die "$v must be between 0 and 100"
    done
    (( STOP_SUBMIT_PERCENT <= FORCE_TRANSFER_PERCENT )) || die "STOP_SUBMIT_PERCENT must be <= FORCE_TRANSFER_PERCENT"
    (( FORCE_TRANSFER_PERCENT <= EMERGENCY_PERCENT )) || die "FORCE_TRANSFER_PERCENT must be <= EMERGENCY_PERCENT"
    (( WORK_STOP_SUBMIT_PERCENT <= WORK_EMERGENCY_CLEAN_PERCENT && WORK_EMERGENCY_CLEAN_PERCENT <= WORK_CRITICAL_PERCENT && WORK_CRITICAL_PERCENT <= 100 )) || die "work disk thresholds must be monotonic and <= 100"
    (( WORK_ARRAY_RELEASE_PERCENT < WORK_CRITICAL_PERCENT )) || die "WORK_ARRAY_RELEASE_PERCENT must be below WORK_CRITICAL_PERCENT"
    (( DISK_PRESSURE_POLL_SECONDS > 0 )) || die "DISK_PRESSURE_POLL_SECONDS must be greater than zero"
    (( FAILED_CACHE_CLEAN_TARGET_PERCENT < FAILED_CACHE_CLEAN_TRIGGER_PERCENT && FAILED_CACHE_CLEAN_TRIGGER_PERCENT <= WORK_CRITICAL_PERCENT )) || die "failed-cache target must be below trigger and trigger <= critical"
    case "${GLOBUS_SYNC_LEVEL:-}" in exists|size|mtime|checksum) ;; *) die "GLOBUS_SYNC_LEVEL must be one of: exists size mtime checksum" ;; esac
    if [[ "$ENABLE_PIPELINE_SUBMIT" == 1 ]]; then command -v sbatch >/dev/null || die "sbatch not found"; fi
    if [[ "$ENABLE_TRANSFER" == 1 && "$DRY_RUN" == 0 ]]; then load_globus_module; fi
}

submission_task_state() {
    local job="$1" task="$2"
    command -v sacct >/dev/null 2>&1 || return 0
    # JobIDRaw can be Slurm's internal numeric ID (rather than array_job_task).
    # JobID is the portable field that identifies the requested array element.
    sacct -n -j "${job}_${task}" --format=JobID,State --parsable2 2>/dev/null |
      awk -F '|' -v wanted="${job}_${task}" '$1==wanted && state==""{sub(/ .*/,"",$2);sub(/\+$/, "", $2);state=$2} END{if(state!="")print state}'
}

active_submission_count() {
    awk -F '\t' 'NR>1&&$9~/^(CREATED|SUBMITTED|RUNNING)$/{n++}END{print n+0}' "$WAVE_STATUS_FILE"
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

migrate_wave_state_file() {
    [[ -e "$WAVE_STATUS_FILE" ]] || { wave_header > "$WAVE_STATUS_FILE"; return; }
    local fields backup tmp
    fields=$(awk -F '\t' 'NR==1{print NF}' "$WAVE_STATUS_FILE")
    [[ "$fields" == 22 ]] && return
    if [[ "$fields" == 21 && "${PIPELINE_MODE:-legacy_batch}" != streaming_per_sample ]]; then return; fi
    if [[ "$fields" == 21 ]]; then
      backup="${WAVE_STATUS_FILE}.bak.$(date -u +%Y%m%dT%H%M%SZ).$$"; cp -p "$WAVE_STATUS_FILE" "$backup"; tmp="${WAVE_STATUS_FILE}.tmp.$$"
      { wave_header; awk -F '\t' -v OFS='\t' 'NR>1{print $0,"WAVE_ROOT"}' "$WAVE_STATUS_FILE"; } > "$tmp"; mv "$tmp" "$WAVE_STATUS_FILE"; return
    fi
    [[ "$fields" == 11 ]] || die "Unsupported wave state schema ($fields columns): $WAVE_STATUS_FILE"
    backup="${WAVE_STATUS_FILE}.bak.$(date -u +%Y%m%dT%H%M%SZ).$$"; cp -p "$WAVE_STATUS_FILE" "$backup"
    tmp="${WAVE_STATUS_FILE}.tmp.$$"
    { wave_header; awk -F '\t' -v OFS='\t' 'NR>1 { print $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,"","",$1,"",0,"unknown","unknown","unknown","","unknown","WAVE_ROOT" }' "$WAVE_STATUS_FILE"; } > "$tmp"
    mv "$tmp" "$WAVE_STATUS_FILE"; log "Migrated wave state; backup: $backup"
}
ensure_wave_state_file() { with_state_lock migrate_wave_state_file; }
ensure_state_files() {
    with_state_lock migrate_state_file
    [[ -e "$TRANSFER_TASK_FILE" ]] || transfer_header > "$TRANSFER_TASK_FILE"
    if [[ ! -e "$VALIDATION_FILE" ]]; then
        validation_header > "$VALIDATION_FILE"
    elif [[ $(awk -F '\t' 'NR==1{print NF}' "$VALIDATION_FILE") -lt 22 ]]; then
        local validation_tmp="${VALIDATION_FILE}.tmp.$$"
        { validation_header; awk -F '\t' 'NR>1' "$VALIDATION_FILE"; } > "$validation_tmp"
        mv "$validation_tmp" "$VALIDATION_FILE"
    fi
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
slurm_normalize_state() { local s="${1%% *}"; s="${s%+}"; printf '%s\n' "$s"; }
slurm_state_is_active() { case "$(slurm_normalize_state "$1")" in PENDING|RUNNING|CONFIGURING|COMPLETING|REQUEUED|RESIZING|SUSPENDED) return 0;; *) return 1;; esac; }
slurm_state_is_executing() { case "$(slurm_normalize_state "$1")" in RUNNING|CONFIGURING|COMPLETING) return 0;; *) return 1;; esac; }
slurm_state_is_terminal() { case "$(slurm_normalize_state "$1")" in COMPLETED|FAILED|CANCELLED|TIMEOUT|PREEMPTED|NODE_FAIL|OUT_OF_MEMORY|BOOT_FAIL|DEADLINE|REVOKED|SPECIAL_EXIT) return 0;; *) return 1;; esac; }

# Query the live queue once, without asking Slurm to resolve a possibly expired
# job ID.  %A is the exact array master ID for both expanded elements and
# compact pending-array rows; comparing that field numerically avoids prefix
# matches.  Exit 2 means the queue query itself failed.
slurm_live_job_rows() {
    local user="$1" job="$2" listing
    listing=$(squeue --noheader --user="$user" --format='%A|%a|%T|%j') || return 2
    awk -F'|' -v job="$job" '$1==job {print}' <<<"$listing"
}

# Print one of LIVE, TERMINAL, ACCOUNTING_UNAVAILABLE,
# ACCOUNTING_AMBIGUOUS, or QUERY_ERROR.  For TERMINAL an additional stable
# states= line describes only true top-level array elements.
slurm_cancelled_recovery_contract() {
    local user="$1" job="$2" rows accounting jid state
    if ! rows=$(slurm_live_job_rows "$user" "$job"); then printf 'QUERY_ERROR\n'; return; fi
    if [[ -n "${rows//$'\n'/}" ]]; then printf 'LIVE\n'; return; fi
    command -v sacct >/dev/null 2>&1 || { printf 'ACCOUNTING_UNAVAILABLE\n'; return; }
    accounting=$(sacct -n -j "$job" --format=JobID,State --parsable2 2>/dev/null) || { printf 'ACCOUNTING_UNAVAILABLE\n'; return; }
    local discovered=0 cancelled=0 ambiguous=0 states=""
    while IFS='|' read -r jid state _; do
        [[ "$jid" =~ ^${job}_[0-9]+$ ]] || continue
        discovered=$((discovered + 1)); state=$(slurm_normalize_state "$state")
        states="${states}${states:+,}$state"; [[ "$state" == CANCELLED ]] && cancelled=1
        slurm_state_is_terminal "$state" || ambiguous=1
    done <<<"$accounting"
    if ((discovered == 0)); then printf 'ACCOUNTING_UNAVAILABLE\n'; return; fi
    if ((ambiguous)); then printf 'ACCOUNTING_AMBIGUOUS\nstates=%s\n' "$states"; return; fi
    printf 'TERMINAL\nstates=%s\ncancellation_evidence=%s\n' "$states" "$cancelled"
}

# Resolve the sole authoritative recovery owner.  This function performs no
# Slurm queries and no mutation, so planners and appliers share discovery.
resolve_cancelled_recovery_target() {
    local -a active orphaned; local sid source matches status job map row
    mapfile -t active < <(awk -F '\t' 'NR>1&&$9~/^(CREATED|SUBMITTED|RUNNING)$/{print $1}' "$WAVE_STATUS_FILE")
    if ((${#active[@]} > 1)); then die "recovery blocked: multiple active submissions: ${active[*]}"; fi
    if ((${#active[@]} == 1)); then sid=${active[0]}; source=active_submission
    else
        mapfile -t orphaned < <(awk -F '\t' 'NR>1&&$4~/^(PIPELINE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_DEFERRED_RUNNING)$/&&$6!=""{n[$6]++}END{for(w in n)print w"\t"n[w]}' "$STATUS_FILE" | sort)
        if ((${#orphaned[@]} == 0)); then printf 'target_source=none\nnothing_to_recover=1\n'; return; fi
        if ((${#orphaned[@]} > 1)); then printf 'recovery blocked: orphaned running samples reference multiple waves:\n' >&2; printf '  %s\n' "${orphaned[@]}" >&2; return 1; fi
        sid=${orphaned[0]%%$'\t'*}; source=orphaned_sample_wave_id
        matches=$(awk -F '\t' -v w="$sid" 'NR>1&&$1==w{n++}END{print n+0}' "$WAVE_STATUS_FILE")
        ((matches == 1)) || die "orphaned sample wave $sid does not resolve to exactly one wave_status row (found $matches)"
        status=$(wave_field "$sid" status); [[ "$status" == CANCELLED ]] || die "orphaned sample wave $sid must be CANCELLED (status: ${status:-empty})"
    fi
    status=$(wave_field "$sid" status); job=$(wave_field "$sid" pipeline_job_id)
    [[ "$job" =~ ^[0-9]+$ ]] || die "recovery submission $sid has no valid pipeline job ID"
    map="$MANAGER_ROOT/state/submission_task_map/$sid.tsv"; [[ -s "$map" ]] || die "submission task map missing: $map"
    if [[ "$source" == orphaned_sample_wave_id ]]; then
        while IFS= read -r row; do
            awk -F '\t' -v s="$row" 'NR>1&&$8==s{ok=1}END{exit !ok}' "$map" || die "orphaned running sample $row is not present in submission task map for $sid"
        done < <(awk -F '\t' -v w="$sid" 'NR>1&&$6==w&&$4~/^(PIPELINE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_DEFERRED_RUNNING)$/{print $1}' "$STATUS_FILE")
    fi
    printf 'target_source=%s\nsubmission_id=%s\nwave_status=%s\npipeline_job_id=%s\nmap_file=%s\nnothing_to_recover=0\n' "$source" "$sid" "$status" "$job" "$map"
}
manager_wave_state_is_active() { case "$1" in CREATED|SUBMITTED|RUNNING) return 0;; *) return 1;; esac; }
active_wave_count() { awk -F '\t' 'NR>1 && $9 ~ /^(CREATED|SUBMITTED|RUNNING)$/ {n++} END{print n+0}' "$WAVE_STATUS_FILE"; }
samples_in_wave() { local wave="${1:-}"; awk -F '\t' -v w="$wave" 'NR>1 && $6!="" && (w==""||$6==w) && $4 ~ /^(WAVE_SUBMITTED|PIPELINE_RUNNING|PIPELINE_RETRY_RUNNING|PIPELINE_DEFERRED_RUNNING)$/ {print $1}' "$STATUS_FILE"; }
wave_is_active() { local w="$1"; awk -F '\t' -v w="$w" 'NR>1&&$1==w&&$9~/^(CREATED|SUBMITTED|RUNNING)$/{ok=1} END{exit !ok}' "$WAVE_STATUS_FILE"; }
file_sha256() { [[ -s "${1:-}" ]] && sha256sum "$1" | awk '{print $1}' || printf 'unknown\n'; }
git_commit_or_unknown() { git -C "$PIPELINE_REPO" rev-parse HEAD 2>/dev/null || printf 'unknown\n'; }
wave_field() { local wave="$1" field="$2"; awk -F '\t' -v w="$wave" -v f="$field" 'NR==1{for(i=1;i<=NF;i++)h[$i]=i;next} $1==w{print (h[f]?$h[f]:"");exit}' "$WAVE_STATUS_FILE"; }
latest_failed_wave_for_samples() { local list="$1"; awk -F '\t' -v list="$list" 'BEGIN{while((getline<list)>0){need[$1]=1;n++}} NR==1{for(i=1;i<=NF;i++)h[$i]=i;next} NR>1&&($9=="FAILED"||$9=="PARTIAL_COMPLETE"){split("",seen); c=0; while((getline line<$2)>0){split(line,a,"\t"); if(a[1] in need && !seen[a[1]]){seen[a[1]]=1;c++}} close($2); if(c==n) last=$1} END{print last}' "$WAVE_STATUS_FILE"; }
work_root_resume_in_use() { local root="$1"; awk -F '\t' -v r="$root" 'NR==1{for(i=1;i<=NF;i++)h[$i]=i;next} NR>1 && h["work_root"] && $h["work_root"]==r && h["retry_of_wave_id"] && $h["retry_of_wave_id"]!="" && $9~/^(CREATED|SUBMITTED|RUNNING)$/ {found=1} END{exit !found}' "$WAVE_STATUS_FILE"; }
get_samples_by_status() { local regex="$1"; awk -F '\t' -v r="$regex" 'NR>1 && $4 ~ r {print $1}' "$STATUS_FILE"; }
# Print at most limit matches while still reading the complete state file.  Unlike
# piping get_samples_by_status through head, this cannot close a producer early.
get_samples_by_status_limit() { local regex="$1" limit="$2"; awk -F '\t' -v r="$regex" -v limit="$limit" 'NR>1 && $4 ~ r {if(n < limit) print $1; n++}' "$STATUS_FILE"; }

# Read all of scontrol's output so that it cannot receive SIGPIPE under pipefail.
parse_slurm_max_array_size() {
    awk -F= '
      /^[[:space:]]*MaxArraySize[[:space:]]*=/ {
        value=$2
        gsub(/[[:space:]]/, "", value)
        if (max == "") max=value
      }
      END {if (max != "") print max}
    '
}
sample_species() { local sample="$1"; awk -F '\t' -v s="$sample" 'NR>1&&$1==s{print $2;exit}' "$STATUS_FILE"; }
disk_used_percent() { df -P "$DISK_CHECK_PATH" | awk 'NR==2{gsub(/%/,"",$5);print $5}'; }
work_disk_used_percent() { df -P "$WORK_DISK_CHECK_PATH" | awk 'NR==2{gsub(/%/,"",$5);print $5}'; }
safe_sample_id() { [[ "${1:-}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] && [[ "$1" != . && "$1" != .. && "$1" != .sample_state && "$1" != .locks && "$1" != .manifests ]]; }
sample_work_root() { safe_sample_id "$1" || die "unsafe sample ID: $1"; printf '%s/%s\n' "${PIPELINE_WORK_ROOT%/}" "$1"; }
marker_field() {
    local file="$1" field="$2"
    # Workers write vertical key/value TSVs.  Older workers wrote a header and
    # one data row, so recognize the shape before selecting the parser.
    awk -F '\t' -v f="$field" '
      NR==1 { nf1=NF; for(i=1;i<=NF;i++){head[i]=$i; h[$i]=i}; next }
      NR==2 {
        # A second recognized key identifies the vertical representation;
        # otherwise this is the legacy data row (including two-column files).
        vertical=($1 ~ /^(sample_id|worker_state|reference_name|failed_stage|failure_class|failure_reason|immediate_worker_attempt|first_failure_epoch|last_failure_epoch|fingerprint|completed_at)$/)
        if (!vertical) { if(h[f]) print $h[f]; exit }
        if(head[1]==f) {print head[2]; exit}
        if($1==f) {print $2; exit}
        next
      }
      { if(vertical && $1==f){print $2; exit} }
    ' "$file"
}
sample_array_state() {
    local sample="$1" map job task
    map=$(awk -F '\t' -v s="$sample" 'NR>1&&$6=="SAMPLE"&&$8==s{line=$0}END{print line}' "${MANAGER_ROOT}/state/submission_task_map/"*.tsv 2>/dev/null || true)
    if [[ -n "$map" ]]; then job=$(cut -f4 <<<"$map"); task=$(cut -f5 <<<"$map")
    else
      map=$(awk -F '\t' -v s="$sample" 'NR>1&&$4==s{line=$0}END{print line}' "${MANAGER_ROOT}/state/array_sample_map/"*.tsv 2>/dev/null || true)
      [[ -n "$map" ]] || return 0; job=$(cut -f2 <<<"$map"); task=$(cut -f3 <<<"$map")
    fi
    submission_task_state "$job" "$task"
}

# pipeline_attempts includes the initial manager submission.  Thus a value of 2
# permits at most two additional manager deferred submissions (attempts 2 and 3).
# Worker self-requeues and immediate retries do not update pipeline_attempts.
deferred_terminal_status() {
    local wave_phase="$1" attempts="$2"
    if [[ "$wave_phase" == DEFERRED_RETRY ]]; then
        if (( attempts >= MAX_DEFERRED_RETRIES + 1 )); then printf '%s\n' PIPELINE_DEFERRED_FAILED
        else printf '%s\n' PIPELINE_DEFERRED_RETRY; fi
    else
        printf '%s\n' PIPELINE_DEFERRED_RETRY
    fi
}
normal_active_wave_count() { awk -F '\t' 'NR>1&&$9~/^(CREATED|SUBMITTED|RUNNING)$/&&$11!~/phase=DEFERRED_RETRY/{n++}END{print n+0}' "$WAVE_STATUS_FILE"; }
normal_active_streaming_task_count() {
    local file job task phase state active=0
    # Exact task state is authoritative once Slurm exposes it.  Submission audit
    # state is the conservative fallback during the short post-sbatch interval
    # before the array elements appear in accounting.
    for file in "${MANAGER_ROOT}"/state/submission_task_map/*.tsv; do
        [[ -e "$file" ]] || continue
        while IFS=$'\t' read -r phase job task; do
            [[ "$phase" != DEFERRED_RETRY ]] || continue
            state=$(submission_task_state "$job" "$task")
            slurm_state_is_active "$state" && active=$((active + 1))
        done < <(awk -F '\t' 'NR>1&&!seen[$4 FS $5]++{print $3"\t"$4"\t"$5}' "$file")
    done
    (( active > 0 )) && { printf '%s\n' "$active"; return; }
    normal_active_wave_count
}
determine_manager_phase() {
    local used pending active deferred phase reason tmp
    used=$(work_disk_used_percent); pending=$(awk -F '\t' 'NR>1&&$4=="PENDING"{n++}END{print n+0}' "$STATUS_FILE")
    if [[ "$PIPELINE_MODE" == streaming_per_sample ]]; then
        active=$(normal_active_streaming_task_count)
    else active=$(normal_active_wave_count); fi
    deferred=$(awk -F '\t' 'NR>1&&$4=="PIPELINE_DEFERRED_RETRY"{n++}END{print n+0}' "$STATUS_FILE")
    if (( used >= WORK_CRITICAL_PERCENT )); then phase=PAUSED_DISK_PRESSURE; reason="critical work filesystem ${used}%";
    elif (( pending > 0 || active > 0 )); then phase=NORMAL; reason="new samples or normal sample tasks active";
    elif (( deferred > 0 && ENABLE_DEFERRED_RETRY == 1 )); then phase=DEFERRED_RETRY; reason="pending empty; deferred work available";
    else phase=NORMAL; reason="idle"; fi
    tmp="${MANAGER_PHASE_FILE}.tmp.$$"; { printf 'phase\tupdated_at\tpending_count\tnormal_active_wave_count\tdeferred_count\treason\n'; printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$phase" "$(now_iso)" "$pending" "$active" "$deferred" "$reason"; } > "$tmp"; mv "$tmp" "$MANAGER_PHASE_FILE"
}
manager_phase() { [[ -s "$MANAGER_PHASE_FILE" ]] || determine_manager_phase; awk -F '\t' 'NR==2{print $1}' "$MANAGER_PHASE_FILE"; }
local_sample_dir_count() {
    [[ -d "$LOCAL_RESULTS" ]] || { echo 0; return; }
    local dir name excluded count=0 exclusion
    while IFS= read -r -d '' dir; do
        name=${dir##*/}; excluded=0
        for exclusion in ${LOCAL_RESULTS_EXCLUDE_DIRS:-}; do
            [[ "$name" == "$exclusion" ]] && { excluded=1; break; }
        done
        (( excluded )) || count=$((count + 1))
    done < <(find "$LOCAL_RESULTS" -mindepth 1 -maxdepth 1 -type d -print0)
    echo "$count"
}
find_exact_one() { find "$1" -type f -name "$2" -print -quit 2>/dev/null || true; }
require_nonempty() { [[ -n "${1:-}" && -s "$1" ]]; }
