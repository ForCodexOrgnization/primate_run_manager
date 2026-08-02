set -euo pipefail
REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
new_env() {
 T=$(mktemp -d); mkdir -p "$T"/{manager,samples,results,analysis,work,mockbin}; export T
 printf 's1\tsp1\ns2\tsp2\ns3\tsp3\n' > "$T/samples/list.tsv"
 cat > "$T/launcher.sh" <<'L'
#!/usr/bin/env bash
printf '%s\t%s\t%s\n' "$BATCH_SIZE" "$CHAIN_CONCURRENT_BATCHES" "$NF_BASE_WORK_DIR" >> "$TEST_ROOT/invocations"
echo 'launcher: Submitted batch job 123456'
L
 chmod +x "$T/launcher.sh"
 cat > "$T/mockbin/sbatch" <<'S'
#!/usr/bin/env bash
exit 0
S
 chmod +x "$T/mockbin/sbatch"; export PATH="$T/mockbin:$PATH" TEST_ROOT="$T"
 cat > "$T/config.sh" <<EOF2
HPC_NAME=TEST
MANAGER_ROOT=$T/manager
PIPELINE_REPO=$T
PIPELINE_LAUNCHER=$T/launcher.sh
PIPELINE_CONFIG=test.config
ASSIGNED_SAMPLE_LIST=$T/samples/list.tsv
LOCAL_RESULTS=$T/results
ANALYSIS_ROOT=$T/analysis
PIPELINE_WORK_ROOT=$T/work
PIPELINE_WAVE_SIZE=2
PIPELINE_BATCH_SIZE=5
CHAIN_CONCURRENT_BATCHES=1
NUMT_CONCURRENT=3
MAX_ACTIVE_PIPELINE_WAVES=1
MAX_PIPELINE_RETRIES=2
AUTO_RETRY_IMPORTED_INCOMPLETE=0
CLEAN_ON_SUCCESS=1
ENABLE_CHUNKED_ALIGNMENT=true
ENABLE_PIPELINE_SUBMIT=1
DISK_CHECK_PATH=$T
STOP_SUBMIT_PERCENT=100
FORCE_TRANSFER_PERCENT=0
EMERGENCY_PERCENT=100
MAX_LOCAL_SAMPLE_DIRS=100
TRANSFER_BATCH_SIZE=2
SOURCE_COLLECTION=src
SOURCE_ROOT=/source
DEST_COLLECTION=dst
DEST_ROOT=/dest
GLOBUS_SYNC_LEVEL=checksum
ENABLE_TRANSFER=0
ENABLE_LOCAL_CLEANUP=0
DRY_RUN=0
SAMTOOLS_MODULE=
NEXTFLOW_MODULE=
MANAGER_POLL_SECONDS=1
EOF2
}
assert() { "$@" || { echo "ASSERTION FAILED: $*" >&2; exit 1; }; }
