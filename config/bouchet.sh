#!/usr/bin/env bash
# Bouchet/Palmer configuration

HPC_NAME="BOUCHET"

# Manager repository root. Adjust after cloning/copying this repository.
MANAGER_ROOT="/home/lt692/ycga_work/primate_run_manager"

# Existing analysis pipeline: do not modify the pipeline itself.
PIPELINE_REPO="/home/lt692/ycga_work/primate_mt_variant_calling"
PIPELINE_SCRIPT="${PIPELINE_REPO}/launch_pipeline_streaming_per_sample.sh"
PIPELINE_CONFIG="nextflow_mcc.config"

# Input assigned to this HPC. Strict two-column TAB-separated file, no header:
# sample_id<TAB>reference_or_species_name
ASSIGNED_SAMPLE_LIST="${MANAGER_ROOT}/samples/bouchet_samples.txt"

# Local pipeline output and Nextflow work roots.
LOCAL_RESULTS="/vast/palmer/scratch/lake_nicole/lt692/primate_results"
NF_BASE_WORK_DIR="/home/lt692/ycga_work/nf_work_dir_all_per_batch/nf_work_dir_streaming_per_sample"

# Small retained files for downstream analysis after full Workspace transfer.
ANALYSIS_ROOT="/vast/palmer/scratch/lake_nicole/lt692/primate_final_analysis"

# Existing streaming-pipeline batch settings.
PIPELINE_BATCH_SIZE=25
PIPELINE_MAX_CONCURRENT=10

# Disk guardrails. New batches are not submitted at or above STOP_SUBMIT_PERCENT.
DISK_CHECK_PATH="/vast/palmer/scratch/lake_nicole/lt692"
STOP_SUBMIT_PERCENT=70
FORCE_TRANSFER_PERCENT=75
EMERGENCY_PERCENT=85
MAX_LOCAL_SAMPLE_DIRS=180

# Transfer settings.
TRANSFER_BATCH_SIZE=25
SOURCE_COLLECTION="a2bf0df9-5633-4565-b083-b8907423bb77"
# This is the path visible from the Bouchet Globus collection, not necessarily
# the POSIX path used by jobs on Palmer.
SOURCE_ROOT="/nfs/roberts/pi/pi_njl27/lt692/primate_results/"
DEST_COLLECTION="3f8ab775-c5e9-4280-849f-766ac428c358"
DEST_ROOT="/primate_results/"
GLOBUS_SYNC_LEVEL="checksum"

# Safety switches. Start with deletion disabled, verify several transfers, then set 1.
ENABLE_TRANSFER=1
ENABLE_LOCAL_CLEANUP=0

# Modules used by the manager. The existing pipeline keeps its own module setup.
SAMTOOLS_MODULE="SAMtools/1.21-GCC-13.3.0"
NEXTFLOW_MODULE="Nextflow/24.04.4"

# Manager cadence when using manager_daemon.slurm.
MANAGER_POLL_SECONDS=1800
