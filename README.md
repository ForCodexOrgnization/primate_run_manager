# Primate run manager

This repository is an external orchestration layer; it does not alter the preprocessing, NUMT, round1, or round2 workflows in `primate_mt_variant_calling`.

## Execution model and terminology

```text
manager wave (up to PIPELINE_WAVE_SIZE samples)
  -> one launch_pipeline_all_per_batch.sh invocation
     -> internal fixed-size pipeline batches (PIPELINE_BATCH_SIZE)
        -> pre -> NUMT -> round1 -> round2
```

A **manager wave** is selected and tracked by this repository. A **pipeline batch** is created internally by the pipeline launcher. For example, a 50-sample wave and batch size 5 results in one launcher invocation that creates ten internal batches—not ten manager invocations. Every wave has its own manifest, Nextflow work directory, batch-list directory, and Slurm array job ID.

Sample states include `PENDING`, the normal running states, `PIPELINE_DEFERRED_RETRY`, `PIPELINE_DEFERRED_RUNNING`, `PIPELINE_DEFERRED_FAILED`, transfer states, and the legacy-compatible `PIPELINE_RETRY_READY` / `PIPELINE_RETRY_RUNNING` states. Failed waves never blanket-fail their samples; strict output scanning evaluates each sample independently. Historical incomplete imports enter `PIPELINE_INCOMPLETE_REVIEW` and are never moved into the deferred queue automatically. Newly incomplete normal-wave samples enter `PIPELINE_DEFERRED_RETRY`; incomplete fresh retries become `PIPELINE_DEFERRED_FAILED` after `MAX_DEFERRED_RETRIES`.

## State and migration

`bin/initialize_samples.sh` is idempotent: existing rows remain, new assigned samples are appended, and samples are not duplicated. On first use, an old 10-column `state/sample_status.tsv` is copied to a timestamped `sample_status.tsv.bak.*` and atomically migrated to the 14-column schema. Wave state is in `state/wave_status.tsv`; validation details are in `state/output_validation.tsv`.

All state updates use a lock, temporary output, and atomic rename. Do not edit state while the manager is running.

## Conservative incomplete-sample workflow

Import historical directories through Slurm with `bin/submit_import_existing.sh CONFIG`; do not run `bin/import_existing_results.sh` on a login node. Complete samples become transfer-ready, while incomplete samples default to `PIPELINE_INCOMPLETE_REVIEW`. The import job logs each sample before and after validation, applies the configured timeout to `samtools quickcheck`, and reuses successful validation rows when all required output sizes and modification times are unchanged. Review `state/pipeline_incomplete_report.tsv`, place approved sample IDs (one per line) in a file, and run `bin/approve_retry_samples.sh CONFIG sample_ids.txt`. Approval changes only manager state to `PIPELINE_RETRY_READY`; it never deletes or changes pipeline output. A retry receives a new manager-wave work directory while the all-per-batch launcher reuses valid existing stages through its skip/resume behavior. Generate a fresh report at any time with `bin/report_incomplete_samples.sh CONFIG`.

## Workspace transfer and local retention

Globus transfers each complete sample directory recursively from `${SOURCE_ROOT}/${sample}/` to `${DEST_ROOT}/${sample}/` with checksum synchronization. After Globus reports `SUCCEEDED`, optional cleanup first verifies the destination and retains:

- final round2 VCF and its optional `.tbi`;
- round2 per-base coverage;
- NUMT-decoy per-base coverage;
- round2 mtCN.

The complete directory remains on Workspace. Local retained files live under `ANALYSIS_ROOT/{vcf,round2_coverage,numt_decoy_coverage,mtcn,receipts}`. Cleanup uses temporary copies, byte comparisons, atomic renames, and a receipt before removing the original directory. Cleanup is disabled by default.

At most `MAX_ACTIVE_TRANSFER_TASKS` Globus tasks are submitted concurrently, and
the configured `GLOBUS_SYNC_LEVEL` is passed to the CLI. Cleanup requires the
destination listing to contain the CRAM and all four core final outputs; a
directory listing alone is not evidence of a complete transfer.

## Deployment migration note

Set `PIPELINE_CONFIG` to an absolute path and add these settings when migrating
an existing config:

```bash
MAX_ACTIVE_TRANSFER_TASKS=2
LOCAL_RESULTS_EXCLUDE_DIRS="numt_discovery numt_besthit logs lost+found"
SOURCE_ROOT_LOCAL_VIEW="/local/POSIX/view/of/SOURCE_ROOT"
PATH_CHECK_REQUIRED=1
PATH_CHECK_INCLUDE_CRAM=0
PATH_CHECK_MAX_FILES=5
REQUIRE_SLURM_FOR_EXISTING_IMPORT=1
ALLOW_INTERACTIVE_IMPORT=0
SAMTOOLS_QUICKCHECK_TIMEOUT_SECONDS=600
```

Run `bin/check_paths.sh CONFIG` before enabling transfers. It compares up to
`PATH_CHECK_MAX_FILES` recognized sample outputs by path, size, and SHA-256
checksum, preferring final VCF, mtCN, and coverage files. CRAM comparison is
disabled unless `PATH_CHECK_INCLUDE_CRAM=1`. A successful comparison writes the
auditable `state/path_check.passed` marker required for transfer. The check
fails instead of assuming that `LOCAL_RESULTS` and `SOURCE_ROOT_LOCAL_VIEW` are
aliases. Historical import writes the auditable inventories
`state/existing_local_samples.tsv` and
`state/unrecognized_local_directories.tsv`; review the latter before proceeding.

## First Bouchet deployment

Confirm collection-visible paths separately from job-visible POSIX paths, and start with:

```bash
PIPELINE_WAVE_SIZE=10
PIPELINE_BATCH_SIZE=5
CHAIN_CONCURRENT_BATCHES=1
MAX_ACTIVE_PIPELINE_WAVES=1
AUTO_RETRY_IMPORTED_INCOMPLETE=0
TRANSFER_BATCH_SIZE=5
ENABLE_PIPELINE_SUBMIT=0
ENABLE_TRANSFER=0
ENABLE_LOCAL_CLEANUP=0
DRY_RUN=1
```

First initialize and inventory the historical results. Initialization is intentionally safe to run on the login node; CRAM validation is submitted to Slurm:

```bash
bin/initialize_samples.sh config/bouchet.sh
bin/submit_import_existing.sh config/bouchet.sh
squeue -u "$USER"
tail -f /nfs/roberts/project/pi_njl27/lt692/primate_run_manager/logs/import_existing_<jobid>.out
```

Historical import is the full validation workflow. It may inspect every
assigned sample and therefore must run as the Slurm job above; direct
interactive full scans are blocked by default.

If a config defines `RUNTIME_LOG_DIR`, the submit helper places the output and error files there. Otherwise they are placed under `${MANAGER_RUNTIME_ROOT:-${RUNTIME_ROOT:-$MANAGER_ROOT}}/logs`. After the job completes, inspect both status and the incomplete report:

```bash
bin/show_status.sh config/bouchet.sh
bin/report_incomplete_samples.sh config/bouchet.sh
```

Then follow this sequence for new pipeline work:

1. Validate the configured Globus CLI, then submit one normal manager cycle:
   ```bash
   bin/check_globus.sh config/bouchet.sh
   bin/submit_manager_cycle.sh config/bouchet.sh
   ```
2. Inspect `manifests/pipeline_waves/*.samples.tsv`, the printed launcher command, and `bin/show_status.sh config/bouchet.sh`.
3. Set `DRY_RUN=0` while leaving transfer and cleanup disabled.
4. Submit one 10-sample test wave: `bin/submit_manager_cycle.sh config/bouchet.sh`.
5. Verify outputs during a manager job and inspect `state/output_validation.tsv`.
6. Set `ENABLE_TRANSFER=1` and run a cycle.
7. Inspect the complete sample directories on Workspace and verify Globus tasks.
8. Only after manual validation, set `ENABLE_LOCAL_CLEANUP=1`.

On Bouchet, `GLOBUS_MODULE` identifies the cluster module that provides the
CLI. The manager loads it automatically immediately before a live Globus
operation if `globus` is not already on `PATH`; manual `module load` is no
longer required. `bin/check_globus.sh` is read-only: it prints the selected
executable and version and runs `globus whoami`, but does not create or modify
a transfer.

Normal manager cycles use `scan_active_results.sh` and validate only samples in
active pipeline states. `ENABLE_FULL_SCAN_IN_MANAGER_CYCLE=0` is the safe
default; transfer-only cycles consequently do not rescan historical CRAM files.
If full scanning is explicitly enabled, `REQUIRE_SLURM_FOR_FULL_SCAN=1` blocks
it outside Slurm unless the administrative interactive override is also set.

Useful exact commands:

```bash
cd /home/lt692/ycga_work/primate_run_manager
bin/initialize_samples.sh config/bouchet.sh
bin/submit_import_existing.sh config/bouchet.sh
bin/submit_manager_cycle.sh config/bouchet.sh # use a DRY_RUN=1 config override/copy for validation
column -t -s $'\t' manifests/pipeline_waves/*.samples.tsv
bin/show_status.sh config/bouchet.sh
```

For an environment override, make a testing copy of the config and edit `DRY_RUN=1`; shell variables in the sourced config intentionally define the authoritative settings.

## Continuous manager daemon

The manager can run continuously as a five-day Slurm job; it is never started
as a login-node background process. The daemon runs one `manager_cycle.sh` at a
time, waits `MANAGER_POLL_SECONDS` after each successful cycle, and uses bounded
delays of 5, 15, and then 30 minutes after consecutive failures. A separate
daemon lock prevents overlapping daemon processes. Five minutes before its
wall time, Slurm signals the job so it can finish its active cycle and submit a
replacement with an `afterany` dependency. It does not pull Git changes, reset
state, or alter the conservative incomplete-sample retry policy.

Start, check, and stop it with:

```bash
bin/submit_manager_daemon.sh config/bouchet.sh
squeue -u "$USER" -n primate_manager_daemon
bin/show_status.sh config/bouchet.sh
bin/stop_manager_daemon.sh config/bouchet.sh
```

The submit helper refuses to create a daemon while one owned by the user is
already pending or running. The stop helper selects only this user's exact
`primate_manager_daemon` job name, leaving pipeline, transfer, and ordinary
manager-cycle jobs untouched. Daemon output is written separately as
`logs/manager_daemon_<jobid>.out` and `logs/manager_daemon_<jobid>.err` (under
`RUNTIME_LOG_DIR` when configured).

Recommended current Bouchet automation settings are:

```bash
ENABLE_TRANSFER=1
ENABLE_LOCAL_CLEANUP=1
ENABLE_PIPELINE_SUBMIT=0
MAX_ACTIVE_TRANSFER_TASKS=2
TRANSFER_BATCH_SIZE=25
MANAGER_POLL_SECONDS=1800
```

Keep `AUTO_RETRY_IMPORTED_INCOMPLETE=0`; samples in
`PIPELINE_INCOMPLETE_REVIEW` continue to require explicit approval. To disable
continuous automation, use the stop helper. Manual resubmission with the start
helper is also clean if automatic replacement cannot be submitted.

## Validation and safety

Config validation rejects missing launchers/sample lists, unsafe root paths, equal result/analysis roots, malformed numeric settings, and missing required CLIs when the corresponding live operation is enabled. `DRY_RUN=1` still writes manifests and scans/displays state, but does not invoke the pipeline launcher, `globus transfer`, or deletion.

Run repository checks with:

```bash
find . -type f -name '*.sh' -print0 | xargs -0 -n1 bash -n
shellcheck config/*.sh lib/common.sh bin/*.sh tests/*.sh run_import_existing.slurm
bash tests/run_tests.sh
```

## Slurm self-requeue and resume semantics

`NF_Primate_Chain` may self-requeue the same Slurm job or array element before walltime. While Slurm reports `PENDING`, `RUNNING`, `CONFIGURING`, `COMPLETING`, `REQUEUED`, `RESIZING`, or `SUSPENDED` (including states with a trailing `+`), the manager treats the same job/array element as the same active manager wave. The wave remains `SUBMITTED`/`RUNNING`, samples remain in `WAVE_SUBMITTED`, `PIPELINE_RUNNING`, or `PIPELINE_RETRY_RUNNING`, the original work root is retained, and `pipeline_attempts` is unchanged.

If self-requeue does not save the job and Slurm reaches an infrastructure terminal state (`TIMEOUT`, `PREEMPTED`, `NODE_FAIL`, or `BOOT_FAIL` by default), the manager records `failure_class=INFRASTRUCTURE`, `resume_eligible=1`, the wave work root, batch-list location, sample manifest checksum, pipeline config checksum, and pipeline git commit in `state/wave_status.tsv`. A fallback resume is a new manager wave, but it reuses the old Nextflow work root only when the retry manifest exactly matches the failed wave's manifest, the config checksum and git commit still match, no Slurm job from the old wave is active, and no other retry holds `${work_root}/.manager_resume.lock`.

When those checks pass, the launcher receives `NF_BASE_WORK_DIR` pointing at the original work root, `PIPELINE_RESUME=1`, `RETRY_OF_WAVE_ID`, and `ORIGINAL_WAVE_ID`. When any check fails, the retry is a fresh manager wave with a new work root and `PIPELINE_RESUME=0`. Historical `PIPELINE_INCOMPLETE_REVIEW` samples remain excluded from automatic retry/resume unless explicitly approved; `AUTO_RETRY_IMPORTED_INCOMPLETE` remains disabled by default.
# Two-phase, throughput-first scheduling

The manager uses an automatically recomputed `NORMAL` / `DEFERRED_RETRY`
phase.  During `NORMAL`, only new `PENDING` samples are submitted.  Incomplete
samples from a terminal wave are recorded as `PIPELINE_DEFERRED_RETRY`; the
manager archives bounded failure diagnostics and then safely removes the whole
terminal wave work directory.  Once pending input and normal active waves are
empty, deferred samples are retried together with a fresh work root and
`PIPELINE_RESUME=0`.

Slurm self-requeue states remain part of the original active wave and retain
their cache.  They neither consume another pipeline attempt nor create a
replacement wave.  The work filesystem is monitored independently from the
results filesystem; stop, emergency-clean, and critical thresholds prevent new
submission without ever removing active work.
