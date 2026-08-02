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

Sample states are `PENDING`, `WAVE_SUBMITTED`, `PIPELINE_RUNNING`, `PIPELINE_COMPLETE`, `PIPELINE_INCOMPLETE`, `PIPELINE_FAILED`, `READY_TO_TRANSFER`, `TRANSFERRING`, `TRANSFERRED_FULL`, `TRANSFER_FAILED`, and `LOCAL_FINAL_RETAINED`. Failed waves never blanket-fail their samples; strict output scanning evaluates each sample independently.

## State and migration

`bin/initialize_samples.sh` is idempotent: existing rows remain, new assigned samples are appended, and samples are not duplicated. On first use, an old 10-column `state/sample_status.tsv` is copied to a timestamped `sample_status.tsv.bak.*` and atomically migrated to the 14-column schema. Wave state is in `state/wave_status.tsv`; validation details are in `state/output_validation.tsv`.

All state updates use a lock, temporary output, and atomic rename. Do not edit state while the manager is running.

## Workspace transfer and local retention

Globus transfers each complete sample directory recursively from `${SOURCE_ROOT}/${sample}/` to `${DEST_ROOT}/${sample}/` with checksum synchronization. After Globus reports `SUCCEEDED`, optional cleanup first verifies the destination and retains:

- final round2 VCF and its optional `.tbi`;
- round2 per-base coverage;
- NUMT-decoy per-base coverage;
- round2 mtCN.

The complete directory remains on Workspace. Local retained files live under `ANALYSIS_ROOT/{vcf,round2_coverage,numt_decoy_coverage,mtcn,receipts}`. Cleanup uses temporary copies, byte comparisons, atomic renames, and a receipt before removing the original directory. Cleanup is disabled by default.

## First Bouchet deployment

Confirm collection-visible paths separately from job-visible POSIX paths, and start with:

```bash
PIPELINE_WAVE_SIZE=10
PIPELINE_BATCH_SIZE=5
CHAIN_CONCURRENT_BATCHES=1
MAX_ACTIVE_PIPELINE_WAVES=1
TRANSFER_BATCH_SIZE=5
ENABLE_TRANSFER=0
ENABLE_LOCAL_CLEANUP=0
DRY_RUN=1
```

Then follow this sequence:

1. Initialize samples: `bin/initialize_samples.sh config/bouchet.sh`.
2. Run one dry cycle: `bin/manager_cycle.sh config/bouchet.sh`.
3. Inspect `manifests/pipeline_waves/*.samples.tsv`, the printed launcher command, and `bin/show_status.sh config/bouchet.sh`.
4. Set `DRY_RUN=0` while leaving transfer and cleanup disabled.
5. Submit one 10-sample test wave: `bin/manager_cycle.sh config/bouchet.sh`.
6. Verify outputs with `bin/scan_results.sh config/bouchet.sh` and inspect `state/output_validation.tsv`.
7. Set `ENABLE_TRANSFER=1` and run a cycle.
8. Inspect the complete sample directories on Workspace and verify Globus tasks.
9. Only after manual validation, set `ENABLE_LOCAL_CLEANUP=1`.

Useful exact commands:

```bash
cd /home/lt692/ycga_work/primate_run_manager
bin/initialize_samples.sh config/bouchet.sh
DRY_RUN=1 bin/manager_cycle.sh config/bouchet.sh # use a config override/copy; sourced config values take precedence
column -t -s $'\t' manifests/pipeline_waves/*.samples.tsv
bin/show_status.sh config/bouchet.sh
bin/scan_results.sh config/bouchet.sh
RUN_MANAGER_CONFIG="$PWD/config/bouchet.sh" sbatch manager_daemon.slurm
```

For an environment override, make a testing copy of the config and edit `DRY_RUN=1`; shell variables in the sourced config intentionally define the authoritative settings.

## Validation and safety

Config validation rejects missing launchers/sample lists, unsafe root paths, equal result/analysis roots, malformed numeric settings, and missing required CLIs when the corresponding live operation is enabled. `DRY_RUN=1` still writes manifests and scans/displays state, but does not invoke the pipeline launcher, `globus transfer`, or deletion.

Run repository checks with:

```bash
find . -type f -name '*.sh' -print0 | xargs -0 -n1 bash -n
shellcheck config/*.sh lib/common.sh bin/*.sh tests/*.sh
bash tests/run_tests.sh
```
