#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
for test in tests/test_globus_module.sh tests/test_manager_cycle_scanning.sh tests/test_initialization.sh tests/test_wave_submission.sh tests/test_streaming_manager.sh tests/test_slurm_requeue_resume.sh tests/test_scan_results.sh tests/test_cleanup.sh tests/test_conservative_retry.sh tests/test_safety_changes.sh tests/test_deployment_safety.sh tests/test_slurm_import.sh; do echo "==> $test"; bash "$test"; done
echo 'All tests passed.'
