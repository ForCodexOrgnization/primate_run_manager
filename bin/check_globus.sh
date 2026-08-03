#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${SCRIPT_DIR}/../lib/common.sh"

load_config "${1:-}"
load_globus_module
printf 'Globus executable: %s\n' "$(command -v globus)"
if ! globus version; then
    die "Globus CLI version check failed: 'globus version' returned an error."
fi
globus whoami
