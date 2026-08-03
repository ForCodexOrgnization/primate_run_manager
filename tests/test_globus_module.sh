#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/test_helper.sh"

# An existing Globus CLI wins; no module operation is attempted.
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/bin"
printf '#!/usr/bin/env bash\nexit 0\n' > "$tmp/bin/globus"
printf '#!/usr/bin/env bash\necho called >> "$MODULE_MARKER"\n' > "$tmp/bin/module"
chmod +x "$tmp/bin/globus" "$tmp/bin/module"
MODULE_MARKER="$tmp/module-called" PATH="$tmp/bin:/usr/bin:/bin" GLOBUS_MODULE=test-module \
    bash -c 'source "$1/lib/common.sh"; load_globus_module' _ "$REPO"
assert test ! -e "$tmp/module-called"

# A configured module can make Globus available.
rm "$tmp/bin/globus"
cat > "$tmp/bin/module" <<'MODULE'
#!/usr/bin/env bash
printf '%s\n' "$2" > "$MODULE_MARKER"
printf '#!/usr/bin/env bash\nexit 0\n' > "$(dirname "$0")/globus"
chmod +x "$(dirname "$0")/globus"
MODULE
chmod +x "$tmp/bin/module"
MODULE_MARKER="$tmp/module-loaded" PATH="$tmp/bin:/usr/bin:/bin" GLOBUS_MODULE=Globus-CLI/test \
    bash -c 'source "$1/lib/common.sh"; load_globus_module' _ "$REPO"
assert grep -qx 'Globus-CLI/test' "$tmp/module-loaded"

# A module operation that does not provide Globus ends with an actionable error.
rm "$tmp/bin/globus"
printf '#!/usr/bin/env bash\nexit 0\n' > "$tmp/bin/module"
chmod +x "$tmp/bin/module"
if output=$(PATH="$tmp/bin:/usr/bin:/bin" GLOBUS_MODULE=broken-module \
    bash -c 'source "$1/lib/common.sh"; load_globus_module' _ "$REPO" 2>&1); then
    echo 'missing Globus unexpectedly succeeded' >&2; exit 1
fi
assert grep -q 'Globus CLI not found. Configure GLOBUS_MODULE or install globus-cli.' <<< "$output"

# Configuration validation preserves dry runs but requires Globus for real transfers.
new_env
sed -i 's/ENABLE_TRANSFER=0/ENABLE_TRANSFER=1/; s/DRY_RUN=0/DRY_RUN=1/' "$T/config.sh"
rm -f "$T/mockbin/globus" "$T/mockbin/module"
bash -c 'source "$1/lib/common.sh"; load_config "$2"; validate_config' _ "$REPO" "$T/config.sh"
sed -i 's/DRY_RUN=1/DRY_RUN=0/' "$T/config.sh"
if output=$(bash -c 'source "$1/lib/common.sh"; load_config "$2"; validate_config' _ "$REPO" "$T/config.sh" 2>&1); then
    echo 'real transfer validation unexpectedly succeeded' >&2; exit 1
fi
assert grep -q 'Globus CLI not found' <<< "$output"

assert grep -qx 'GLOBUS_MODULE="Globus-CLI/3.34.0-GCCcore-13.3.0"' "$REPO/config/bouchet.sh"

# The Globus diagnostic uses the CLI 3.x version subcommand, then checks identity.
assert grep -Eq '^[[:space:]]*(if ! )?globus version;? then$' "$REPO/bin/check_globus.sh"
legacy_version='globus -''-version'
assert bash -c '! grep -Fq -- "$1" "$2/bin/check_globus.sh"' _ "$legacy_version" "$REPO"

new_env
cat > "$T/mockbin/globus" <<'GLOBUS'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$GLOBUS_CALLS"
case "$1" in
    version)
        [[ "${GLOBUS_VERSION_FAIL:-0}" == 0 ]] || exit 42
        echo 'Globus CLI 3.34.0'
        ;;
    whoami) echo 'test@example.org' ;;
    *) exit 2 ;;
esac
GLOBUS
chmod +x "$T/mockbin/globus"

output=$(GLOBUS_CALLS="$T/globus-calls" \
    "$REPO/bin/check_globus.sh" "$T/config.sh")
assert grep -q 'Globus CLI 3.34.0' <<< "$output"
assert grep -q 'test@example.org' <<< "$output"
assert test "$(sed -n '1p' "$T/globus-calls")" = version
assert test "$(sed -n '2p' "$T/globus-calls")" = whoami

: > "$T/globus-calls"
if output=$(GLOBUS_CALLS="$T/globus-calls" GLOBUS_VERSION_FAIL=1 \
    "$REPO/bin/check_globus.sh" "$T/config.sh" 2>&1); then
    echo 'failed Globus version check unexpectedly succeeded' >&2; exit 1
fi
assert grep -q "ERROR: Globus CLI version check failed: 'globus version' returned an error." <<< "$output"
assert grep -qx version "$T/globus-calls"

echo 'Globus module tests passed.'
