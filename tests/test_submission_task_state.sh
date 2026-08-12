#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test_helper.sh"
new_env
export MANAGER_ROOT="$T/manager"
source "$REPO/lib/common.sh"

mkdir -p "$MANAGER_ROOT/state/submission_task_map" "$MANAGER_ROOT/state/array_sample_map"
cat > "$MANAGER_ROOT/state/submission_task_map/current.tsv" <<'MAP'
submission_id	kind	name	job_id	task_id	scope	unused	sample_id
stream_20260808T023530Z_BOUCHET_000001	pipeline	main	21676885	69	SAMPLE	-	ERS14600617
stream_completed	pipeline	main	21676885	1	SAMPLE	-	COMPLETED_SAMPLE
MAP
cat > "$MANAGER_ROOT/state/array_sample_map/legacy.tsv" <<'MAP'
submission_id	job_id	task_id	sample_id
legacy_submission	98765	3	LEGACY_SAMPLE
MAP

cat > "$T/mockbin/sacct" <<'SACCT'
#!/usr/bin/env bash
[[ " $* " == *' --format=JobID,State '* ]] || {
  printf 'unexpected accounting format: %s\n' "$*" >&2
  exit 1
}
case " $* " in
  *' 21676885_1 '*) printf '%s\n' '21676885_1|COMPLETED|' '21676885_1.batch|FAILED|' '21676885_1.extern|FAILED|' ;;
  *' 21676885_69 '*) printf '%s\n' '21676885_69|FAILED|' '21676885_69.batch|FAILED|' '21676885_69.extern|COMPLETED|' ;;
  *' 21676885_17 '*) printf '%s\n' '21676885_17|FAILED|' ;;
  *' 21676885_18 '*) printf '%s\n' '21676885_18|TIMEOUT+|' ;;
  *' 21676885_19 '*) printf '%s\n' '21676885_19|CANCELLED by 123|' ;;
  *' 21676885_20 '*) printf '%s\n' '21676885_20.batch|FAILED|' '21676885_20|OUT_OF_MEMORY|' '21676885_20.extern|COMPLETED|' ;;
  *' 98765_3 '*) printf '%s\n' '98765_3|COMPLETED|' '98765_3.batch|COMPLETED|' ;;
esac
SACCT
chmod +x "$T/mockbin/sacct"

[[ $(submission_task_state 21676885 1) == COMPLETED ]]
[[ $(submission_task_state 21676885 69) == FAILED ]]
[[ $(submission_task_state 21676885 17) == FAILED ]]
[[ $(submission_task_state 21676885 18) == TIMEOUT ]]
[[ $(submission_task_state 21676885 19) == CANCELLED ]]
[[ $(submission_task_state 21676885 20) == OUT_OF_MEMORY ]]
[[ -z $(submission_task_state 21676885 999) ]]
[[ $(submission_task_state 98765 3) == COMPLETED ]]
[[ $(sample_array_state ERS14600617) == FAILED ]]
[[ $(sample_array_state COMPLETED_SAMPLE) == COMPLETED ]]
[[ -z $(sample_array_state UNKNOWN_SAMPLE) ]]
[[ $(sample_array_state LEGACY_SAMPLE) == COMPLETED ]]

# Mapping is sample_array_state's only responsibility; state interpretation is
# delegated to the authoritative parser, submission_task_state.
submission_task_state() { printf '%s:%s\n' "$1" "$2"; }
[[ $(sample_array_state ERS14600617) == 21676885:69 ]]
[[ $(sample_array_state LEGACY_SAMPLE) == 98765:3 ]]

[[ $(cat "$T/mockbin/sacct") == *'21676885_1.batch'* ]]
echo 'Submission task state tests passed.'
