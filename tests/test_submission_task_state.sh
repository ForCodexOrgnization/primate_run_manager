#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test_helper.sh"
new_env
source "$REPO/lib/common.sh"

cat > "$T/mockbin/sacct" <<'SACCT'
#!/usr/bin/env bash
case " $* " in
  *' 21676885_1 '*) printf '%s\n' '21676885_1|COMPLETED|' '21676885_1.batch|COMPLETED|' '21676885_1.extern|COMPLETED|' ;;
  *' 21676885_17 '*) printf '%s\n' '21676885_17|FAILED|' ;;
  *' 21676885_18 '*) printf '%s\n' '21676885_18|TIMEOUT+|' ;;
  *' 21676885_19 '*) printf '%s\n' '21676885_19|CANCELLED by 123|' ;;
  *' 21676885_20 '*) printf '%s\n' '21676885_20.batch|FAILED|' '21676885_20|OUT_OF_MEMORY|' '21676885_20.extern|COMPLETED|' ;;
  *' 98765_3 '*) printf '%s\n' '98765_3|COMPLETED|' '98765_3.batch|COMPLETED|' ;;
esac
SACCT
chmod +x "$T/mockbin/sacct"

[[ $(submission_task_state 21676885 1) == COMPLETED ]]
[[ $(submission_task_state 21676885 17) == FAILED ]]
[[ $(submission_task_state 21676885 18) == TIMEOUT ]]
[[ $(submission_task_state 21676885 19) == CANCELLED ]]
[[ $(submission_task_state 21676885 20) == OUT_OF_MEMORY ]]
[[ -z $(submission_task_state 21676885 999) ]]
[[ $(submission_task_state 98765 3) == COMPLETED ]]
[[ $(cat "$T/mockbin/sacct") == *'21676885_1.batch'* ]]
echo 'Submission task state tests passed.'
