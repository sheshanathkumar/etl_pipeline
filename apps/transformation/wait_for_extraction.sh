#!/bin/bash
set -euo pipefail

RAW_ROOT="/data/raw/credit_card"
MAX_WAIT_SECONDS=${MAX_WAIT_SECONDS:-900}
SLEEP_SECONDS=${SLEEP_SECONDS:-15}

elapsed=0
while [ "$elapsed" -lt "$MAX_WAIT_SECONDS" ]; do
  if ls ${RAW_ROOT}/job_id=*/credit_card_spent_*.csv >/dev/null 2>&1; then
    echo "Extraction output found. Starting transformation."
    break
  fi
  echo "Waiting for extraction output in ${RAW_ROOT} (elapsed: ${elapsed}s)..."
  sleep "$SLEEP_SECONDS"
  elapsed=$((elapsed + SLEEP_SECONDS))
done

if [ "$elapsed" -ge "$MAX_WAIT_SECONDS" ]; then
  echo "Timed out after ${MAX_WAIT_SECONDS}s waiting for extraction output." >&2
  exit 1
fi

exec /opt/spark/bin/spark-submit --master spark://spark-master:7077 transformation.py
