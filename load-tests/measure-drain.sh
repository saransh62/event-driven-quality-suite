#!/usr/bin/env bash
# Reports notification-service consumer lag right after a load run and how long it takes to drain.
# Reads lag with kafka-consumer-groups.sh inside the kafka container; writes results/drain.json.
#   load-tests/measure-drain.sh [timeout_seconds]
set -euo pipefail

TIMEOUT="${1:-180}"
GROUP="notification-service"
OUT="$(dirname "$0")/results/drain.json"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if docker compose version >/dev/null 2>&1; then COMPOSE=(docker compose); else COMPOSE=(docker-compose); fi

lag() {
  "${COMPOSE[@]}" -f "$ROOT/docker-compose.yml" exec -T kafka \
    /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group "$GROUP" 2>/dev/null \
    | awk '$2 == "orders.created" && $6 ~ /^[0-9]+$/ { sum += $6 } END { print sum + 0 }'
}

start=$(date +%s)
initial=$(lag)
current=$initial
echo "Consumer lag right after load: $initial"

while [ "$current" -gt 0 ]; do
  if [ $(( $(date +%s) - start )) -ge "$TIMEOUT" ]; then
    echo "Lag still $current after ${TIMEOUT}s" >&2
    printf '{"group":"%s","initialLag":%s,"finalLag":%s,"drainSeconds":null,"drained":false}\n' \
      "$GROUP" "$initial" "$current" > "$OUT"
    exit 1
  fi
  sleep 1
  current=$(lag)
done

elapsed=$(( $(date +%s) - start ))
echo "Lag drained to 0 in ${elapsed}s"
printf '{"group":"%s","initialLag":%s,"finalLag":0,"drainSeconds":%s,"drained":true}\n' \
  "$GROUP" "$initial" "$elapsed" > "$OUT"
