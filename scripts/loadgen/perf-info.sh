#!/usr/bin/env bash

SERVER="nats://localhost:30422"
STREAM="LOGS_INPUT"
CONSUMER="${1:-receiver-batch}"
INTERVAL=1

previous=""

while true; do
  info="$(nats --server "$SERVER" consumer info "$STREAM" "$CONSUMER")"

  delivered=$(
    printf '%s\n' "$info" |
      sed -nE 's/.*Last Delivered Message: Consumer sequence: ([0-9,]+).*/\1/p' |
      tr -d ','
  )

  unprocessed=$(
    printf '%s\n' "$info" |
      sed -nE 's/.*Unprocessed Messages: *([0-9,]+).*/\1/p' |
      tr -d ','
  )

  outstanding_acks=$(
    printf '%s\n' "$info" |
      sed -nE 's/.*Outstanding Acks: *([0-9,]+).*/\1/p' |
      tr -d ','
  )

  if [[ "$delivered" =~ ^[0-9]+$ ]]; then
    if [[ -n "$previous" ]]; then
      delta=$((delivered - previous))
      rate=$((delta / INTERVAL))

      printf '%s delivered=%d rate=%d msg/s unprocessed=%s outstanding_acks=%s\n' \
        "$(date '+%H:%M:%S')" \
        "$delivered" \
        "$rate" \
        "${unprocessed:-N/A}" \
        "${outstanding_acks:-N/A}"
    fi

    previous="$delivered"
  fi

  sleep "$INTERVAL"
done