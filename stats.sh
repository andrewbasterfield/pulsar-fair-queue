#!/bin/sh

set -e
#set -xe

. ./config.sh

TS=$(date +%Y%m%d-%H%M%S)

$PULSAR_ADMIN topics list-partitioned-topics "$NAMESPACE" | while read topic; do
  stats=$($PULSAR_ADMIN </dev/null topics partitioned-stats "$topic" | tee $(basename "$topic")-$TS.json)
  backlog=$(echo $stats | jq .backlogSize)
  subscriptions=$(echo $stats | jq -c '.subscriptions | keys')
  consumers=$(echo $stats | jq -c .subscriptions[].consumers[].connectedSince)
  echo "PARTITIONED_TOPIC: $topic, BACKLOG: $backlog, SUBSCRIPTIONS: $subscriptions, CONSUMERS: $consumers"
  echo "\t"$PULSAR_ADMIN </dev/null topics unsubscribe "$topic" -s $subscriptions
  echo "\t"$PULSAR_ADMIN </dev/null topics delete-partitioned-topic "$topic"
done

$PULSAR_ADMIN topics list "$NAMESPACE" | while read topic; do
  stats=$($PULSAR_ADMIN </dev/null topics stats "$topic" | tee $(basename "$topic")-$TS.json)
  backlog=$(echo $stats | jq .backlogSize)
  subscriptions=$(echo $stats | jq -c '.subscriptions | keys')
  consumers=$(echo $stats | jq -c .subscriptions[].consumers[].connectedSince)
  stats_internal=$($PULSAR_ADMIN </dev/null topics stats-internal "$topic" | tee $(basename "$topic")-internal-$TS.json)
  state=$(echo $stats_internal | jq -c .state)
  last_write=$(echo $stats_internal | jq -c .lastLedgerCreatedTimestamp)
  cursor_active=$(echo $stats_internal | jq -c .cursors[].active)
  echo "TOPIC: $topic, BACKLOG: $backlog, SUBSCRIPTIONS: $subscriptions, CONSUMERS: $consumers, STATE: $state, LAST_WRITE: $last_write, CURSOR_ACTIVE: $cursor_active"
  echo "\t"$PULSAR_ADMIN </dev/null topics unsubscribe "$topic" -s $subscriptions
  echo "\t"$PULSAR_ADMIN </dev/null topics delete "$topic"
done

