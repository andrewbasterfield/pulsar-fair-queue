#!/bin/sh

set -xe

. ./config.sh

TOPIC="$TOPIC_PREFIX/queue-class-"$(pwgen -1)

# ensure there is a subscription present to avoid immediate expiry with set-retention --size=0 --time=0 (required for topic cleanup)
$PULSAR_ADMIN topics create-subscription "$TOPIC" --subscription="$SUBSCRIPTION_NAME" --messageId=earliest | true

$PULSAR_CLIENT produce "$TOPIC" --num-produce=10 --messages="This is a extra message for $TOPIC"
