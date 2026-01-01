#!/bin/sh

set -xe

. ./config.sh

# ensure there is a subscription present to avoid immediate expiry with set-retention --size=0 --time=0 (required for topic cleanup)
$PULSAR_ADMIN topics create-subscription "$TOPIC_QUIET" --subscription="$SUBSCRIPTION_NAME" --messageId=earliest | true

$PULSAR_CLIENT produce "$TOPIC_QUIET" --num-produce=10 --messages="This is a quiet message"
