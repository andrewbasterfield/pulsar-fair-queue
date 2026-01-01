#!/bin/sh

set -xe

. ./config.sh

# ensure there is a subscription present to avoid immediate expiry with set-retention --size=0 --time=0 (required for topic cleanup)
$PULSAR_ADMIN topics create-subscription "$TOPIC_NOISY" --subscription="$SUBSCRIPTION_NAME" --messageId=earliest | true

$PULSAR_CLIENT produce "$TOPIC_NOISY" --num-produce=1000 --rate=100 --messages="This is a noisy message"
