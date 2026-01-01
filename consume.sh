#!/bin/sh

# https://pulsar.apache.org/docs/2.7.2/reference-cli-tools/#consume

set -xe

. ./config.sh

$PULSAR_CLIENT_RUN consume "$TOPIC_REGEX" --subscription-name="$SUBSCRIPTION_NAME" --subscription-type=Shared --num-messages=0 --regex --queue-size=1 --subscription-position=Earliest
