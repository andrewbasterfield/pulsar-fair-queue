#!/bin/sh

set -xe

. ./config.sh

#$PULSAR_ADMIN brokers update-dynamic-config --config brokerDeleteInactiveTopicsEnabled --value=true
#$PULSAR_ADMIN brokers update-dynamic-config --config brokerDeleteInactiveTopicsFrequencySeconds --value=60
#$PULSAR_ADMIN brokers update-dynamic-config --config brokerDeleteInactiveTopicsMaxInactiveDurationSeconds --value=120
#$PULSAR_ADMIN brokers update-dynamic-config --config brokerDeleteInactiveTopicsMode --value=delete_when_subscriptions_caught_up
#$PULSAR_ADMIN brokers update-dynamic-config --config brokerDeleteInactivePartitionedTopicMetadataEnabled --value=true

# docker-compose exec broker bin/pulsar-admin brokers get-all-dynamic-config | egrep 'brokerDeleteInactiveTopicsEnabled|brokerDeleteInactiveTopicsFrequencySeconds|brokerDeleteInactiveTopicsMaxInactiveDurationSeconds|brokerDeleteInactiveTopicsMode|brokerDeleteInactivePartitionedTopicMetadataEnabled'


$PULSAR_ADMIN namespaces create "$NAMESPACE" | true
#$PULSAR_ADMIN namespaces set-retention "$NAMESPACE" --size -1 --time -1
# Setting --size 0 and --time 0 will make any messages not backlogged on a subscription get immediately expired. This is both ack'd messages and messages to a topic with no subscription. Thus we must ensure there is always a subscription when we produce.
$PULSAR_ADMIN namespaces set-retention "$NAMESPACE" --size 0 --time 0
$PULSAR_ADMIN namespaces set-message-ttl "$NAMESPACE" --messageTTL 0
$PULSAR_ADMIN namespaces set-auto-topic-creation "$NAMESPACE" --enable # --type partitioned --num-partitions 3
$PULSAR_ADMIN namespaces set-inactive-topic-policies "$NAMESPACE" --enable-delete-while-inactive --max-inactive-duration 120s --delete-mode delete_when_subscriptions_caught_up
