package com.example.pulsar.impl;

import com.example.pulsar.PulsarQueue;
import com.example.pulsar.PulsarQueueConsumer;
import com.example.pulsar.PulsarQueueProducer;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.regex.Pattern;

/**
 * Implementation of the PulsarQueue interface.
 * <p>
 * This class serves as the factory for creating consumers and producers that adhere to the
 * "Fair Queueing" pattern. It encapsulates the configuration required to connect to Pulsar
 * and manages the lifecycle of subscriptions and topic discovery.
 * </p>
 */
public class PulsarQueueImpl implements PulsarQueue {
    private static final Logger log = LoggerFactory.getLogger(PulsarQueueImpl.class);
    private final PulsarClient client;
    private final String queueName;
    private final String subscriptionName;
    private final Duration autoDiscoveryPeriod;
    private final int maxProducerCreationAttempts;
    private final int maxProducerSendAttempts;
    private final int maxConsumerBatchSize;

    // Matches TopicQueueFormat in Go: "%s-%s"
    static final String TOPIC_QUEUE_FORMAT = "%s-%s";

    /**
     * Constructs a new PulsarQueueImpl.
     *
     * @param client              The PulsarClient instance to use.
     * @param queueName           The base name of the queue (e.g., "persistent://public/queues/queue").
     * @param subscriptionName    The shared subscription name to use for all consumers.
     * @param autoDiscoveryPeriod The interval for discovering new topics (used for pattern auto-discovery fallback).
     * @param maxProducerCreationAttempts  The maximum number of attempts for producer creation.
     */
    public PulsarQueueImpl(PulsarClient client, String queueName, String subscriptionName, Duration autoDiscoveryPeriod, int maxProducerCreationAttempts, int maxProducerSendAttempts, int maxConsumerBatchSize) {
        this.client = client;
        this.queueName = queueName;
        this.subscriptionName = subscriptionName;
        this.autoDiscoveryPeriod = autoDiscoveryPeriod;
        this.maxProducerCreationAttempts = maxProducerCreationAttempts;
        this.maxProducerSendAttempts = maxProducerSendAttempts;
        this.maxConsumerBatchSize = maxConsumerBatchSize;
    }

    /**
     * Creates a consumer for the queue.
     * <p>
     * If {@code messageClass} is provided, it creates a consumer for that specific topic.
     * If {@code messageClass} is {@code null}, it creates a wildcard (regex) consumer that subscribes
     * to all topics matching the pattern {@code queueName-.*}.
     * </p>
     *
     * @param messageClass The specific message class to consume from, or null for all classes.
     * @return A configured {@link PulsarQueueConsumer}.
     */
    @Override
    public PulsarQueueConsumer createConsumer(String messageClass) {
        try {
            ConsumerBuilder<byte[]> builder = client.newConsumer()
                    .subscriptionName(subscriptionName)
                    .subscriptionType(SubscriptionType.Shared) // Shared subscription allows scaling consumers
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest) // Start from beginning if new subscription
                    .autoUpdatePartitions(true) // Automatically handle partition changes
                    // Set a fixed interval for checking partition changes (60s) to reduce log noise,
                    // as dynamic partition resizing is a rare operational event.
                    .autoUpdatePartitionsInterval(60, java.util.concurrent.TimeUnit.SECONDS)
                    // Use the user-provided discovery period for pattern auto-discovery fallback.
                    // Note: Modern Pulsar (PIP-145) uses CommandWatchTopicList for server-side push notifications,
                    // making this polling largely redundant but still useful as a fallback.
                    .patternAutoDiscoveryPeriod((int) autoDiscoveryPeriod.toSeconds(), java.util.concurrent.TimeUnit.SECONDS);

            if (messageClass != null) {
                // Direct topic subscription
                String topicName = String.format(TOPIC_QUEUE_FORMAT, queueName, messageClass);
                builder.topic(topicName);
            } else {
                // Wildcard subscription for Fair Queueing consumer
                String patternString = String.format(TOPIC_QUEUE_FORMAT, queueName, ".*");
                builder.topicsPattern(Pattern.compile(patternString));
                
                // Crucial for solving the "Zombie Topic" issue:
                // PersistentOnly mode combined with server-side regex subscription allows the broker
                // to manage the list of topics. When a topic is deleted (e.g., due to inactivity),
                // the broker notifies the client to remove it, preventing the client from
                // endlessly trying to reconnect and accidentally resurrecting the topic.
                builder.subscriptionTopicsMode(RegexSubscriptionMode.PersistentOnly);
            }

            builder.batchReceivePolicy(BatchReceivePolicy.builder().maxNumMessages(maxConsumerBatchSize).build());

            Consumer<byte[]> consumer = builder.subscribe();
            return new PulsarQueueConsumerImpl(consumer);

        } catch (PulsarClientException e) {
            throw new RuntimeException("Failed to subscribe", e);
        }
    }

    @Override
    public PulsarQueueProducer createProducer() {
      return new PulsarQueueProducerImpl(client, queueName, subscriptionName,
          maxProducerCreationAttempts, maxProducerSendAttempts);
    }
}
