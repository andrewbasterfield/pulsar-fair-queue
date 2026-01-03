package com.example.pulsar.impl;

import com.example.pulsar.PulsarQueueProducer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the PulsarQueueProducer interface.
 * <p>
 * This class handles producing messages to specific message classes (topics).
 * It implements the critical "Safety Contract" required by the Zero-Retention policy:
 * it ensures a subscription exists *before* producing to a topic to prevent data loss.
 * </p>
 */
class PulsarQueueProducerImpl implements PulsarQueueProducer {

  private static final Logger log = LoggerFactory.getLogger(PulsarQueueProducerImpl.class);

  private final PulsarClient client;
  private final String queueName;
  private final String subscriptionName;
  private final Map<String, Producer<byte[]>> producers = new ConcurrentHashMap<>();

  PulsarQueueProducerImpl(PulsarClient client, String queueName, String subscriptionName) {
    this.client = client;
    this.queueName = queueName;
    this.subscriptionName = subscriptionName;
  }

  /**
   * Sends a batch of messages to a specific message class.
   * <p>
   * This method first checks if a producer for the specific topic already exists in the cache.
   * If not, it performs the "Ensure Subscription" handshake:
   * 1. Temporarily creates a consumer to ensure the topic and subscription exist.
   * 2. Creates and caches the actual producer.
   * </p>
   *
   * @param messages     The list of messages to send.
   * @param messageClass The target message class (used to construct the topic name).
   * @throws PulsarClientException If the operation fails after retries.
   */
  @Override
  public void send(List<String> messages, String messageClass) throws PulsarClientException {
    String topicName = String.format(PulsarQueueImpl.TOPIC_QUEUE_FORMAT, queueName, messageClass);

    Producer<byte[]> producer = producers.computeIfAbsent(topicName, t -> {
      try {
        // --- Safety Mechanism: Ensure Subscription Exists ---
        // With aggressive retention policies (size=0, time=0), messages produced to a topic
        // with no active subscription are immediately deleted.
        // We create a temporary consumer to force the creation of the subscription (and topic)
        // before we write any data.
        try (Consumer<byte[]> consumer = client.newConsumer()
            .topic(t)
            .subscriptionName(subscriptionName)
            .subscriptionType(SubscriptionType.Shared)
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe()) {
          // Immediately close, we just needed to create the subscription metadata.
        } catch (PulsarClientException e) {
          log.warn("Warning: Failed to ensure subscription for topic {}: {}", t,
              e.getMessage());
          // Proceeding, as it might already exist or be a non-fatal error
        }

        // --- Retry Mechanism for Producer Creation ---
        // When a partitioned topic is auto-created by the step above, there can be a race condition
        // where the metadata for individual partitions isn't immediately available to the Producer.
        // We retry a few times with a backoff to handle this transient state.
        int maxRetries = 3;
        for (int i = 0; i < maxRetries; i++) {
            try {
                return client.newProducer()
                    .topic(t)
                    .create();
            } catch (PulsarClientException e) {
                if (i == maxRetries - 1) {
                    throw e; // Throw on last attempt
                }
                log.warn("Failed to create producer for topic {}, retrying... ({}/{})", t, i + 1, maxRetries);
                try {
                    Thread.sleep(100); // Short backoff
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while retrying producer creation", ie);
                }
            }
        }
        throw new RuntimeException("Unreachable code"); // Should not happen
      } catch (PulsarClientException e) {
        throw new RuntimeException("Failed to create producer for topic " + t, e);
      }
    });

    for (String msg : messages) {
      producer.send(msg.getBytes());
    }
  }

  @Override
  public void close() throws Exception {
    for (Producer<byte[]> producer : producers.values()) {
      producer.close();
    }
    producers.clear();
  }
}
