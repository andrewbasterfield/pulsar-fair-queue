package com.example.pulsar;

public interface PulsarQueue {
    /**
     * Creates a consumer for a specific message class or a wildcard pattern.
     * If messageClass is null, it consumes from all topics matching the queueName-* pattern.
     */
    PulsarQueueConsumer createConsumer(String messageClass);

    /**
     * Returns a producer interface for sending messages to various message classes.
     */
    PulsarQueueProducer createProducer();
}
