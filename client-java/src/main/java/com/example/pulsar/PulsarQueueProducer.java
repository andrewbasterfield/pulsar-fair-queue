package com.example.pulsar;

import org.apache.pulsar.client.api.PulsarClientException;
import java.util.List;

public interface PulsarQueueProducer extends AutoCloseable {
    /**
     * Sends a batch of messages to a Pulsar topic associated with a messageClass.
     * It ensures that a subscription exists for the target topic before sending messages
     * to prevent data loss due to aggressive retention policies.
     */
    void send(List<String> messages, String messageClass) throws PulsarClientException;
    void close() throws Exception;
}
