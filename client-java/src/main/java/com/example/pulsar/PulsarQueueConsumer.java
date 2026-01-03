package com.example.pulsar;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;

public interface PulsarQueueConsumer extends AutoCloseable {
    Message<byte[]> receive() throws PulsarClientException;
    void ack(Message<?> msg) throws PulsarClientException;
    void close() throws Exception;
}
