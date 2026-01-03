package com.example.pulsar;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;

public interface PulsarQueueConsumer extends AutoCloseable {
    Message<byte[]> receive() throws PulsarClientException;
    Messages<byte[]> receiveBatch() throws PulsarClientException;
    void ack(Message<?> msg) throws PulsarClientException;
    void ack(Messages<?> msgs) throws PulsarClientException;
    void close() throws Exception;
}
