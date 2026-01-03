package com.example.pulsar.impl;

import com.example.pulsar.PulsarQueueConsumer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;

class PulsarQueueConsumerImpl implements PulsarQueueConsumer {

  private final Consumer<byte[]> consumer;

  PulsarQueueConsumerImpl(Consumer<byte[]> consumer) {
    this.consumer = consumer;
  }

  @Override
  public Message<byte[]> receive() throws PulsarClientException {
    return consumer.receive();
  }

  @Override
  public Messages<byte[]> receiveBatch() throws PulsarClientException {
    return consumer.batchReceive();
  }

  @Override
  public void ack(Message<?> msg) throws PulsarClientException {
    consumer.acknowledge(msg);
  }

  @Override
  public void ack(Messages<?> msgs) throws PulsarClientException {
    consumer.acknowledge(msgs);
  }

  @Override
  public void close() throws Exception {
    consumer.close();
  }
}
