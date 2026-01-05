package com.example.pulsar.impl;

import com.example.pulsar.PulsarQueueConsumer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
  public Messages<byte[]> receiveBatch(long timeout, TimeUnit unit) throws PulsarClientException {
    try {
      return consumer.batchReceiveAsync().get(timeout, unit);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PulsarClientException(e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof PulsarClientException) {
        throw (PulsarClientException) e.getCause();
      }
      throw new PulsarClientException(e.getCause());
    } catch (TimeoutException e) {
      // Return null to indicate a timeout without messages
      return null;
    }
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
