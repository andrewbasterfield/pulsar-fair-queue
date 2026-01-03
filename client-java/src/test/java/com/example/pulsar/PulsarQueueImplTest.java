package com.example.pulsar;

import com.example.pulsar.impl.PulsarQueueImpl;
import org.apache.pulsar.client.api.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PulsarQueueImplTest {

    @Mock
    private PulsarClient pulsarClient;
    @Mock
    private ConsumerBuilder<byte[]> consumerBuilder;
    @Mock
    private ProducerBuilder<byte[]> producerBuilder;
    @Mock
    private Consumer<byte[]> consumer;
    @Mock
    private Producer<byte[]> producer;

    private PulsarQueueImpl pulsarQueue;
    private final String queueName = "test-queue";
    private final String subName = "test-sub";

    @BeforeEach
    void setUp() throws PulsarClientException {
        // Default mocks setup to avoid NPEs in chains
        lenient().when(pulsarClient.newConsumer()).thenReturn(consumerBuilder);
        lenient().when(pulsarClient.newProducer()).thenReturn(producerBuilder);
        
        lenient().when(consumerBuilder.subscriptionName(anyString())).thenReturn(consumerBuilder);
        lenient().when(consumerBuilder.subscriptionType(any())).thenReturn(consumerBuilder);
        lenient().when(consumerBuilder.subscriptionInitialPosition(any())).thenReturn(consumerBuilder);
        lenient().when(consumerBuilder.autoUpdatePartitions(anyBoolean())).thenReturn(consumerBuilder);
        lenient().when(consumerBuilder.autoUpdatePartitionsInterval(anyInt(), any())).thenReturn(consumerBuilder);
        lenient().when(consumerBuilder.topic(anyString())).thenReturn(consumerBuilder);
        lenient().when(consumerBuilder.topicsPattern(any(Pattern.class))).thenReturn(consumerBuilder);
        lenient().when(consumerBuilder.subscriptionTopicsMode(any())).thenReturn(consumerBuilder);
        lenient().when(consumerBuilder.subscribe()).thenReturn(consumer);

        lenient().when(producerBuilder.topic(anyString())).thenReturn(producerBuilder);
        lenient().when(producerBuilder.create()).thenReturn(producer);

        pulsarQueue = new PulsarQueueImpl(pulsarClient, queueName, subName, Duration.ofSeconds(1));
    }

    @Test
    void testProducerSendSuccess() throws PulsarClientException {
        PulsarQueueProducer queueProducer = pulsarQueue.createProducer();
        String msgClass = "classA";
        String expectedTopic = queueName + "-" + msgClass;
        List<String> messages = Collections.singletonList("msg1");

        queueProducer.send(messages, msgClass);

        // Verify subscription check (consumer creation)
        verify(consumerBuilder).topic(expectedTopic);
        verify(consumerBuilder).subscribe();
        verify(consumer).close();

        // Verify producer creation and send
        verify(producerBuilder).topic(expectedTopic);
        verify(producerBuilder).create();
        verify(producer).send("msg1".getBytes());
    }

    @Test
    void testProducerReuse() throws PulsarClientException {
        PulsarQueueProducer queueProducer = pulsarQueue.createProducer();
        String msgClass = "classA";
        List<String> messages = Collections.singletonList("msg");

        // First send
        queueProducer.send(messages, msgClass);
        // Second send
        queueProducer.send(messages, msgClass);

        // Should only create producer once
        verify(producerBuilder, times(1)).create();
        // Should send twice
        verify(producer, times(2)).send(any());
    }

    @Test
    void testProducerSubscriptionCheckFailure() throws PulsarClientException {
        // Simulate subscription check failure
        when(consumerBuilder.subscribe()).thenThrow(new PulsarClientException("Subscription failed"));

        PulsarQueueProducer queueProducer = pulsarQueue.createProducer();
        String msgClass = "classA";
        
        // Should not throw exception, just log warning and proceed
        assertDoesNotThrow(() -> queueProducer.send(Collections.singletonList("msg"), msgClass));

        // Verify it still attempted to create producer and send
        verify(producerBuilder).create();
        verify(producer).send(any());
    }

    @Test
    void testWildcardConsumer() throws PulsarClientException {
        pulsarQueue.createConsumer(null);

        // Verify topics pattern is used
        verify(consumerBuilder).topicsPattern(any(Pattern.class));
        // Verify regex mode is set to PersistentOnly (server-side filtering optimization)
        verify(consumerBuilder).subscriptionTopicsMode(RegexSubscriptionMode.PersistentOnly);
        verify(consumerBuilder).subscribe();
    }

    @Test
    void testSpecificConsumer() throws PulsarClientException {
        String msgClass = "classA";
        String expectedTopic = queueName + "-" + msgClass;

        pulsarQueue.createConsumer(msgClass);

        // Verify specific topic is used
        verify(consumerBuilder).topic(expectedTopic);
        verify(consumerBuilder, never()).topicsPattern(any(Pattern.class));
        verify(consumerBuilder).subscribe();
    }

    @Test
    void testConsumerReceive() throws PulsarClientException {
        PulsarQueueConsumer queueConsumer = pulsarQueue.createConsumer(null);
        
        queueConsumer.receive();
        verify(consumer).receive();
    }

    @Test
    void testConsumerAck() throws PulsarClientException {
        PulsarQueueConsumer queueConsumer = pulsarQueue.createConsumer(null);
        Message<?> msg = mock(Message.class);

        queueConsumer.ack(msg);
        verify(consumer).acknowledge(msg);
    }

    @Test
    void testConcurrentProducerUsage() throws InterruptedException, PulsarClientException {
        PulsarQueueProducer queueProducer = pulsarQueue.createProducer();
        String msgClass = "classA";
        int threads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    queueProducer.send(Collections.singletonList("msg"), msgClass);
                } catch (PulsarClientException e) {
                    fail("Send failed: " + e.getMessage());
                }
            });
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        // Verify producer was created only once despite concurrent access
        verify(producerBuilder, times(1)).create();
        // Verify send was called 10 times
        verify(producer, times(threads)).send(any());
    }
}
