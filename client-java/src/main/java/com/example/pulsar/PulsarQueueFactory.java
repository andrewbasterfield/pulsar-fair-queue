package com.example.pulsar;

import com.example.pulsar.impl.PulsarQueueImpl;
import org.apache.pulsar.client.api.PulsarClient;

import java.time.Duration;

public class PulsarQueueFactory {
    public static PulsarQueue create(PulsarClient client, String queueName, String subscriptionName, Duration autoDiscoveryPeriod) {
        return new PulsarQueueImpl(client, queueName, subscriptionName, autoDiscoveryPeriod);
    }
}
