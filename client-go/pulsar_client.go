package main

import (
	"context"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	_ "github.com/apache/pulsar-client-go/pulsar"
)

// PulsarQueueConsumer defines the interface for consuming messages from a Pulsar queue.
type PulsarQueueConsumer interface {
	// Receive method blocks until a message is available to be received.
	Receive() (pulsar.Message, error)
}

// PulsarQueueProducer defines the interface for producing messages to Pulsar topics.
type PulsarQueueProducer interface {
	// Send method sends a batch of messages to a Pulsar topic associated with a messageClass.
	// It ensures that a subscription exists for the target topic before sending messages
	// to prevent data loss due to aggressive retention policies.
	Send(ctx context.Context, messages []*pulsar.ProducerMessage, messageClass string) error
}

// PulsarQueue defines the main interface for interacting with the Pulsar fair queueing system.
type PulsarQueue interface {
	// Consumer creates a consumer for a specific message class or a wildcard pattern.
	// If messageClass is nil, it consumes from all topics matching the queueName-* pattern.
	Consumer(messageClass *string) PulsarQueueConsumer
	// Producer returns a producer interface for sending messages to various message classes.
	Producer() PulsarQueueProducer
}

// TopicQueueFormat defines the format string for constructing topic names based on queue name and message class.
// The first %s is for the queueName, and the second %s is for the messageClass.
const TopicQueueFormat = "%s-%s"

// pulsarQueueConsumerImpl implements the PulsarQueueConsumer interface using the Pulsar client library.
type pulsarQueueConsumerImpl struct {
	consumer pulsar.Consumer // The underlying Pulsar consumer instance.
}

// Receive method retrieves a single message from the consumer.
// It blocks until a message is available or an error occurs.
func (c pulsarQueueConsumerImpl) Receive() (pulsar.Message, error) {
	msg, err := c.consumer.Receive(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to receive message: %v", err)
	}
	return msg, nil
}

var _ PulsarQueueConsumer = (*pulsarQueueConsumerImpl)(nil)

// PulsarQueueImpl implements the PulsarQueue interface, providing a client for interacting with Pulsar.
// It manages the underlying Pulsar client and configuration for a specific queue and subscription.
type PulsarQueueImpl struct {
	client           pulsar.Client // The underlying Pulsar client instance.
	queueName        string        // The base name of the queue.
	subscriptionName string        // The name of the subscription to use for consumers and for ensuring producer subscriptions.
}

// Producer returns a PulsarQueueProducer for sending messages.
// It initializes a new producer implementation that manages topic-specific producers.
func (q PulsarQueueImpl) Producer() PulsarQueueProducer {
	return &pulsarQueueProducerImpl{
		client:           q.client,
		queueName:        q.queueName,
		subscriptionName: q.subscriptionName,
		producers:        make(map[string]pulsar.Producer), // Initialize producers map.
	}
}

// NewPulsarQueueImpl creates a new instance of PulsarQueueImpl.
// It initializes the queue with the provided Pulsar client, queue name, and subscription name.
func NewPulsarQueueImpl(client pulsar.Client, queueName string, subscriptionName string) *PulsarQueueImpl {
	return &PulsarQueueImpl{
		client:           client,
		queueName:        queueName,
		subscriptionName: subscriptionName,
	}
}

var _ PulsarQueueConsumer = (*pulsarQueueConsumerImpl)(nil)

// pulsarQueueProducerImpl implements the PulsarQueueProducer interface.
// It manages multiple Pulsar producers, one for each distinct message class,
// and ensures that a subscription exists before messages are sent to a topic.
type pulsarQueueProducerImpl struct {
	client           pulsar.Client              // The underlying Pulsar client instance.
	queueName        string                     // The base name of the queue.
	subscriptionName string                     // The name of the subscription used by consumers.
	producers        map[string]pulsar.Producer // A map of topicName to Pulsar Producer instances.
}

// Send sends a batch of messages to a Pulsar topic determined by the messageClass.
// It first attempts to create a consumer subscription for the topic to ensure its existence,
// then uses or creates a topic-specific producer to send the messages.
func (p *pulsarQueueProducerImpl) Send(ctx context.Context, messages []*pulsar.ProducerMessage, messageClass string) error {
	topicName := fmt.Sprintf(TopicQueueFormat, p.queueName, messageClass)

	// Ensure a subscription exists for the topic before producing.
	// This helps prevent data loss with aggressive retention policies.
	// The Subscribe call will create the topic and subscription if they don't exist.
	// We do not need to actively use the returned consumer, only ensure its creation.
	_, err := p.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topicName,
		SubscriptionName:            p.subscriptionName,
		Type:                        pulsar.Shared,                       // Use Shared subscription type for fair queueing.
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest, // Start consuming from the earliest message.
	})
	if err != nil {
		// Log a warning if subscription creation/lookup fails but continue,
		// as the subscription might already exist, and the client might
		// return a non-fatal error in such cases. In a production system,
		// more robust error handling and type checking might be necessary.
		fmt.Printf("Warning: Failed to ensure subscription for topic %s: %v\n", topicName, err)
	}

	// Retrieve or create a producer for the specific topic.
	producer, ok := p.producers[topicName]
	if !ok {
		newProducer, err := p.client.CreateProducer(pulsar.ProducerOptions{
			Topic: topicName, // Associate producer with the specific topic.
		})
		if err != nil {
			return fmt.Errorf("failed to create producer for topic %s: %v", topicName, err)
		}
		producer = newProducer
		p.producers[topicName] = producer // Store the new producer for reuse.
	}

	// Send each message in the batch.
	for _, message := range messages {
		_, err := producer.Send(ctx, message)
		if err != nil {
			return fmt.Errorf("failed to send message to topic %s: %v", topicName, err)
		}
	}
	return nil
}

var _ PulsarQueueProducer = (*pulsarQueueProducerImpl)(nil)

var _ PulsarQueue = (*PulsarQueueImpl)(nil)

// Consumer creates a PulsarQueueConsumer instance for consuming messages.
// If messageClass is provided, it subscribes to topics matching the specific class (e.g., "queueName-messageClass").
// If messageClass is nil, it subscribes to all topics matching a wildcard pattern (e.g., "queueName-*").
func (q PulsarQueueImpl) Consumer(messageClass *string) PulsarQueueConsumer {

	opts := pulsar.ConsumerOptions{
		TopicsPattern: fmt.Sprintf(TopicQueueFormat, q.queueName, "*"), // Default to wildcard consumption.
	}

	if messageClass != nil {
		opts = pulsar.ConsumerOptions{
			TopicsPattern: fmt.Sprintf(TopicQueueFormat, q.queueName, *messageClass), // Specific message class.
		}
	}

	opts.SubscriptionName = q.subscriptionName                             // Use the predefined subscription name.
	opts.Type = pulsar.Shared                                              // Use a Shared subscription to allow multiple consumers.
	opts.SubscriptionInitialPosition = pulsar.SubscriptionPositionEarliest // Start consuming from the earliest available message.

	consumer, err := q.client.Subscribe(opts)

	if err != nil {
		// Panic if subscription fails, as a consumer cannot function without a valid subscription.
		panic(fmt.Errorf("subscribe to topic %s failed: %v", q.queueName, err))
	}

	return &pulsarQueueConsumerImpl{
		consumer: consumer, // Return a new consumer wrapper.
	}
}
