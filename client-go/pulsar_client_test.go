package main

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
)

// --- Mocks ---

type mockPulsarClient struct {
	pulsar.Client // Embed interface to panic on unexpected calls

	subscribeFunc      func(pulsar.ConsumerOptions) (pulsar.Consumer, error)
	createProducerFunc func(pulsar.ProducerOptions) (pulsar.Producer, error)
}

func (m *mockPulsarClient) Subscribe(options pulsar.ConsumerOptions) (pulsar.Consumer, error) {
	if m.subscribeFunc != nil {
		return m.subscribeFunc(options)
	}
	return nil, nil
}

func (m *mockPulsarClient) CreateProducer(options pulsar.ProducerOptions) (pulsar.Producer, error) {
	if m.createProducerFunc != nil {
		return m.createProducerFunc(options)
	}
	return nil, nil
}

var _ pulsar.Client = (*mockPulsarClient)(nil)

type mockPulsarProducer struct {
	pulsar.Producer // Embed interface

	sendFunc func(context.Context, *pulsar.ProducerMessage) (pulsar.MessageID, error)
	topic    string
}

func (m *mockPulsarProducer) Send(ctx context.Context, msg *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	if m.sendFunc != nil {
		return m.sendFunc(ctx, msg)
	}
	return nil, nil
}

func (m *mockPulsarProducer) Topic() string {
	return m.topic
}

func (m *mockPulsarProducer) Close() {
	// No-op for mock
}

var _ pulsar.Producer = (*mockPulsarProducer)(nil)

// --- Tests ---

func TestPulsarQueueImpl_Producer_Send(t *testing.T) {
	queueName := "test-queue"
	subName := "test-sub"
	msgClass := "classA"
	expectedTopic := "test-queue-classA"

	t.Run("Success path: subscribes and creates producer", func(t *testing.T) {
		subscribeCalled := false
		createProducerCalled := false
		sendCalled := false

		mockProd := &mockPulsarProducer{
			topic: expectedTopic,
			sendFunc: func(ctx context.Context, msg *pulsar.ProducerMessage) (pulsar.MessageID, error) {
				sendCalled = true
				return nil, nil
			},
		}

		mockCli := &mockPulsarClient{
			subscribeFunc: func(options pulsar.ConsumerOptions) (pulsar.Consumer, error) {
				subscribeCalled = true
				assert.Equal(t, expectedTopic, options.Topic)
				assert.Equal(t, subName, options.SubscriptionName)
				assert.Equal(t, pulsar.Shared, options.Type)
				assert.Equal(t, pulsar.SubscriptionPositionEarliest, options.SubscriptionInitialPosition)
				return nil, nil // Consumer not used
			},
			createProducerFunc: func(options pulsar.ProducerOptions) (pulsar.Producer, error) {
				createProducerCalled = true
				assert.Equal(t, expectedTopic, options.Topic)
				return mockProd, nil
			},
		}

		pq := NewPulsarQueueImpl(mockCli, queueName, subName)
		producer := pq.Producer()

		messages := []*pulsar.ProducerMessage{{Payload: []byte("test")}}
		err := producer.Send(context.Background(), messages, msgClass)

		assert.NoError(t, err)
		assert.True(t, subscribeCalled, "Subscribe should be called")
		assert.True(t, createProducerCalled, "CreateProducer should be called")
		assert.True(t, sendCalled, "Producer.Send should be called")
	})

	t.Run("Reuses producer", func(t *testing.T) {
		createProducerCalls := 0
		mockProd := &mockPulsarProducer{
			sendFunc: func(ctx context.Context, msg *pulsar.ProducerMessage) (pulsar.MessageID, error) {
				return nil, nil
			},
		}
		mockCli := &mockPulsarClient{
			subscribeFunc: func(options pulsar.ConsumerOptions) (pulsar.Consumer, error) {
				return nil, nil
			},
			createProducerFunc: func(options pulsar.ProducerOptions) (pulsar.Producer, error) {
				createProducerCalls++
				return mockProd, nil
			},
		}

		pq := NewPulsarQueueImpl(mockCli, queueName, subName)
		producer := pq.Producer()
		messages := []*pulsar.ProducerMessage{{Payload: []byte("test")}}

		// First send
		_ = producer.Send(context.Background(), messages, msgClass)
		// Second send
		_ = producer.Send(context.Background(), messages, msgClass)

		assert.Equal(t, 1, createProducerCalls, "CreateProducer should be called only once")
	})

	t.Run("Subscribe error is logged but not fatal", func(t *testing.T) {
		// Note: Since we capture stdout for logging check, or just verify it doesn't return error
		// The code prints to fmt.Printf. We won't assert on stdout here easily,
		// but we assert that the function continues.

		mockProd := &mockPulsarProducer{
			sendFunc: func(ctx context.Context, msg *pulsar.ProducerMessage) (pulsar.MessageID, error) {
				return nil, nil
			},
		}
		mockCli := &mockPulsarClient{
			subscribeFunc: func(options pulsar.ConsumerOptions) (pulsar.Consumer, error) {
				return nil, errors.New("subscribe failed")
			},
			createProducerFunc: func(options pulsar.ProducerOptions) (pulsar.Producer, error) {
				return mockProd, nil
			},
		}

		pq := NewPulsarQueueImpl(mockCli, queueName, subName)
		producer := pq.Producer()
		messages := []*pulsar.ProducerMessage{{Payload: []byte("test")}}

		err := producer.Send(context.Background(), messages, msgClass)
		assert.NoError(t, err, "Send should succeed even if Subscribe fails (mocking 'already exists' scenario)")
	})

	t.Run("CreateProducer error returns error", func(t *testing.T) {
		mockCli := &mockPulsarClient{
			subscribeFunc: func(options pulsar.ConsumerOptions) (pulsar.Consumer, error) {
				return nil, nil
			},
			createProducerFunc: func(options pulsar.ProducerOptions) (pulsar.Producer, error) {
				return nil, errors.New("create producer failed")
			},
		}

		pq := NewPulsarQueueImpl(mockCli, queueName, subName)
		producer := pq.Producer()
		messages := []*pulsar.ProducerMessage{{Payload: []byte("test")}}

		err := producer.Send(context.Background(), messages, msgClass)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "create producer failed")
	})

	t.Run("Send error returns error", func(t *testing.T) {
		mockProd := &mockPulsarProducer{
			sendFunc: func(ctx context.Context, msg *pulsar.ProducerMessage) (pulsar.MessageID, error) {
				return nil, errors.New("send failed")
			},
		}
		mockCli := &mockPulsarClient{
			subscribeFunc: func(options pulsar.ConsumerOptions) (pulsar.Consumer, error) {
				return nil, nil
			},
			createProducerFunc: func(options pulsar.ProducerOptions) (pulsar.Producer, error) {
				return mockProd, nil
			},
		}

		pq := NewPulsarQueueImpl(mockCli, queueName, subName)
		producer := pq.Producer()
		messages := []*pulsar.ProducerMessage{{Payload: []byte("test")}}

		err := producer.Send(context.Background(), messages, msgClass)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "send failed")
	})
}
