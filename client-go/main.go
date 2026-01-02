package main

import (
	"context"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	_ "github.com/apache/pulsar-client-go/pulsar"
)

func main() {

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})

	if err != nil {
		panic(fmt.Errorf("could not connect to Pulsar: %v", err))
	}

	queue := NewPulsarQueueImpl(client, "persistent://public/queues/queue", "fair-subscription")

	err = queue.Producer().Send(context.Background(), []*pulsar.ProducerMessage{
		{
			Payload: []byte("Hello World"),
		},
	}, "foo")

	if err != nil {
		panic(fmt.Errorf("could not send message: %v", err))
	}

}
