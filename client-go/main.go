package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

var (
	mode      = flag.String("mode", "both", "Mode: produce, consume, or both")
	url       = flag.String("url", "pulsar://localhost:6650", "Pulsar URL")
	queueName = flag.String("queue", "persistent://public/queues/queue", "Queue name")
	subName   = flag.String("sub", "fair-subscription", "Subscription name")
	msgClass  = flag.String("class", "foo", "Message class for production")
	count     = flag.Int("count", 1000, "Total messages to produce")
	batchSize = flag.Int("batch", 10, "Messages per batch")
	workers   = flag.Int("workers", 1, "Number of concurrent workers")
	discovery = flag.Int("discovery", 1000, "Topic discovery interval in milliseconds")
)

func consume(ctx context.Context, queue PulsarQueueConsumer, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	log.Printf("[Consumer-%d] Started", id)

	var receivedCount uint64

	for {
		if ctx.Err() != nil {
			return
		}

		msg, err := queue.Receive(ctx)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("[Consumer-%d] Error receiving: %v", id, err)
			}
			return
		}

		if msg != nil {
			if err := queue.Ack(msg); err != nil {
				log.Printf("[Consumer-%d] Failed to ack: %v", id, err)
			} else {
				newCount := atomic.AddUint64(&receivedCount, 1)
				if newCount%100 == 0 {
					log.Printf("[Consumer-%d] Processed %d messages", id, newCount)
				}
			}
		}
	}
}

func produce(ctx context.Context, producer PulsarQueueProducer, wg *sync.WaitGroup, id int, totalMessages int, batchSize int) {
	defer wg.Done()
	log.Printf("[Producer-%d] Started, producing %d messages in batches of %d", id, totalMessages, batchSize)

	sentCount := 0

	for sentCount < totalMessages {
		if ctx.Err() != nil {
			return
		}

		currentBatch := batchSize
		if totalMessages-sentCount < batchSize {
			currentBatch = totalMessages - sentCount
		}

		messages := make([]*pulsar.ProducerMessage, 0, currentBatch)
		for i := 0; i < currentBatch; i++ {
			payload := fmt.Sprintf("msg-%d-%d", id, sentCount+i)
			messages = append(messages, &pulsar.ProducerMessage{
				Payload: []byte(payload),
			})
		}

		err := producer.Send(ctx, messages, *msgClass)
		if err != nil {
			log.Printf("[Producer-%d] Error sending batch: %v", id, err)
			if ctx.Err() != nil {
				return
			}
		} else {
			sentCount += currentBatch
		}
	}
	log.Printf("[Producer-%d] Completed. Sent %d messages.", id, sentCount)
}

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: *url,
	})
	if err != nil {
		log.Fatalf("Could not connect to Pulsar: %v", err)
	}
	defer client.Close()

	queue := NewPulsarQueueImpl(client, *queueName, *subName, time.Duration(*discovery)*time.Millisecond)

	var wg sync.WaitGroup

	// Consumers
	if *mode == "consume" || *mode == "both" {
		for i := 0; i < *workers; i++ {
			wg.Add(1)
			go consume(ctx, queue.Consumer(nil), &wg, i)
		}
	}

	// Producers
	if *mode == "produce" || *mode == "both" {
		msgsPerWorker := *count / *workers
		if msgsPerWorker == 0 {
			msgsPerWorker = 1
		}

		for i := 0; i < *workers; i++ {
			wg.Add(1)
			go produce(ctx, queue.Producer(), &wg, i, msgsPerWorker, *batchSize)
		}
	}

	log.Println("Running... Press Ctrl+C to stop.")

	if *mode == "produce" {
		// Wait for producers to finish or context cancellation
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-ctx.Done():
			log.Println("Context cancelled")
		case <-done:
			log.Println("Production complete")
			cancel() // Stop nicely
		}
	} else {
		// Consume or Both: Wait for signal
		<-ctx.Done()
		log.Println("Shutting down...")
		wg.Wait()
	}

	log.Println("Shutdown complete.")
}
