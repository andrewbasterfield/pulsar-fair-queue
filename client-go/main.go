package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
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
	msgClass  = flag.String("class", "foo", "Message class prefix for production")
	count     = flag.Int("count", 1000, "Total messages to produce")
	batchSize = flag.Int("batch", 10, "Messages per batch")
	workers   = flag.Int("workers", 1, "Number of concurrent workers")
	discovery = flag.Int("discovery", 1, "Topic discovery interval in seconds")
	numTopics = flag.Int("topics", 1, "Number of unique topics (message classes) to distribute across")
)

type Stats struct {
	sentMessages     uint64
	receivedMessages uint64
}

func reportStats(ctx context.Context, stats *Stats) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastSent, lastReceived uint64
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currSent := atomic.LoadUint64(&stats.sentMessages)
			currReceived := atomic.LoadUint64(&stats.receivedMessages)

			rateSent := float64(currSent - lastSent)
			rateReceived := float64(currReceived - lastReceived)

			elapsed := time.Since(startTime).Seconds()
			avgSent := float64(currSent) / elapsed
			avgReceived := float64(currReceived) / elapsed

			log.Printf("Stats: Sent %d (%.1f/s), Received %d (%.1f/s) | Avg: %.1f sent/s, %.1f recv/s",
				currSent, rateSent, currReceived, rateReceived, avgSent, avgReceived)

			lastSent = currSent
			lastReceived = currReceived
		}
	}
}

func consume(ctx context.Context, queue PulsarQueueConsumer, wg *sync.WaitGroup, id int, stats *Stats) {
	defer wg.Done()
	log.Printf("[Consumer-%d] Started", id)

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
				atomic.AddUint64(&stats.receivedMessages, 1)
			}
		}
	}
}

func produce(ctx context.Context, queue PulsarQueue, wg *sync.WaitGroup, id int, totalMessages int, batchSize int, stats *Stats) {
	defer wg.Done()
	log.Printf("[Producer-%d] Started, producing %d messages in batches of %d", id, totalMessages, batchSize)

	producer := queue.Producer()
	sentCount := 0

	// Local random source to avoid lock contention
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

	for sentCount < totalMessages {
		if ctx.Err() != nil {
			return
		}

		currentBatch := batchSize
		if totalMessages-sentCount < batchSize {
			currentBatch = totalMessages - sentCount
		}

		// Determine message class (topic)
		className := *msgClass
		if *numTopics > 1 {
			classID := r.Intn(*numTopics)
			className = fmt.Sprintf("%s-%d", *msgClass, classID)
		}

		messages := make([]*pulsar.ProducerMessage, 0, currentBatch)
		for i := 0; i < currentBatch; i++ {
			payload := fmt.Sprintf("msg-%d-%d", id, sentCount+i)
			messages = append(messages, &pulsar.ProducerMessage{
				Payload: []byte(payload),
			})
		}

		err := producer.Send(ctx, messages, className)
		if err != nil {
			log.Printf("[Producer-%d] Error sending batch to %s: %v", id, className, err)
			if ctx.Err() != nil {
				return
			}
		} else {
			sentCount += currentBatch
			atomic.AddUint64(&stats.sentMessages, uint64(currentBatch))
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

	queue := NewPulsarQueueImpl(client, *queueName, *subName, time.Duration(*discovery)*time.Second)

	var wg sync.WaitGroup
	stats := &Stats{}

	// Start stats reporter
	go reportStats(ctx, stats)

	// Consumers
	if *mode == "consume" || *mode == "both" {
		for i := 0; i < *workers; i++ {
			wg.Add(1)
			go consume(ctx, queue.Consumer(nil), &wg, i, stats)
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
			go produce(ctx, queue, &wg, i, msgsPerWorker, *batchSize, stats)
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