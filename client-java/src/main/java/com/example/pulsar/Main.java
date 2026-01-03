package com.example.pulsar;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static String mode = "both";
    private static String url = "pulsar://localhost:6650";
    private static String queueName = "persistent://public/queues/queue";
    private static String subName = "fair-subscription";
    private static String msgClass = "foo";
    private static int count = 1000;
    private static int batchSize = 10;
    private static int workers = 1;
    private static int discovery = 60;
    private static int numTopics = 1;
    private static int maxProducerCreationAttempts = 3;
    private static int maxProducerSendAttempts = 3;
    private static int maxConsumerBatchSize = 100; // New parameter for batch consumption

    public static void main(String[] args) {
        parseArgs(args);

        log.info("Starting with config: mode={}, url={}, queue={}, sub={}, class={}, topics={}, maxProducerCreationAttempts={}, maxProducerSendAttempts={}, maxConsumerBatchMessages={}",
                mode, url, queueName, subName, msgClass, numTopics, maxProducerCreationAttempts, maxProducerSendAttempts,
            maxConsumerBatchSize);

        try (PulsarClient client = PulsarClient.builder().serviceUrl(url).build()) {
            PulsarQueue queue = PulsarQueueFactory.create(client, queueName, subName, Duration.ofSeconds(discovery),
                maxProducerCreationAttempts, maxProducerSendAttempts, maxConsumerBatchSize);
            
            Stats stats = new Stats();
            ScheduledExecutorService scheduler = startStatsReporter(stats);

            ExecutorService executor = Executors.newCachedThreadPool();
            List<Runnable> tasks = new ArrayList<>();

            // Consumers
            if ("consume".equals(mode) || "both".equals(mode)) {
                for (int i = 0; i < workers; i++) {
                    int id = i;
                    tasks.add(() -> consume(queue, id, stats));
                }
            }

            // Producers
            if ("produce".equals(mode) || "both".equals(mode)) {
                int msgsPerWorker = count / workers;
                if (msgsPerWorker == 0) msgsPerWorker = 1;

                List<String> topicSuffixes = generateTopicSuffixes(numTopics);

                for (int i = 0; i < workers; i++) {
                    int id = i;
                    int finalMsgsPerWorker = msgsPerWorker;
                    tasks.add(() -> produce(queue, id, finalMsgsPerWorker, batchSize, stats, topicSuffixes));
                }
            }
            
            // Execute all tasks
            for (Runnable task : tasks) {
                executor.submit(task);
            }

            // Wait logic
            if ("produce".equals(mode)) {
                executor.shutdown();
                if (executor.awaitTermination(1, TimeUnit.HOURS)) {
                    log.info("Production complete");
                }
                scheduler.shutdownNow(); // Shut down the stats reporter
            } else {
                // Keep running until interrupted
                synchronized (Main.class) {
                    Main.class.wait();
                }
            }

        } catch (Exception e) {
            log.error("Error in main", e);
        }
    }

    private static List<String> generateTopicSuffixes(int count) {
        List<String> suffixes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
             // Generate a random 7-char hex string (like a git shortref)
             String suffix = java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 7);
             suffixes.add(suffix);
        }
        return suffixes;
    }

    private static void consume(PulsarQueue queue, int id, Stats stats) {
        log.info("[Consumer-{}] Started", id);
        try (PulsarQueueConsumer consumer = queue.createConsumer(null)) {
            while (true) {
                // Receive messages in batches
                var messages = consumer.receiveBatch();
                if (messages != null && messages.size() > 0) {
                    stats.receivedMessages.addAndGet(messages.size());
                    consumer.ack(messages); // Acknowledge the entire batch
                }
            }
        } catch (Exception e) {
            log.error("[Consumer-{}] Error", id, e);
        }
    }

    private static void produce(PulsarQueue queue, int id, int totalMessages, int batchSize, Stats stats, List<String> topicSuffixes) {
        log.info("[Producer-{}] Started, producing {} messages", id, totalMessages);
        try (PulsarQueueProducer producer = queue.createProducer()) {
            int sentCount = 0;
            Random r = new Random(System.nanoTime() + id);

            while (sentCount < totalMessages) {
                int currentBatch = Math.min(batchSize, totalMessages - sentCount);
                
                String className = msgClass;
                if (numTopics > 1) {
                    String suffix = topicSuffixes.get(r.nextInt(numTopics));
                    className = String.format("%s-%s", msgClass, suffix);
                }

                List<String> batch = new ArrayList<>(currentBatch);
                for (int i = 0; i < currentBatch; i++) {
                    batch.add(String.format("msg-%d-%d", id, sentCount + i));
                }

                producer.send(batch, className);
                sentCount += currentBatch;
                stats.sentMessages.addAndGet(currentBatch);
            }
            log.info("[Producer-{}] Completed. Sent {} messages.", id, sentCount);
        } catch (Exception e) {
            log.error("[Producer-{}] Error", id, e);
        }
    }

    private static void parseArgs(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--mode=")) mode = arg.split("=")[1];
            if (arg.startsWith("--url=")) url = arg.split("=")[1];
            if (arg.startsWith("--queue=")) queueName = arg.split("=")[1];
            if (arg.startsWith("--sub=")) subName = arg.split("=")[1];
            if (arg.startsWith("--class=")) msgClass = arg.split("=")[1];
            if (arg.startsWith("--count=")) count = Integer.parseInt(arg.split("=")[1]);
            if (arg.startsWith("--batch=")) batchSize = Integer.parseInt(arg.split("=")[1]);
            if (arg.startsWith("--workers=")) workers = Integer.parseInt(arg.split("=")[1]);
            if (arg.startsWith("--discovery=")) discovery = Integer.parseInt(arg.split("=")[1]);
            if (arg.startsWith("--topics=")) numTopics = Integer.parseInt(arg.split("=")[1]);
            if (arg.startsWith("--max-producer-creation-attempts=")) maxProducerCreationAttempts = Integer.parseInt(arg.split("=")[1]);
            if (arg.startsWith("--max-producer-send-attempts=")) maxProducerSendAttempts = Integer.parseInt(arg.split("=")[1]);
            if (arg.startsWith("--max-consumer-batch-messages=")) maxConsumerBatchSize = Integer.parseInt(arg.split("=")[1]);
        }
    }

    static class Stats {
        AtomicLong sentMessages = new AtomicLong(0);
        AtomicLong receivedMessages = new AtomicLong(0);
    }

    private static ScheduledExecutorService startStatsReporter(Stats stats) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        final long[] lastSent = {0};
        final long[] lastReceived = {0};
        final long startTime = System.currentTimeMillis();

        scheduler.scheduleAtFixedRate(() -> {
            long currSent = stats.sentMessages.get();
            long currReceived = stats.receivedMessages.get();
            
            double elapsed = (System.currentTimeMillis() - startTime) / 1000.0;
            double rateSent = currSent - lastSent[0];
            double rateReceived = currReceived - lastReceived[0];
            
            log.info("Stats: Sent {} ({}/s), Received {} ({}/s) | Avg: {} sent/s, {} recv/s",
                    currSent, rateSent, currReceived, rateReceived,
                    String.format("%.1f", currSent / elapsed),
                    String.format("%.1f", currReceived / elapsed));
            
            lastSent[0] = currSent;
            lastReceived[0] = currReceived;
        }, 1, 1, TimeUnit.SECONDS);
        return scheduler;
    }
}
