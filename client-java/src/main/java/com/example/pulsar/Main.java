package com.example.pulsar;

import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.pulsar.client.api.Messages;
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
import java.util.concurrent.atomic.AtomicBoolean;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static String mode = "both";
    private static String url = "pulsar://localhost:6650";
    private static String queueName = "persistent://public/queues/queue";
    private static String subName = "fair-subscription";
    private static String classSubTopicPrefix = "foo";
    private static int count = 1000;
    private static int batchSize = 10;
    private static int workers = 1;
    private static int discovery = 60;
    private static int numClassSubTopics = 1;
    private static int maxProducerCreationAttempts = 3;
    private static int maxProducerSendAttempts = 3;
    private static int maxConsumerBatchSize = 100;
    private static int consumerIdleTimeout = 0;
    
    public static void main(String[] args) {
        parseArgs(args);

        log.info("Starting with config: mode={}, url={}, queue={}, sub={}, class={}, topics={}, maxProducerCreationAttempts={}, maxProducerSendAttempts={}, maxConsumerBatchMessages={}, consumerIdleTimeout={}",
                mode, url, queueName, subName, classSubTopicPrefix, numClassSubTopics, maxProducerCreationAttempts, maxProducerSendAttempts,
            maxConsumerBatchSize, consumerIdleTimeout);

        try (PulsarClient client = PulsarClient.builder().serviceUrl(url).build()) {
            PulsarQueue queue = PulsarQueueFactory.create(client, queueName, subName, Duration.ofSeconds(discovery),
                maxProducerCreationAttempts, maxProducerSendAttempts, maxConsumerBatchSize);
            
            Stats stats = new Stats();
            long startTime = System.currentTimeMillis();
            ScheduledExecutorService scheduler = startStatsReporter(stats, startTime);

            ExecutorService executor = Executors.newCachedThreadPool();
            AtomicBoolean shutdownCalled = new AtomicBoolean(false);

            Runnable shutdownTask = () -> {
                if (shutdownCalled.compareAndSet(false, true)) {
                    log.info("Shutting down...");
                    executor.shutdownNow();
                    scheduler.shutdownNow();
                    printStatsSummary(stats, startTime);
                }
            };

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Received SIGINT, triggering shutdown...");
                shutdownTask.run();
            }));

            List<Runnable> tasks = new ArrayList<>();

            // Consumers
            if ("consume".equals(mode) || "both".equals(mode)) {
                for (int i = 0; i < workers; i++) {
                    int id = i;
                    // We will wrap the task creation to pass the latch later or use a custom Runnable
                    // For now, let's create the latch based on expected count.
                }
            }

            // Consumers
            int consumerCount = ("consume".equals(mode) || "both".equals(mode)) ? workers : 0;
            int producerCount = ("produce".equals(mode) || "both".equals(mode)) ? workers : 0;

            CountDownLatch latch = new CountDownLatch(consumerCount + producerCount);

            if (consumerCount > 0) {
                 for (int i = 0; i < consumerCount; i++) {
                    int id = i;
                    executor.submit(() -> consume(queue, id, stats, latch));
                }
            }

            if (producerCount > 0) {
                int msgsPerWorker = count / workers;
                if (msgsPerWorker == 0) msgsPerWorker = 1;
                List<String> topicSuffixes = generateTopicSuffixes(numClassSubTopics);

                for (int i = 0; i < producerCount; i++) {
                    int id = i;
                    int finalMsgsPerWorker = msgsPerWorker;
                    executor.submit(() -> produce(queue, id, finalMsgsPerWorker, batchSize, stats, topicSuffixes, latch));
                }
            }

            // Wait for all tasks to complete
            latch.await();
            log.info("All tasks completed.");
            shutdownTask.run();

        } catch (Exception e) {
            log.error("Error in main", e);
        }
    }

    private static List<String> generateTopicSuffixes(int count) {
        List<String> suffixes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
             String suffix = java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 7);
             suffixes.add(suffix);
        }
        return suffixes;
    }

    private static void consume(PulsarQueue queue, int id, Stats stats, CountDownLatch latch) {
        log.info("[Consumer-{}] Started", id);
        try {
            while (!Thread.currentThread().isInterrupted()) {
                boolean shouldRetry = true;
                try (PulsarQueueConsumer consumer = queue.createConsumer(null)) {
                    log.info("[Consumer-{}] Connected to {}", id, queueName);
                    long lastMessageTime = System.currentTimeMillis();
                    while (!Thread.currentThread().isInterrupted()) {
                        Messages<byte[]> messages;
                        if (consumerIdleTimeout > 0) {
                            messages = consumer.receiveBatch(1, TimeUnit.SECONDS); 
                            if (messages == null && (System.currentTimeMillis() - lastMessageTime) > (consumerIdleTimeout * 1000L)) {
                                log.info("[Consumer-{}] Idle for {} seconds, shutting down.", id, consumerIdleTimeout);
                                shouldRetry = false;
                                break; 
                            }
                        } else {
                            messages = consumer.receiveBatch();
                        }

                        if (messages != null && messages.size() > 0) {
                            lastMessageTime = System.currentTimeMillis();
                            consumer.ack(messages);
                            stats.receivedMessages.addAndGet(messages.size());
                            for (var message : messages) {
                                stats.receivedTopicsCounts.merge(message.getTopicName(), 1L, Long::sum);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("[Consumer-{}] Error, retrying in 5s...", id, e);
                    try {
                        TimeUnit.SECONDS.sleep(5);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                
                if (!shouldRetry) {
                    break;
                }
            }
        } finally {
            log.info("[Consumer-{}] Stopped", id);
            latch.countDown();
        }
    }

    private static void produce(PulsarQueue queue, int id, int totalMessages, int batchSize, Stats stats, List<String> topicSuffixes, CountDownLatch latch) {
        log.info("[Producer-{}] Started, producing {} messages", id, totalMessages);
        try {
            try (PulsarQueueProducer producer = queue.createProducer()) {
                int sentCount = 0;
                Random r = new Random(System.nanoTime() + id);

                while (sentCount < totalMessages) {
                    int currentBatch = Math.min(batchSize, totalMessages - sentCount);
                    
                    String className = classSubTopicPrefix;
                    if (numClassSubTopics > 1) {
                        String suffix = topicSuffixes.get(r.nextInt(numClassSubTopics));
                        className = String.format("%s-%s", classSubTopicPrefix, suffix);
                    }

                    List<String> batch = new ArrayList<>(currentBatch);
                    for (int i = 0; i < currentBatch; i++) {
                        batch.add(String.format("msg-%d-%d", id, sentCount + i));
                    }

                    producer.send(batch, className);
                    sentCount += currentBatch;
                    stats.sentMessages.addAndGet(currentBatch);
                    stats.sentTopicsCount.merge(className, (long) currentBatch, Long::sum);
                }
                log.info("[Producer-{}] Completed. Sent {} messages.", id, sentCount);
            } catch (Exception e) {
                log.error("[Producer-{}] Error", id, e);
            }
        } finally {
            latch.countDown();
        }
    }

    private static void parseArgs(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--mode=")) {
                mode = arg.split("=")[1];
            } else if (arg.startsWith("--url=")) {
                url = arg.split("=")[1];
            } else if (arg.startsWith("--queue=")) {
                queueName = arg.split("=")[1];
            } else if (arg.startsWith("--sub=")) {
                subName = arg.split("=")[1];
            } else if (arg.startsWith("--class-sub-topic-prefix=")) {
                classSubTopicPrefix = arg.split("=")[1];
            } else if (arg.startsWith("--count=")) {
                count = Integer.parseInt(arg.split("=")[1]);
            } else if (arg.startsWith("--batch=")) {
                batchSize = Integer.parseInt(arg.split("=")[1]);
            } else if (arg.startsWith("--workers=")) {
                workers = Integer.parseInt(arg.split("=")[1]);
            } else if (arg.startsWith("--discovery=")) {
                discovery = Integer.parseInt(arg.split("=")[1]);
            } else if (arg.startsWith("--class-sub-topics=")) {
                numClassSubTopics = Integer.parseInt(arg.split("=")[1]);
            } else if (arg.startsWith("--max-producer-creation-attempts=")) {
                maxProducerCreationAttempts = Integer.parseInt(arg.split("=")[1]);
            } else if (arg.startsWith("--max-producer-send-attempts=")) {
                maxProducerSendAttempts = Integer.parseInt(arg.split("=")[1]);
            } else if (arg.startsWith("--max-consumer-batch-messages=")) {
                maxConsumerBatchSize = Integer.parseInt(arg.split("=")[1]);
            } else if (arg.startsWith("--consumer-idle-timeout-seconds=")) {
                consumerIdleTimeout = Integer.parseInt(arg.split("=")[1]);
            } else if (arg.equals("--help")) {
                printUsage(System.out);
                System.exit(0);
            } else {
                log.warn("Unknown argument: {}", arg);
                printUsage(System.err);
                System.exit(1);
            }
        }
    }

    private static void printUsage(PrintStream ps) {
        ps.println("Usage: java -jar client.jar [options]");
        ps.println("Options:");
        ps.println("  --mode=<mode>                                 Mode of operation: produce, consume, both (default: " + mode + ")");
        ps.println("  --url=<url>                                   Pulsar service URL (default: " + url + ")");
        ps.println("  --queue=<queue>                               Queue name (default: " + queueName + ")");
        ps.println("  --sub=<subName>                               Subscription name (default: " + subName + ")");
        ps.println("  --class-sub-topic-prefix=<prefix>             Prefix for class sub-topics (default: " + classSubTopicPrefix + ")");
        ps.println("  --count=<count>                               Total number of messages to produce (default: " + count + ")");
        ps.println("  --batch=<batchSize>                           Producer batch size (default: " + batchSize + ")");
        ps.println("  --workers=<workers>                           Number of worker threads (default: " + workers + ")");
        ps.println("  --discovery=<seconds>                         Discovery interval in seconds (default: " + discovery + ")");
        ps.println("  --class-sub-topics=<num>                      Number of class sub-topics (default: " + numClassSubTopics + ")");
        ps.println("  --max-producer-creation-attempts=<attempts>   Max attempts to create a producer (default: " + maxProducerCreationAttempts + ")");
        ps.println("  --max-producer-send-attempts=<attempts>       Max attempts to send a message (default: " + maxProducerSendAttempts + ")");
        ps.println("  --max-consumer-batch-messages=<size>          Max messages per consumer batch (default: " + maxConsumerBatchSize + ")");
        ps.println("  --consumer-idle-timeout-seconds=<seconds>     Consumer idle timeout in seconds (0 to disable, default: " + consumerIdleTimeout + ")");
        ps.println("  --help                                        Show this help message");
    }

    static class Stats {
        AtomicLong sentMessages = new AtomicLong(0);
        AtomicLong receivedMessages = new AtomicLong(0);
        Map<String, Long> sentTopicsCount = new ConcurrentHashMap<>();
        Map<String, Long> receivedTopicsCounts = new ConcurrentHashMap<>();
    }

    private static ScheduledExecutorService startStatsReporter(Stats stats, long startTime) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        final AtomicLong lastSent = new AtomicLong(0);
        final AtomicLong lastReceived = new AtomicLong(0);

        scheduler.scheduleAtFixedRate(() -> {
            printStats(stats, lastSent, lastReceived, startTime);
        }, 1, 1, TimeUnit.SECONDS);
        return scheduler;
    }

    private static void printStats(Stats stats, final AtomicLong lastSent, final AtomicLong lastReceived, final long startTime) {
        long currSent = stats.sentMessages.get();
        long currReceived = stats.receivedMessages.get();

        double elapsed = (System.currentTimeMillis() - startTime) / 1000.0;
        double rateSent = currSent - lastSent.getAndSet(currSent);
        double rateReceived = currReceived - lastReceived.getAndSet(currReceived);

        log.info("Stats: Sent {} ({}/s), Received {} ({}/s) | Avg: {} sent/s, {} recv/s | Topics: {}",
                currSent, rateSent, currReceived, rateReceived,
                String.format("%.1f", currSent / elapsed),
                String.format("%.1f", currReceived / elapsed),
                stats.receivedTopicsCounts.size());
    }

    private static void printStatsSummary(Stats stats, long startTime) {
        long currSent = stats.sentMessages.get();
        long currReceived = stats.receivedMessages.get();
        double elapsed = (System.currentTimeMillis() - startTime) / 1000.0;

        stats.sentTopicsCount.forEach((topic, count) -> log.info("Topic '{}' sent {} messages", topic, count));
        stats.receivedTopicsCounts.forEach((topic, count) -> log.info("Topic '{}' received {} messages", topic, count));

        log.info("Stats Summary: Sent {} | Received {} | Avg: {} sent/s, {} recv/s | Topics: {}",
                currSent, currReceived,
                String.format("%.1f", currSent / elapsed),
                String.format("%.1f", currReceived / elapsed),
                stats.receivedTopicsCounts.size());
    }
}
