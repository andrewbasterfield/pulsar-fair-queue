# Pulsar Fair Queueing (Java Client)

This folder contains a Java implementation of the "Fair Queueing" pattern for Apache Pulsar, designed to address specific limitations found in the Go client implementation (specifically regarding infinite topic resurrection).

## Overview

The library implements a pattern where:
1.  **Producers** write to specific "message classes" (mapped to individual Pulsar topics).
2.  **Consumers** use a wildcard (regex) subscription to consume from *all* matching class-topics fairly.
3.  **Infrastructure is Lazy**: Topics and subscriptions are created on demand.
4.  **Cleanup is Automatic**: Topics are deleted by the broker when inactive (Zero-Retention policy).

## Key Features & Fixes

### 1. Zombie Topic Fix (Server-Side Filtering)
Unlike the Go client, which relies on client-side polling, this Java client utilizes **Server-Side Regex Subscriptions** (`RegexSubscriptionMode.PersistentOnly`).
*   **Mechanism**: The client registers a watcher with the broker (`CommandWatchTopicList`).
*   **Benefit**: When the broker deletes an inactive topic, it pushes a notification to the client. The client stops listening to that topic immediately without attempting to reconnect. This prevents the "Infinite Resurrection" loop where a client's reconnection attempt accidentally recreates a topic that was just deleted.

### 2. Strict Producer Safety
To support the aggressive **Zero-Retention Policy** (where messages are dropped immediately if no subscription exists), the Producer enforces a strict contract:
*   **Ensure Subscription**: Before sending a message to a new topic, it temporarily connects a consumer to ensure the subscription is created.
*   **Retry Logic**: It includes a retry mechanism with backoff for producer creation to handle race conditions where auto-created partitioned topic metadata might not be immediately available.

## Usage

### Prerequisites
*   Java 11+
*   Running Pulsar Cluster (see root `README.md`)

### Building
The project uses Gradle.

```bash
cd client-java
./gradlew installDist
```

### Running the CLI Tool
The build generates an executable script in `build/install/pulsar-fair-queue/bin/`.

**Producer Mode:**
Sends 1000 messages distributed across 5 random message classes.
```bash
./client-java/build/install/pulsar-fair-queue/bin/pulsar-fair-queue --mode=produce --count=1000 --topics=5
```

**Consumer Mode:**
Starts a wildcard consumer for all topics.
```bash
./client-java/build/install/pulsar-fair-queue/bin/pulsar-fair-queue --mode=consume
```

**Both:**
Runs producer and consumer in the same process.
```bash
./client-java/build/install/pulsar-fair-queue/bin/pulsar-fair-queue --mode=both --count=100
```

## Project Structure

*   `src/main/java/com/example/pulsar/`
    *   `PulsarQueue.java`: Main interface.
    *   `PulsarQueueFactory.java`: Entry point for creating instances.
    *   `impl/PulsarQueueImpl.java`: Implementation of the queue logic (Consumer creation).
    *   `impl/PulsarQueueProducerImpl.java`: Implementation of the safe producer logic.
    *   `Main.java`: CLI demo application.
*   `src/test/java/`: JUnit 5 tests.
