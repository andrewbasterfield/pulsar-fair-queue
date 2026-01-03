# Pulsar Fair Queueing (Go Client)

This folder contains a Go client implementation demonstrating the "Fair Queueing" pattern for Apache Pulsar.

## Overview

The core concept implemented here is to dynamically allocate messages into classes, each assigned to its own Pulsar topic. These class-specific topics are then consumed using a single wildcard consumer, aiming for fair service irrespective of the message volume per class.

## Key Features

1.  **Dynamic Topic Creation**: Topics are created on demand for new message classes.
2.  **Wildcard Consumption**: A single consumer subscribes to a regex pattern (e.g., `queue-class-*`) to process messages from all class-specific topics.
3.  **Shared Subscription**: Consumers use a shared subscription type, allowing horizontal scaling and load balancing across multiple consumer instances.
4.  **Zero-Retention Policy**: The Pulsar namespace is configured with zero message retention to enable aggressive topic cleanup for inactive topics.

## Critical Limitation: Infinite Topic Resurrection (PIP-145 Issue)

This Go client implementation, due to the nature of the Go Pulsar client library (specifically versions prior to full PIP-145 server-side support), experiences a known issue: **Infinite Topic Resurrection**, also referred to as "Zombie Topics."

### The Problem
When the Pulsar broker automatically deletes an inactive topic (as per the configured inactivity policy and zero-retention), the Go client's `RegexConsumer` attempts to reconnect to that specific topic. This reconnection attempt, unfortunately, acts as a "new connection" and triggers **Auto-Topic Creation**, thereby resurrecting the topic that was just deleted. This leads to:

*   **Log Noise**: Constant "Consumer closed" -> "Reconnecting" -> "Connected" messages.
*   **Broker Load**: Unnecessary metadata churn (create/delete cycles) in ZooKeeper.
*   **Resource Leak**: Topics that should be garbage-collected remain active in a zombie state as long as the wildcard consumer is running.

### Root Cause (Client-Side Discovery)
The Go client, in the relevant versions, implements regex subscriptions primarily through **client-side polling**. It periodically polls the broker for a list of topics, filters them locally, and then establishes individual consumer connections to each matching topic. When a topic vanishes, the client detects the connection loss and, unaware that the deletion was intentional, tries to re-establish the connection, thus recreating the topic.

### Solution in Java (PIP-145)
The **Java Pulsar client** (and newer Pulsar broker versions) supports **server-side filtering** for regex subscriptions (introduced in **PIP-145: Improve performance of regex subscriptions**). This allows the broker to manage the topic list and push updates to the client. When a topic is deleted, the broker simply stops routing messages, and the client is notified to remove it from its internal list *without* attempting to reconnect, thus preventing the resurrection.

This Go client currently demonstrates the problem that PIP-145 aimed to solve.

## Usage

### Prerequisites
*   Go 1.24+
*   Running Pulsar Cluster (see root `README.md`)

### Building
```bash
cd client-go
go build -o pulsar main.go
```

### Running the CLI Tool

The Go client executable (`pulsar`) supports the following flags:
*   `--mode`: `produce`, `consume`, or `both`
*   `--url`: Pulsar service URL (default: `pulsar://localhost:6650`)
*   `--queue`: Base queue name (default: `persistent://public/queues/queue`)
*   `--sub`: Subscription name (default: `fair-subscription`)
*   `--class`: Message class prefix (default: `foo`)
*   `--count`: Total messages to produce (default: `1000`)
*   `--batch`: Messages per batch (default: `10`)
*   `--workers`: Number of concurrent workers (default: `1`)
*   `--discovery`: Topic discovery interval in seconds (default: `1`)
*   `--topics`: Number of unique topics (message classes) to distribute across (default: `1`)

**Producer Mode:**
Sends messages to dynamically created topics.
```bash
./client-go/pulsar --mode=produce --count=1000 --topics=5
```

**Consumer Mode:**
Starts a wildcard consumer. Run this in a separate terminal.
```bash
./client-go/pulsar --mode=consume
```
*   **Note**: Observe the logs when topics become inactive/deleted by the broker; you will see the "reconnecting" and "resurrecting" behavior described above.

**Both (Producer and Consumer in one process):**
```bash
./client-go/pulsar --mode=both --count=100 --workers=2
```

## Project Structure

*   `main.go`: Main application logic and CLI parsing.
*   `pulsar_client.go`: Core Pulsar queueing logic (interfaces and implementation).
*   `pulsar_client_test.go`: Unit tests for the Pulsar client logic.
