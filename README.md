# Pulsar Fair Queueing Demo

This project demonstrates a setup for 'fair' message queueing using Apache Pulsar.

## Concept

The core idea is to allocate messages into classes dynamically and lazily. These message classes are unknown at system boot time.

- **Dynamic Class Allocation**: Each message class is assigned to its own Pulsar topic.
- **On-Demand Topic Creation**: Topics are created automatically as new classes are encountered.
- **Wildcard Consumption**: All these class-specific topics are consumed using a single wildcard consumer.
- **Fair Service**: Classes are served fairly, irrespective of the number of messages in that class (e.g., a "noisy" class doesn't starve a "quiet" class).
- **Automatic Cleanup**: Topics are automatically deleted after a period of inactivity (specifically, when they have been empty for 120 seconds and subscriptions are caught up), as configured in `setup.sh`.

## Usage

This project uses Docker Compose to run a local Pulsar cluster.

### Prerequisites
- Docker and Docker Compose

### Scripts
- `config.sh`: Central configuration file defining tenant, namespace, topic names, and command aliases.
- `setup.sh`: Configures the Pulsar namespace and topic policies for auto-creation and auto-deletion.
- `produce-*.sh`: Scripts to generate traffic for different "classes" (noisy, quiet, extra).
- `consume.sh`: Runs the wildcard consumer.
- `stats.sh`: Utility script to inspect topic statistics, backlogs, and consumers, helping to verify system state.

## Configuration & Implementation Details

This section details the specific Pulsar configurations that enable this dynamic workflow.

### Topic Lifecycle Management

**Auto-Creation & Partitioned Topics**
Topics are created automatically when a producer or consumer attempts to connect. Partitioned topics can also be supported. For example:
```sh
pulsar-admin namespaces set-auto-topic-creation "$NAMESPACE" --enable --type partitioned --num-partitions 3
```

**Automatic Cleanup**
Topics are automatically deleted after a period of inactivity.
*   **Configuration**: `brokerDeleteInactivePartitionedTopicMetadataEnabled` is set to `true` in the broker configuration.
*   **Why**: This is crucial to prevent metadata inconsistencies. Without it, inactive partitioned topics might be deleted, but their metadata could persist, causing issues if the topic is re-created later.

### Message Retention & Subscriptions

**Aggressive Retention**
To facilitate aggressive cleanup and prevent storage waste, we use the following retention policy:
```sh
pulsar-admin namespaces set-retention "$NAMESPACE" --size 0 --time 0
```
This causes all messages that are not backlogged to a subscription to be deleted immediately.
- **Intended Effect**: Acknowledged messages are immediately eligible for deletion.
- **Side Effect**: Messages produced to a topic *before* a subscription exists are also immediately deleted (lost).

**Mitigation (Crucial)**
A subscription *must* exist for a topic before producing messages to it. This pattern is implemented in the `produce-*.sh` scripts by explicitly creating a subscription if one is missing.

### Scaling & Ordering

**Horizontal Scaling**
The consumer relies on a `Shared` subscription (configured in `consume.sh`). This allows multiple instances of `consume.sh` to be run simultaneously. Pulsar acts as a load balancer, distributing messages from all matched topics across the available consumers.

**Ordering Guarantees**
*   **Current Setup (Shared)**: Messages are distributed round-robin. Global ordering is **not** guaranteed across the consumer group.
*   **Preserving Order (Key_Shared)**: If strict ordering is required (e.g., for events related to a specific entity like a User ID), use the `Key_Shared` subscription type.
    *   **How**: Set `--subscription-type=Key_Shared` and ensure producers attach a Key (e.g., the User ID) to every message.
    *   **Result**: Pulsar guarantees that all messages with the same Key are delivered to the *same* consumer instance in the correct order, while still allowing the overall workload to be shared across multiple consumers.

## Project Assessment

### Architecture & Design
*   **Concept**: The project implements a "Fair Queueing" pattern by isolating message classes into separate, dynamically created topics. This is a robust strategy to prevent "noisy neighbor" problems where high-volume message classes starve low-volume ones.
*   **Lifecycle Management**: It relies heavily on Pulsar's automated features (`auto-topic-creation`, `inactive-topic-policies`) to manage infrastructure lazily. This reduces operational overhead but increases configuration complexity.

### Implementation Details
*   **Aggressive Cleanup**: The configuration uses `set-retention --size 0 --time 0` effectively to ensure acknowledged messages don't waste storage.
*   **Risk Mitigation**: The scripts (`produce-*.sh`) implement an "idempotent creation" pattern. They speculatively attempt to create the subscription and suppress the error if it already exists. This ensures the subscription is guaranteed to be present before production begins, satisfying the strict producer contract.
*   **Consumption**: The wildcard (`regex`) consumer in `consume.sh` is the correct approach to aggregate these dynamic topics back into a single processing stream.

### Strengths
*   **Scalability**: New message classes can be introduced without any broker reconfiguration or manual intervention.
*   **Resource Efficiency**: The aggressive cleanup policies prevent "zombie" topics from accumulating in Zookeeper/Bookkeeper.

### Weaknesses & Risks
*   **Strict Producer Contract (Data Loss Risk)**: The Zero-Retention policy imposes a strict requirement: a producer **must never** write to a topic without ensuring a subscription exists first. If this contract is violated (e.g., a producer writes to a new class-topic before the consumer detects it), messages are immediately and irretrievably dropped. This requires rigorous enforcement in the producer implementation.
*   **Coupling & Configuration Drift**: The Producer must know the **exact** subscription name the Consumer will use. If these drift (e.g., consumer updates to v2 but producer uses v1), messages will accumulate on the old subscription (preventing topic cleanup) while the new subscription might see them (since `InitialPosition=Earliest` is enforced), but the original subscription remains as "junk".
*   **Broker Load**: High churn (rapid creation/deletion) of topics can strain Zookeeper. The 120s inactivity buffer helps, but very bursty traffic could still be an issue.

### Verdict
The project is a solid Proof of Concept (PoC) for dynamic fair queueing. It works as intended but requires disciplined producer behavior (always ensure subscription exists) to be production-safe.
