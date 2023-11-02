## Workload Scheduler

A workload scheduler is designed to showcase how consumer worker pools can be managed to balance work loads while reading messages published
from a producer. The crate consists of the following modules:

1. `producer.rs` -> The producer module.

   - The producer module generates tasks and publishes them to a list of topics using the `produce_messages` method.
   - The producer also hosts an API to add tasks to the worker pool using the `start_task_pool` method.

2. `consumer.rs` -> The consumer worker pool module.

   - The worker pool hosts a set of consumers to consume messages from a set of topics using the `consume` method.
   - The worker pool load balancing strategy includes the following features:
   - Each consumer is assigned a group ID, allowing a consumer within a group to process messages from a topic
     partition using the `process` method.
   - Each consumer in a group is allowed to subscribe to a topic partition for a certain amount of time. Once the
     timer elapses, another consumer from the same group is allowed to subscribe from where the previous member left off. This is done to maintain **optimal CPU load** of each consumer in a group. If all consumers in a group have processed messages from a topic, the offset is saved and the next topic is processed.

3. `config.rs` -> A module for reading configuration from a TOML file.

4. `error.rs` -> A module for returning human-readable error messages from the system.

5. `main.rs` -> The entry point to the task scheduler. It reads the configuration, sets up the producer and
   consumer worker pool.

### Ensuring Sequential Execution of Tasks within a Specific Topic

To ensure that tasks within a specific topic are executed in the order they are received, sequentially, you can use partitioning with keys in Kafka. Here's a code snippet inside `producer.rs` that demonstrates this:

```rust
let delivery_status = producer
    .send(
        FutureRecord::to(&topic)
            .payload(...)
            .key(&format!("Key {}", key))
            .headers(...),
        Duration::from_secs(0),
    )
    .await;
```

By using keys to partition the data in Kafka, when a consumer group subscribes to the topic, it can read the messages in the sequence in which they arrive. The load balancing strategy commits the message offset in Async mode before the timer elapses:

```rust
if let Err(e) = consumer.commit_message(&m, CommitMode::Async) {
    info!("Commit error: {:?}", e);
}
```

After the timer elapses, the next consumer in the group picks up where the previous consumer left off and processes the remaining messages in the topic. If there are no more consumers in the group, the process moves on to the next topic.

## How to start the service

This step requires you to install and configure a kafka broker.

- First, make sure you have [Docker](https://docs.docker.com/engine/install/) and the [Docker Compose plugin](https://docs.docker.com/compose/install/linux/) installed:
- Second you will need to install Rust. For more information see [here](https://www.rust-lang.org/tools/install).
- Follow the next steps elucidated below:

### Step 1

```sh
$ docker --version
Docker version 24.0.5, build ced0996
$ docker compose version
Docker Compose version v2.20.2
```

### Step 2

Then simply perform

```sh
docker compose up -d
```

This will set up Kafka and Zookeeper

### Step 3

Build the binary from `task-scheduler` directory:

```sh
cargo build --release
```

### Step 4

The `task-scheduler` binary can be executed from target/release using:

```sh
 ./task-scheduler
```

### Add tasks

The API to add a task can be accessed as follows:

```sh
curl --location 'http://localhost:5000/tasks/generate' \
--header 'Content-Type: application/json' \
--data '{
    "topic": "E",
    "message": "EE"
}'
```

Response:

```sh
"Success"
```

### List producer tasks

The API to list tasks left to be broadcasted from producer

```sh
 curl --location 'http://localhost:5000/tasks'
```

Response:

```sh
[
    {
        "topic": "A",
        "message": "AA"
    },
    {
        "topic": "B",
        "message": "BB"
    },
    ...
]
```

### Logs from scheduler

The output would from task scheduler would look something like this:-

```sh
Future completed. Result: Ok((0, 20))
Future completed. Result: Ok((0, 20))
Future completed. Result: Ok((0, 20))
Future completed. Result: Ok((0, 20))
key: 'Some([75, 101, 121, 32, 48])', payload: 'Message AA', topic: A, partition: 0, offset: 12, timestamp: CreateTime(1698602603696)
...
```
