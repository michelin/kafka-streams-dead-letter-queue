# Dead Letter Queue Property

This module demonstrates how to use the `errors.dead.letter.queue.topic.name` property to define a dead letter queue topic for records that fail processing.

## Prerequisites

To compile and run this demo, you’ll need:

- Java 25
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/get-started/platform-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Create a topic named `delivery-booked-topic`.
- Start the Kafka Streams application.

To run the application in Docker, use the following command:

```bash
docker-compose up -d
```

This will start the following services in Docker:

- Kafka Broker
- Control Center
- Kafka Streams Dead Letter Queue Property

## Try It Out

Using the [Kafkagen](https://github.com/michelin/kafkagen) `produce` command, you can produce `DeliveryBooked` events to the `delivery-booked-topic` topic.

```bash
kafkagen produce -f ../.kafkagen/default-record.json
```

To trigger dead letter queue routing via the `errors.dead.letter.queue.topic.name` property,
produce records with a missing `numberOfTires` field to route to `default-dlq-topic`.

```bash
kafkagen produce -f ../.kafkagen/no-tires-record.json
```