# dead-letter-queue-property

Kafka Streams DSL sample that uses the built-in DLQ property:

- `errors.dead.letter.queue.topic.name`

## Prerequisites

- Java 25
- Maven

## What this module does

- Consumes from `delivery_booked_topic`
- Parses JSON into `DeliveryBooked`
- Filters records where `numberOfTires >= 10`
- Produces valid records to `filtered_delivery_booked_topic`
- Sends failed records to `dlq-topic` when a processing exception occurs and the handler is set to continue

In tests, `LogAndContinueProcessingExceptionHandler` is configured together with
`errors.dead.letter.queue.topic.name=dlq-topic`.

## Tested behavior

`src/test/java/com/michelin/kafka/dlq/property/KafkaStreamsAppTest.java` verifies that:

- an `InvalidDeliveryException` record goes to DLQ
- a `NullPointerException` record goes to DLQ

## Run tests

From repository root:

```zsh
mvn -pl dead-letter-queue-property test
```

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/get-started/platform-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Start the Kafka Streams application.

To run the application in Docker, use the following command:

```bash
docker-compose up -d
```

This will start the following services in Docker:

- Kafka Broker
- Control Center
- init-kafka (topic bootstrap)
- Kafka Streams dead letter queue property example

## Try It Out

Using Control Center at `http://localhost:9021`, produce records to `delivery_booked_topic`.

Example valid record:

```json
{
  "deliveryId": "DEL12345",
  "truckId": "TRK56789",
  "numberOfTires": 18,
  "destination": "Bordeaux"
}
```

To trigger the generic DLQ branch, produce a record with missing `numberOfTires` (triggers `NullPointerException` → `dlq-topic`):

```json
{
  "deliveryId": "DEL12345",
  "truckId": "TRK56789",
  "destination": "Bordeaux"
}
```

To trigger `invalid-delivery-exception-dlq-topic`, produce a record with a negative `numberOfTires` (triggers `InvalidDeliveryException` → `dlq-topic`):

```json
{
  "deliveryId": "DEL67145",
  "truckId": "TRK34567",
  "numberOfTires": -1,
  "destination": "Marseille"
}
```

To trigger `json-exception-dlq-topic`, produce an invalid JSON value (triggers `JsonSyntaxException` → `dlq-topic`):

```json
{
  "deliveryId": "DEL67145",
  "truckId": "TRK34567",
  "numberOfTires": -1,
  "destination": "Marseille"
}
```
KABOOM
```

