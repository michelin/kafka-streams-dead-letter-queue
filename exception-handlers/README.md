# exception-handlers

Kafka Streams DSL sample that uses a custom `ProcessingExceptionHandler`:

- `DlqExceptionTypeProcessingHandler`

## Prerequisites

- Java 25
- Maven

## What this module does

- Consumes from `delivery_booked_topic`
- Parses JSON into `DeliveryBooked`
- Filters records where `numberOfTires >= 10`
- Produces valid records to `filtered_delivery_booked_topic`
- Routes failed records to different DLQ topics based on exception type

## DLQ routing strategy

`src/main/java/com/michelin/kafka/exception/handlers/handler/DlqExceptionTypeProcessingHandler.java` routes to:

- `json-exception-dlq-topic` for `JsonSyntaxException`
- `invalid-delivery-exception-dlq-topic` for `InvalidDeliveryException`
- `generic-exception-dlq-topic` for other exceptions (for example `NullPointerException`)

## Tested behavior

`src/test/java/com/michelin/kafka/exception/handlers/KafkaStreamsAppTest.java` validates:

- invalid delivery (`numberOfTires < 0`) goes to `invalid-delivery-exception-dlq-topic`
- invalid JSON (`"KABOOM"`) goes to `json-exception-dlq-topic`
- generic exception (missing `numberOfTires`) goes to `generic-exception-dlq-topic`

## Run tests

From repository root:

```zsh
mvn -pl exception-handlers test
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
- Kafka Streams dead letter queue routing with exception handlers example

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

To trigger the generic DLQ branch, produce a record with missing `numberOfTires` (triggers `NullPointerException` → `generic-exception-dlq-topic`):

```json
{
  "deliveryId": "DEL12345",
  "truckId": "TRK56789",
  "destination": "Bordeaux"
}
```

To trigger `invalid-delivery-exception-dlq-topic`, produce a record with a negative `numberOfTires` (triggers `InvalidDeliveryException` → `invalid-delivery-exception-dlq-topic`):

```json
{
  "deliveryId": "DEL67145",
  "truckId": "TRK34567",
  "numberOfTires": -1,
  "destination": "Marseille"
}
```

To trigger `json-exception-dlq-topic`, produce an invalid JSON value (triggers `JsonSyntaxException` → `json-exception-dlq-topic`):

```
KABOOM
```

