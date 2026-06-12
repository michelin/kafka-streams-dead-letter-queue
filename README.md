<div align="center">

<img src=".readme/logo.png" alt="Apache Kafka"/>

# Kafka Streams Dead Letter Queue

[![GitHub Build](https://img.shields.io/github/actions/workflow/status/michelin/kafka-streams-dead-letter-queue/build.yml?branch=main&logo=github&style=for-the-badge)](https://github.com/michelin/kafka-streams-dead-letter-queue/actions/workflows/build.yml)
[![Kafka Version](https://img.shields.io/badge/dynamic/xml?url=https%3A%2F%2Fraw.githubusercontent.com%2Fmichelin%2Fkafka-streams-processing-error-handling%2Fmain%2Fpom.xml&query=%2F*%5Blocal-name()%3D'project'%5D%2F*%5Blocal-name()%3D'properties'%5D%2F*%5Blocal-name()%3D'kafka-streams.version'%5D%2Ftext()&style=for-the-badge&logo=apachekafka&label=version)](https://github.com/michelin/kafka-streams-dead-letter-queue/blob/main/pom.xml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache&style=for-the-badge)](https://opensource.org/licenses/Apache-2.0)

[Prerequisites](#prerequisites) • [Examples](#examples) • [Michelin IT Blog](#michelin-it-blog)

Code sample for Kafka Streams Dead Letter Queue ([KIP-1034](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1034%3A+Dead+letter+queue+in+Kafka+Streams)).

</div>

## Prerequisites

- Java 25
- Maven
- Docker

## Examples

- Dead Letter Queue with [`errors.dead.letter.queue.topic.name` property](/dead-letter-queue-property).
- Dead Letter Queue with [custom processing exception handler](/exception-handlers).

## Michelin IT Blog

_Soon to be published._