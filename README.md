<div align="center">

<img src=".readme/logo.png" alt="Apache Kafka"/>

# Kafka Streams Dead Letter Queue

[![GitHub Build](https://img.shields.io/github/actions/workflow/status/michelin/kafka-streams-dead-letter-queue/build.yml?branch=main&logo=github&style=for-the-badge)](https://github.com/michelin/kafka-streams-dead-letter-queue/actions/workflows/build.yml)
[![Kafka Version](https://img.shields.io/badge/dynamic/xml?url=https%3A%2F%2Fraw.githubusercontent.com%2Fmichelin%2Fkafka-streams-dead-letter-queue%2Fmain%2Fpom.xml&query=%2F*%5Blocal-name()%3D'project'%5D%2F*%5Blocal-name()%3D'properties'%5D%2F*%5Blocal-name()%3D'kafka-streams.version'%5D%2Ftext()&style=for-the-badge&logo=apachekafka&label=version)](https://github.com/michelin/kafka-streams-dead-letter-queue/blob/main/pom.xml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache&style=for-the-badge)](https://opensource.org/licenses/Apache-2.0)

[Prerequisites](#prerequisites) • [Examples](#examples) • [Michelin IT Blog](#michelin-it-blog) • [Current London 2025](#current-london-2025)

Code sample for Kafka Streams Dead Letter Queue ([KIP-1034](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1034%3A+Dead+letter+queue+in+Kafka+Streams)).

</div>

## Prerequisites

- Java 25
- Maven
- Docker

## Examples

- Dead Letter Queue with [`errors.dead.letter.queue.topic.name` property](/dead-letter-queue-property).
- Dead Letter Queue with [custom processing exception handler](/processing-exception-handler).

## Michelin IT Blog

The full article on Kafka Streams Dead Letter Queue is available on the [Michelin IT Blog](https://blogit.michelin.io/dead-letter-queue-in-kafka-streams-kip-1034).

## Current London 2025

Dead Letter Queue in Kafka Streams (KIP-1034) has been presented at [Current London 2025](https://current.confluent.io/london/agenda):
- Slides are available [here](.readme/Slides_Processing_Exception_Handling.pptx).
- Replay is available [online](https://current.confluent.io/post-conference-videos-2025/processing-exception-handling-and-dead-letter-queue-in-kafka-streams-lnd25).
