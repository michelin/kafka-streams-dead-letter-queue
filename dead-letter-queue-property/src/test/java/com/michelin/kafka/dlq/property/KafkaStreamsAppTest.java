/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.kafka.dlq.property;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Kafka Streams DLQ behavior")
class KafkaStreamsAppTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;
    private TestOutputTopic<byte[], byte[]> dlqTopic;

    @BeforeEach
    void setUp() {
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "dead-letter-queue-dsl-app-test");
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.setProperty(
                PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueProcessingExceptionHandler.class.getName());
        // Define the DLQ property to enable the DLQ functionality.
        properties.setProperty(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "dlq-topic");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsApp.buildTopology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties);
        inputTopic =
                testDriver.createInputTopic("delivery_booked_topic", new StringSerializer(), new StringSerializer());
        outputTopic = testDriver.createOutputTopic(
                "filtered_delivery_booked_topic", new StringDeserializer(), new StringDeserializer());
        dlqTopic = testDriver.createOutputTopic("dlq-topic", new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    @DisplayName("Writes both invalid records to DLQ with original key/value")
    void shouldWriteDlqAfterSingleInvalidInput() {
        // "numberOfTires" is negative. This will be caught by the handler and routed to DLQ as InvalidDeliveryException
        String deliveryNegativeNumber = """
                {
                  "deliveryId": "DEL67145",
                  "truckId": "TRK34567",
                  "numberOfTires": -1,
                  "destination": "Marseille"
                }
                """;
        inputTopic.pipeInput("DEL67145", deliveryNegativeNumber);

        // "numberOfTires" is missing. This will be caught by the handler and routed to DLQ as NullPointerException
        String deliveryMissingNumber = """
                {
                  "deliveryId": "DEL73148",
                  "truckId": "TRK34567",
                  "destination": "Lyon"
                }
                """;
        inputTopic.pipeInput("DEL73148", deliveryMissingNumber);

        List<KeyValue<byte[], byte[]>> dlqResults = dlqTopic.readKeyValuesToList();

        assertEquals(2, dlqResults.size());

        assertEquals("DEL67145", new String(dlqResults.getFirst().key));
        assertEquals(deliveryNegativeNumber, new String(dlqResults.getFirst().value));

        assertEquals("DEL73148", new String(dlqResults.get(1).key));
        assertEquals(deliveryMissingNumber, new String(dlqResults.get(1).value));
    }
}
