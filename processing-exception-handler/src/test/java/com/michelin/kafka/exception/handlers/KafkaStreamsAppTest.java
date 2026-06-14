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
package com.michelin.kafka.exception.handlers;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.kafka.exception.handlers.handler.CustomProcessingExceptionHandler;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsAppTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<byte[], byte[]> nullNumberOfTiresDlqTopic;
    private TestOutputTopic<byte[], byte[]> invalidDeliveryDlqTopic;
    private TestOutputTopic<byte[], byte[]> selectKeyProcessorDlqTopic;
    private TestOutputTopic<byte[], byte[]> defaultDlqTopic;

    @BeforeEach
    void setUp() {
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "dead-letter-queue-processing-exception-handler-test");
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.setProperty(
                PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, CustomProcessingExceptionHandler.class.getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsApp.buildTopology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties);

        inputTopic =
                testDriver.createInputTopic("delivery-booked-topic", new StringSerializer(), new StringSerializer());
        nullNumberOfTiresDlqTopic = testDriver.createOutputTopic(
                "null-number-of-tires-dlq-topic", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        invalidDeliveryDlqTopic = testDriver.createOutputTopic(
                "invalid-delivery-dlq-topic", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        selectKeyProcessorDlqTopic = testDriver.createOutputTopic(
                "select-key-processor-dlq-topic", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        defaultDlqTopic = testDriver.createOutputTopic(
                "default-dlq-topic", new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void shouldRouteToCorrectDlq() {
        String nullNumberOfTires = """
                {
                  "deliveryId": "DEL001",
                  "truckId": "TRK001",
                  "destination": "Bordeaux"
                }
                """;
        inputTopic.pipeInput("DEL001", nullNumberOfTires);

        String negativeNumberOfTires = """
                {
                  "deliveryId": "DEL002",
                  "truckId": "TRK002",
                  "numberOfTires": -3,
                  "destination": "Paris"
                }
                """;
        inputTopic.pipeInput("DEL002", negativeNumberOfTires);

        String missingDeliveryId = """
                {
                  "truckId": "TRK003",
                  "numberOfTires": 4,
                  "destination": "Lyon"
                }
                """;
        inputTopic.pipeInput("DEL003", missingDeliveryId);

        inputTopic.pipeInput("DEL004", "KABOOM");

        List<KeyValue<byte[], byte[]>> nullNumberOfTiresResults = nullNumberOfTiresDlqTopic.readKeyValuesToList();
        assertEquals("DEL001", new String(nullNumberOfTiresResults.getFirst().key));
        assertEquals(nullNumberOfTires, new String(nullNumberOfTiresResults.getFirst().value));

        List<KeyValue<byte[], byte[]>> invalidDeliveryResults = invalidDeliveryDlqTopic.readKeyValuesToList();
        assertEquals("DEL002TRK002", new String(invalidDeliveryResults.getFirst().key));
        assertEquals("Invalid deliveryBooked DEL002", new String(invalidDeliveryResults.getFirst().value));

        List<KeyValue<byte[], byte[]>> selectKeyProcessorResults = selectKeyProcessorDlqTopic.readKeyValuesToList();
        assertEquals("DEL003", new String(selectKeyProcessorResults.getFirst().key));
        assertEquals(missingDeliveryId, new String(selectKeyProcessorResults.getFirst().value));

        List<KeyValue<byte[], byte[]>> defaultResults = defaultDlqTopic.readKeyValuesToList();
        assertEquals("DEL004", new String(defaultResults.getFirst().key));
        assertTrue(new String(defaultResults.getFirst().value).contains("An exception occurred"));
    }
}
