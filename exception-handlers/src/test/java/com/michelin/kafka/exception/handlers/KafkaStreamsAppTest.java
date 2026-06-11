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

import com.michelin.kafka.exception.handlers.handler.DlqExceptionTypeProcessingHandler;
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Kafka Streams DLQ routing by exception type")
class KafkaStreamsAppTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<byte[], byte[]> genericExceptionDeadLetterQueueTopic;
    private TestOutputTopic<byte[], byte[]> jsonExceptionDeadLetterQueueTopic;
    private TestOutputTopic<byte[], byte[]> invalidDeliveryExceptionDeadLetterQueueTopic;

    @BeforeEach
    void setUp() {
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "dead-letter-queue-dsl-app-test");
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.setProperty(
                PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, DlqExceptionTypeProcessingHandler.class.getName());

        // Define DLQ topics by exception type
        properties.put("jsonExceptionDeadLetterQueueTopic", "json-exception-dlq-topic");
        properties.put("invalidDeliveryExceptionDeadLetterQueueTopic", "invalid-delivery-exception-dlq-topic");
        properties.put("genericExceptionDeadLetterQueueTopic", "generic-exception-dlq-topic");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsApp.buildTopology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties);

        inputTopic =
                testDriver.createInputTopic("delivery_booked_topic", new StringSerializer(), new StringSerializer());
        genericExceptionDeadLetterQueueTopic = testDriver.createOutputTopic(
                "generic-exception-dlq-topic", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        jsonExceptionDeadLetterQueueTopic = testDriver.createOutputTopic(
                "json-exception-dlq-topic", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        invalidDeliveryExceptionDeadLetterQueueTopic = testDriver.createOutputTopic(
                "invalid-delivery-exception-dlq-topic", new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    @DisplayName("Routes records to DLQ topics based on exception type")
    void shouldContinueOnInvalidDeliveryAndNullPointerExceptions() {
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

        // This will throw a JsonSyntaxException
        inputTopic.pipeInput("KABOOM", "KABOOM");

        List<KeyValue<byte[], byte[]>> invalidDeliveryExceptionDlqResults =
                invalidDeliveryExceptionDeadLetterQueueTopic.readKeyValuesToList();
        assertEquals("DEL67145", new String(invalidDeliveryExceptionDlqResults.getFirst().key));
        assertEquals(
                "Invalid deliveryBooked DEL67145", new String(invalidDeliveryExceptionDlqResults.getFirst().value));

        List<KeyValue<byte[], byte[]>> jsonExceptionDlqResults =
                jsonExceptionDeadLetterQueueTopic.readKeyValuesToList();
        assertEquals("KABOOM", new String(jsonExceptionDlqResults.getFirst().key));
        assertEquals("KABOOM", new String(jsonExceptionDlqResults.getFirst().value));

        List<KeyValue<byte[], byte[]>> genericExceptionDlqResults =
                genericExceptionDeadLetterQueueTopic.readKeyValuesToList();
        assertEquals("DEL73148", new String(genericExceptionDlqResults.getFirst().key));
        assertTrue(new String(genericExceptionDlqResults.getFirst().value).contains("An exception occurred"));
    }
}
