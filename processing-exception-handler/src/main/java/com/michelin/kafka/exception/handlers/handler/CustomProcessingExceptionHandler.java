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
package com.michelin.kafka.exception.handlers.handler;

import com.michelin.kafka.exception.handlers.DeliveryBooked;
import com.michelin.kafka.exception.handlers.InvalidDeliveryException;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomProcessingExceptionHandler implements ProcessingExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(CustomProcessingExceptionHandler.class);

    @Override
    public Response handleError(ErrorHandlerContext context, Record<?, ?> message, Exception exception) {
        if (log.isWarnEnabled()) {
            log.warn(
                    "Exception during message Processing, processor node: {}, taskId: {}, topic: {}, partition: {}, offset: {}",
                    context.processorNodeId(),
                    context.taskId(),
                    context.topic(),
                    context.partition(),
                    context.offset(),
                    exception);
        }

        if (message.value() instanceof DeliveryBooked deliveryBooked && deliveryBooked.numberOfTires() == null) {
            return Response.resume(List.of(new ProducerRecord<>(
                    "null-number-of-tires-dlq-topic", context.sourceRawKey(), context.sourceRawValue())));
        }

        if (exception instanceof InvalidDeliveryException) {
            return Response.resume(List.of(new ProducerRecord<>(
                    "invalid-delivery-dlq-topic",
                    ((String) message.key()).getBytes(),
                    ("Invalid deliveryBooked " + ((DeliveryBooked) message.value()).deliveryId()).getBytes())));
        }

        if (context.processorNodeId().equals("select-key-processor")) {
            return Response.resume(List.of(new ProducerRecord<>(
                    "select-key-processor-dlq-topic", context.sourceRawKey(), context.sourceRawValue())));
        }

        return Response.resume(List.of(new ProducerRecord<>(
                "default-dlq-topic",
                ((String) message.key()).getBytes(),
                ("An exception occurred " + exception.getMessage()).getBytes())));
    }

    @Override
    public void configure(Map<String, ?> map) {
        // Do nothing
    }
}
