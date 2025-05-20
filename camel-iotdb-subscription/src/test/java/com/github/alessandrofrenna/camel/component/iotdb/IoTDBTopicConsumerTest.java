/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.alessandrofrenna.camel.component.iotdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.tsfile.read.common.RowRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import com.github.alessandrofrenna.camel.component.iotdb.support.IoTDBTestSupport;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class IoTDBTopicConsumerTest extends IoTDBTestSupport {
    private final String TEMPERATURE_PATH = "root.test.demo_device_1.sensor_1.temperature";
    private final String RAIN_PATH = "root.test.demo_device_1.sensor_2.rain";

    private final String TEMPERATURE_TOPIC = "temp_topic";
    private final String RAIN_TOPIC = "rain_topic";

    // private final MockedConstruction<IoTDBSubscriptionEventListener> listenerMocks =
    // mockConstruction(IoTDBSubscriptionEventListener.class);

    @BeforeEach
    void setUpTestSuite() {
        createTimeseriesPathQuietly(TEMPERATURE_PATH);
        createTimeseriesPathQuietly(RAIN_PATH);
    }

    @Override
    @AfterEach
    public void cleanupResources() {
        super.cleanupResources();
        removeTimeseriesPathQuietly(TEMPERATURE_PATH);
        removeTimeseriesPathQuietly(RAIN_PATH);
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                createTopicQuietly(TEMPERATURE_TOPIC, TEMPERATURE_PATH);
                createTopicQuietly(RAIN_TOPIC, RAIN_PATH);
                from("iotdb-subscription:" + TEMPERATURE_TOPIC + "?groupId=group_1&consumerId=consumer_a")
                        .routeId("tempConsumerARoute")
                        .log(LoggingLevel.INFO, "Consumer a received: ${body}")
                        .to("mock:result1");

                from("iotdb-subscription:" + RAIN_TOPIC + "?groupId=group_1&consumerId=consumer_b")
                        .routeId("rainConsumerBRoute")
                        .log(LoggingLevel.INFO, "Consumer b received: ${body}")
                        .to("mock:result2");

                // For the multi-consumer test (your requested scenario)
                from("iotdb-subscription:" + TEMPERATURE_TOPIC + "?groupId=group_2&consumerId=multi_a")
                        .routeId("multiConsumerARouteA")
                        .log(LoggingLevel.DEBUG, "MultiConsumer a received: ${body}")
                        .to("mock:multiResult1");

                from("iotdb-subscription:" + RAIN_TOPIC + "?groupId=group_2&consumerId=multi_a")
                        .routeId("multiConsumerARouteB")
                        .log(LoggingLevel.DEBUG, "MultiConsumer a received: ${body}")
                        .to("mock:multiResult2");
            }
        };
    }

    @Test
    void consume_message_from_temp_topic() throws Exception {
        int size = 10;
        MockEndpoint mockResult1 = getMockEndpoint("mock:result1");
        mockResult1.reset();
        mockResult1.expectedMinimumMessageCount(1);
        mockResult1.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);

        generateDataPoints(TEMPERATURE_PATH, size, 20.5, 25.5);
        MockEndpoint.assertIsSatisfied(context, 30, TimeUnit.SECONDS);

        for (var exchange : mockResult1.getReceivedExchanges()) {
            SubscriptionMessage message = exchange.getIn().getBody(SubscriptionMessage.class);
            assertNotNull(message);
            SubscriptionCommitContext commitContext =
                    exchange.getIn().getHeader(IoTDBTopicConsumer.MESSAGE_HEADER_KEY, SubscriptionCommitContext.class);
            assertEquals(TEMPERATURE_TOPIC, commitContext.getTopicName());

            var rowCount = 0;
            for (var sessionDataSet : message.getSessionDataSetsHandler()) {
                while (sessionDataSet.hasNext()) {
                    RowRecord rowRecord = sessionDataSet.next();
                    System.out.println(rowRecord);
                    rowCount++;
                }
            }
            assertEquals(size, rowCount);
        }
    }

    @Test
    void consume_message_from_rain_topic() throws Exception {
        int size = 10;
        MockEndpoint mockResult2 = getMockEndpoint("mock:result2");
        mockResult2.reset();
        mockResult2.expectedMinimumMessageCount(1);
        mockResult2.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);

        generateDataPoints(RAIN_PATH, size, 3, 7.5);
        MockEndpoint.assertIsSatisfied(context, 30, TimeUnit.SECONDS);

        for (var exchange : mockResult2.getReceivedExchanges()) {
            SubscriptionMessage message = exchange.getIn().getBody(SubscriptionMessage.class);
            assertNotNull(message);
            SubscriptionCommitContext commitContext =
                    exchange.getIn().getHeader(IoTDBTopicConsumer.MESSAGE_HEADER_KEY, SubscriptionCommitContext.class);
            assertEquals(RAIN_TOPIC, commitContext.getTopicName());

            var rowCount = 0;
            for (var sessionDataSet : message.getSessionDataSetsHandler()) {
                while (sessionDataSet.hasNext()) {
                    RowRecord rowRecord = sessionDataSet.next();
                    System.out.println(rowRecord);
                    rowCount++;
                }
            }
            assertEquals(size, rowCount);
        }
    }

    @Test
    void when_dropping_a_topic_the_routes_should_be_stopped_and_then_removed() {
        template.sendBody(String.format("iotdb-subscription:%s?action=drop", TEMPERATURE_TOPIC), null);
        Awaitility.await().atMost(Duration.ofMillis(500)).untilAsserted(() -> {
            assertEquals(2, context.getRoutes().size());
            assertNull(context.getRoute("tempConsumerARoute"));
        });
    }

    void consume_different_consume_message_from_temp_topic() throws Exception {
        int size = 10;
        MockEndpoint mockResult1 = getMockEndpoint("mock:result1");
        mockResult1.reset();
        mockResult1.expectedMinimumMessageCount(1);
        mockResult1.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);

        generateDataPoints(TEMPERATURE_PATH, size, 20.5, 25.5);
        MockEndpoint.assertIsSatisfied(context, 30, TimeUnit.SECONDS);

        for (var exchange : mockResult1.getReceivedExchanges()) {
            SubscriptionMessage message = exchange.getIn().getBody(SubscriptionMessage.class);
            assertNotNull(message);
            SubscriptionCommitContext commitContext =
                    exchange.getIn().getHeader(IoTDBTopicConsumer.MESSAGE_HEADER_KEY, SubscriptionCommitContext.class);
            assertEquals(TEMPERATURE_TOPIC, commitContext.getTopicName());

            var rowCount = 0;
            for (var sessionDataSet : message.getSessionDataSetsHandler()) {
                while (sessionDataSet.hasNext()) {
                    RowRecord rowRecord = sessionDataSet.next();
                    System.out.println(rowRecord);
                    rowCount++;
                }
            }
            assertEquals(size, rowCount);
        }
    }
}
