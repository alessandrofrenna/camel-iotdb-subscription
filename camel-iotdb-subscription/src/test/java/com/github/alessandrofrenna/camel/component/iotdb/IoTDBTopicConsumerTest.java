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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.tsfile.read.common.RowRecord;
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
    private final String MISSING_TOPIC = "missing_topic";

    @BeforeEach
    void setUpTestSuite() {
        createTimeseriesPathQuietly(TEMPERATURE_PATH);
        createTimeseriesPathQuietly(RAIN_PATH);
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
                        .to("mock:result1")
                        .autoStartup(false);

                from("iotdb-subscription:" + RAIN_TOPIC + "?groupId=group_1&consumerId=consumer_b")
                        .routeId("rainConsumerBRoute")
                        .log(LoggingLevel.INFO, "Consumer b received: ${body}")
                        .to("mock:result2")
                        .autoStartup(false);

                // For the multi-consumer test
                from("iotdb-subscription:" + TEMPERATURE_TOPIC + "?groupId=group_2&consumerId=multi_a")
                        .routeId("multiConsumerARouteA")
                        .log(LoggingLevel.INFO, "MultiConsumer a received: ${body}")
                        .to("mock:multiResult1")
                        .autoStartup(false);

                from("iotdb-subscription:" + RAIN_TOPIC + "?groupId=group_2&consumerId=multi_a")
                        .routeId("multiConsumerARouteB")
                        .log(LoggingLevel.INFO, "MultiConsumer a received: ${body}")
                        .to("mock:multiResult2")
                        .autoStartup(false);
            }
        };
    }

    @Test
    void when_tempConsumerARoute_isStarted_shouldReceiveMessagesFromTempTopic() throws Exception {
        int size = 10;
        MockEndpoint mockResult1 = getMockEndpoint("mock:result1");
        mockResult1.reset();
        mockResult1.expectedMinimumMessageCount(1);
        mockResult1.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);

        context.getRouteController().startRoute("tempConsumerARoute");
        generateDataPoints(TEMPERATURE_PATH, size, 20.5, 25.5);
        MockEndpoint.assertIsSatisfied(context, 30, TimeUnit.SECONDS);
        mockResult1.getReceivedExchanges().forEach(exchange -> assertFromExchange(exchange, TEMPERATURE_TOPIC, size));

        context.getRouteController().stopRoute("tempConsumerARoute");
    }

    @Test
    void when_rainConsumerBRoute_isStarted_shouldReceiveMessagesFromRainTopic() throws Exception {
        int size = 10;
        MockEndpoint mockResult2 = getMockEndpoint("mock:result2");
        mockResult2.reset();
        mockResult2.expectedMinimumMessageCount(1);
        mockResult2.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);

        context.getRouteController().startRoute("rainConsumerBRoute");
        generateDataPoints(RAIN_PATH, size, 3, 7.5);
        MockEndpoint.assertIsSatisfied(context, 30, TimeUnit.SECONDS);
        mockResult2.getReceivedExchanges().forEach(exchange -> assertFromExchange(exchange, RAIN_TOPIC, size));

        context.getRouteController().stopRoute("tempConsumerBRoute");
    }

    @Test
    void when_aConsumerIsSubscribedToMultipleTopics_theMessagesShouldBeRoutedCorrectly() throws Exception {
        int size = 10;
        MockEndpoint mockMultiResult1 = getMockEndpoint("mock:multiResult1");
        mockMultiResult1.reset();
        mockMultiResult1.expectedMinimumMessageCount(1);
        mockMultiResult1.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);

        MockEndpoint mockMultiResult2 = getMockEndpoint("mock:multiResult2");
        mockMultiResult2.reset();
        mockMultiResult2.expectedMinimumMessageCount(1);
        mockMultiResult2.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);

        var routeController = context.getRouteController();
        routeController.startRoute("multiConsumerARouteA");
        routeController.startRoute("multiConsumerARouteB");
        generateDataPoints(TEMPERATURE_PATH, size, 20.5, 25.5);
        generateDataPoints(RAIN_PATH, size, 3, 7.5);
        MockEndpoint.assertIsSatisfied(context, 30, TimeUnit.SECONDS);

        mockMultiResult1
                .getReceivedExchanges()
                .forEach(exchange -> assertFromExchange(exchange, TEMPERATURE_TOPIC, size));
        mockMultiResult2.getReceivedExchanges().forEach(exchange -> assertFromExchange(exchange, RAIN_TOPIC, size));

        routeController.stopRoute("multiConsumerARouteA");
        routeController.stopRoute("multiConsumerARouteB");
    }

    @Test
    void when_moreConsumersAreSubscribedToTheSameTopic_theMessagesShouldBeRoutedCorrectly() throws Exception {
        int size = 10;
        MockEndpoint result1 = getMockEndpoint("mock:result1");
        result1.reset();
        result1.expectedMinimumMessageCount(1);
        result1.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);

        MockEndpoint mockMultiResult1 = getMockEndpoint("mock:multiResult1");
        mockMultiResult1.reset();
        mockMultiResult1.expectedMinimumMessageCount(1);
        mockMultiResult1.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);

        var routeController = context.getRouteController();
        routeController.startRoute("tempConsumerARoute");
        routeController.startRoute("multiConsumerARouteA");
        generateDataPoints(TEMPERATURE_PATH, size, 20.5, 25.5);

        MockEndpoint.assertIsSatisfied(context, 30, TimeUnit.SECONDS);
        Stream.concat(result1.getReceivedExchanges().stream(), mockMultiResult1.getReceivedExchanges().stream())
                .forEach(exchange -> assertFromExchange(exchange, TEMPERATURE_TOPIC, size));

        routeController.stopRoute("tempConsumerARoute");
        routeController.stopRoute("multiConsumerARouteA");
    }

    @Test
    void when_droppingATopic_theRoutes_shouldBeStoppedSndThenRemoved() {
        context.getRoutes().forEach(route -> {
            try {
                context.getRouteController().startRoute(route.getRouteId());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        template.sendBody(String.format("iotdb-subscription:%s?action=drop", TEMPERATURE_TOPIC), null);
        Awaitility.await().atMost(Duration.ofMillis(500)).untilAsserted(() -> {
            assertEquals(2, context.getRoutes().size());
            assertNull(context.getRoute("tempConsumerARoute"));
        });
    }

    @Test
    void onSubscriptionException_theRouteShouldBeRemoved() {
        String MISSING_TOPIC_ROUTE_ID = "multiConsumerBRouteA";
        assertNull(context().getRoute(MISSING_TOPIC_ROUTE_ID));

        assertThrows(
                RuntimeCamelException.class,
                () -> context.addRoutes(new RouteBuilder() {
                    @Override
                    public void configure() {
                        from("iotdb-subscription:" + MISSING_TOPIC + "?groupId=group_2&consumerId=multi_b")
                                .routeId(MISSING_TOPIC_ROUTE_ID)
                                .to("mock:fail");
                    }
                }));

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> {
            var routeStatus = context.getRouteController().getRouteStatus(MISSING_TOPIC_ROUTE_ID);
            assertNotNull(routeStatus);
            assertTrue(routeStatus.isStopped());
        });
    }

    private void assertFromExchange(Exchange exchange, String topicName, long timeseriesCount) {
        SubscriptionMessage message = exchange.getIn().getBody(SubscriptionMessage.class);
        assertNotNull(message);
        assertEquals(topicName, message.getCommitContext().getTopicName());
        var rowCount = 0;
        for (var sessionDataSet : message.getSessionDataSetsHandler()) {
            while (sessionDataSet.hasNext()) {
                RowRecord rowRecord = sessionDataSet.next();
                System.out.println(rowRecord);
                rowCount++;
            }
        }
        assertTrue(rowCount >= timeseriesCount);
    }
}
