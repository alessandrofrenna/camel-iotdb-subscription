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

import org.apache.camel.Exchange;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.tsfile.read.common.RowRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import com.github.alessandrofrenna.camel.component.iotdb.support.IoTDBTestSupport;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class IoTDbConsumerEndpointTest extends IoTDBTestSupport {
    private final String TEMPERATURE_TOPIC = "temp_topic";
    private final String TEMPERATURE_PATH = "root.test.demo_device_1.sensor_1.temperature";

    private final String RAIN_TOPIC = "rain_topic";
    private final String RAIN_PATH = "root.test.demo_device_1.sensor_2.rain";

    private final String MISSING_TOPIC = "missing_topic";

    @BeforeEach
    void setUpTestSuite() {
        createTimeseriesPathQuietly(TEMPERATURE_PATH);
        createTopicQuietly(TEMPERATURE_TOPIC, TEMPERATURE_PATH);
        createTimeseriesPathQuietly(RAIN_PATH);
        createTopicQuietly(RAIN_TOPIC, RAIN_PATH);
    }

    @AfterEach
    void tearDownSuite() {
        dropTimeseriesPathQuietly(TEMPERATURE_PATH);
        dropTimeseriesPathQuietly(RAIN_PATH);
    }

    @Test
    @Order(1)
    void when_route1_isStarted_shouldReceiveMessagesFromTempTopic() throws Exception {
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                // spotless:off
                from("iotdb-subscription:test_group:test_consumer_a?subscribeTo=" + TEMPERATURE_TOPIC)
                        .routeId("route1")
                        .to("mock:result1");
                // spotless:on
            }
        });

        int size = 10;
        MockEndpoint mockResult1 = getMockEndpoint("mock:result1");
        mockResult1.reset();
        mockResult1.expectedMinimumMessageCount(1);
        mockResult1.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);

        generateDataPoints(TEMPERATURE_PATH, size, 20.5, 25.5);
        MockEndpoint.assertIsSatisfied(context, 60, TimeUnit.SECONDS);
        mockResult1.getReceivedExchanges().forEach(exchange -> assertFromExchange(exchange, TEMPERATURE_TOPIC, size));

        context.removeRoute("route1");
    }

    @Test
    @Order(2)
    void when_aConsumerIsSubscribedToMultipleTopics_theMessagesShouldBeReceived() throws Exception {
        MockEndpoint mockRainTopicEp = getMockEndpoint("mock:rain_topic");
        MockEndpoint mockTempTopic = getMockEndpoint("mock:temp_topic");

        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                // spotless:off
                from(String.format("iotdb-subscription:test_group:test_consumer?subscribeTo=%s,%s", TEMPERATURE_TOPIC, RAIN_TOPIC))
                        .routeId("route2")
                        .choice()
                            .when(header("topic").isEqualTo(TEMPERATURE_TOPIC)).to(mockTempTopic)
                            .when(header("topic").isEqualTo(RAIN_TOPIC)).to(mockRainTopicEp)
                        .end();
                // spotless:on
            }
        });

        int size = 10;
        mockTempTopic.expectedMinimumMessageCount(1);
        mockTempTopic.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);
        mockTempTopic.setResultWaitTime(120000); // Increased wait time

        generateDataPoints(TEMPERATURE_PATH, size, 20.5, 25.5);
        mockTempTopic.assertIsSatisfied();

        Thread.sleep(3000);

        mockRainTopicEp.expectedMinimumMessageCount(1);
        mockRainTopicEp.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);
        mockTempTopic.setResultWaitTime(120000); // Increased wait time
        generateDataPoints(RAIN_PATH, size, 3, 7.5);

        mockRainTopicEp.assertIsSatisfied();

        mockTempTopic.getReceivedExchanges().forEach(exchange -> assertFromExchange(exchange, TEMPERATURE_TOPIC, size));
        mockRainTopicEp.getReceivedExchanges().forEach(exchange -> assertFromExchange(exchange, RAIN_TOPIC, size));

        context.removeRoute("route2");
    }

    @Test
    @Order(3)
    void when_moreConsumersAreSubscribedToTheSameTopic_theMessagesShouldBeReceived() throws Exception {
        MockEndpoint mockTempTopic1 = getMockEndpoint("mock:temp_topic1");
        MockEndpoint mockTempTopic2 = getMockEndpoint("mock:temp_topic2");

        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                // spotless:off
                from("iotdb-subscription:test_group:test_consumer_a?subscribeTo=" + TEMPERATURE_TOPIC)
                        .routeId("route1")
                        .to(mockTempTopic1);

                from("iotdb-subscription:test_group_2:test_consumer_a2?subscribeTo=" + TEMPERATURE_TOPIC)
                        .routeId("route3")
                        .to(mockTempTopic2);
                // spotless:on
            }
        });

        int size = 10;
        mockTempTopic1.expectedMinimumMessageCount(1);
        mockTempTopic1.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);
        mockTempTopic1.setResultWaitTime(30000);

        mockTempTopic2.expectedMinimumMessageCount(1);
        mockTempTopic2.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);
        mockTempTopic2.setResultWaitTime(30000);

        generateDataPoints(TEMPERATURE_PATH, size, 20.5, 25.5);
        mockTempTopic1.assertIsSatisfied();
        mockTempTopic2.assertIsSatisfied();

        mockTempTopic1
                .getReceivedExchanges()
                .forEach(exchange -> assertFromExchange(exchange, TEMPERATURE_TOPIC, size));
        mockTempTopic2
                .getReceivedExchanges()
                .forEach(exchange -> assertFromExchange(exchange, TEMPERATURE_TOPIC, size));

        context.removeRoute("route1");
        context.removeRoute("route3");
    }

    @Test
    @Order(4)
    void when_aTopicIsAddedToARoute_theMessagesShouldBeReceivedFromBoth() throws Exception {
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                // spotless:off
                from("iotdb-subscription:test_group_2:test_consumer_a2?subscribeTo=" + TEMPERATURE_TOPIC)
                        .routeId("route4")
                        .to("mock:result5");
                // spotless:on
            }
        });

        Awaitility.await()
                .atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> assertTrue(
                        context.getRouteController().getRouteStatus("route4").isStarted()));

        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                // spotless:off
                from(String.format("iotdb-subscription:test_group_2:test_consumer_a2?subscribeTo=%s,%s", TEMPERATURE_TOPIC, RAIN_TOPIC))
                        .routeId("route4")
                        .choice()
                            .when(header("topic").isEqualTo(TEMPERATURE_TOPIC)).to("mock:result5")
                            .when(header("topic").isEqualTo(RAIN_TOPIC)).to("mock:result6")
                        .end();
                // spotless:on
            }
        });

        int size = 10;
        MockEndpoint mockResult5 = getMockEndpoint("mock:result5");
        mockResult5.expectedMinimumMessageCount(1);
        mockResult5.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);
        mockResult5.setResultWaitTime(30000); // Increased wait time

        MockEndpoint mockResult6 = getMockEndpoint("mock:result6");
        mockResult6.expectedMinimumMessageCount(1);
        mockResult6.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);
        mockResult6.setResultWaitTime(30000); // Increased wait time

        generateDataPoints(TEMPERATURE_PATH, size, 20.5, 25.5);
        generateDataPoints(RAIN_PATH, size, 3, 7.5);
        mockResult5.assertIsSatisfied();
        mockResult6.assertIsSatisfied();

        mockResult5.getReceivedExchanges().forEach(exchange -> assertFromExchange(exchange, TEMPERATURE_TOPIC, size));
        mockResult6.getReceivedExchanges().forEach(exchange -> assertFromExchange(exchange, RAIN_TOPIC, size));

        context.removeRoute("route4");
    }

    @Test
    @Order(5)
    void when_aTopicIsRemovedFromARoute_theMessagesFromTheRemovedTopicShouldNotBeReceived() throws Exception {
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                // spotless:off
                from(String.format("iotdb-subscription:test_group:test_consumer?subscribeTo=%s,%s", TEMPERATURE_TOPIC, RAIN_TOPIC))
                        .routeId("route3")
                        .choice()
                            .when(header("topic").isEqualTo(TEMPERATURE_TOPIC)).to("mock:result1")
                            .when(header("topic").isEqualTo(RAIN_TOPIC)).to("mock:result2")
                        .end();
                // spotless:on
            }
        });

        Awaitility.await()
                .atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> assertTrue(
                        context.getRouteController().getRouteStatus("route3").isStarted()));

        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                // spotless:off
                from(String.format("iotdb-subscription:test_group:test_consumer?subscribeTo=%s", TEMPERATURE_TOPIC))
                        .routeId("route3")
                        .choice()
                        .when(header("topic").isEqualTo(TEMPERATURE_TOPIC)).to("mock:result1")
                        .when(header("topic").isEqualTo(RAIN_TOPIC)).to("mock:result2")
                        .end();
                // spotless:on
            }
        });

        int size = 10;
        MockEndpoint mockResult1 = getMockEndpoint("mock:result1");
        mockResult1.expectedMinimumMessageCount(1);
        mockResult1.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);
        mockResult1.setResultWaitTime(30000); // Increased wait time

        MockEndpoint mockResult2 = getMockEndpoint("mock:result2");
        mockResult2.expectedMessageCount(0);
        mockResult2.setResultWaitTime(30000); // Increased wait time

        generateDataPoints(TEMPERATURE_PATH, size, 20.5, 25.5);
        generateDataPoints(RAIN_PATH, size, 3, 7.5);
        mockResult1.assertIsSatisfied();
        mockResult2.assertIsSatisfied();

        mockResult1.getReceivedExchanges().forEach(exchange -> assertFromExchange(exchange, TEMPERATURE_TOPIC, size));
        assertTrue(mockResult2.getReceivedExchanges().isEmpty());

        context.removeRoute("route3");
    }

    @Test
    @Order(6)
    void when_subscribingToAMissingTopic_theRouteShouldBeStopped_afterThrow() {
        String MISSING_TOPIC_ROUTE_ID = "routeOfMissingTopic";
        assertNull(context().getRoute(MISSING_TOPIC_ROUTE_ID));

        assertThrows(
                RuntimeCamelException.class,
                () -> context.addRoutes(new RouteBuilder() {
                    @Override
                    public void configure() {
                        from("iotdb-subscription:group_test_2:wrong_consumer?subscribeTo=" + MISSING_TOPIC)
                                .routeId(MISSING_TOPIC_ROUTE_ID)
                                .to("mock:fail");
                    }
                }));

        Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
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
