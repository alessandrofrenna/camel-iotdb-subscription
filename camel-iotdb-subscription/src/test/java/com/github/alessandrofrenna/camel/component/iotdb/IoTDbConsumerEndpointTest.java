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
public class IoTDbConsumerEndpointTest extends IoTDBTestSupport {
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
                // spotless:off
                from("iotdb-subscription:test_group:test_consumer_a?subscribeTo=" + TEMPERATURE_TOPIC)
                        .routeId("route1")
                        .to("mock:result1")
                        .autoStartup(false);

                from("iotdb-subscription:test_group:test_consumer_b?subscribeTo=" + RAIN_TOPIC)
                        .routeId("route2")
                        .to("mock:result2")
                        .autoStartup(false);

                from(String.format("iotdb-subscription:test_group:test_consumer?subscribeTo=%s,%s", TEMPERATURE_TOPIC, RAIN_TOPIC))
                        .routeId("route3")
                        .autoStartup(false)
                        .choice()
                            .when(header("topic").isEqualTo(TEMPERATURE_TOPIC)).to("mock:result3")
                            .when(header("topic").isEqualTo(RAIN_TOPIC)).to("mock:result4")
                        .end();

                from("iotdb-subscription:test_group_2:test_consumer_a2?subscribeTo=" + TEMPERATURE_TOPIC)
                        .routeId("route4")
                        .to("mock:result5")
                        .autoStartup(false);
                // spotless:on
            }
        };
    }

    @Test
    void when_route1_isStarted_shouldReceiveMessagesFromTempTopic() throws Exception {
        int size = 10;
        MockEndpoint mockResult1 = getMockEndpoint("mock:result1");
        mockResult1.reset();
        mockResult1.expectedMinimumMessageCount(1);
        mockResult1.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);
        context.getRouteController().startRoute("route1");

        generateDataPoints(TEMPERATURE_PATH, size, 20.5, 25.5);
        MockEndpoint.assertIsSatisfied(context, 30, TimeUnit.SECONDS);
        mockResult1.getReceivedExchanges().forEach(exchange -> assertFromExchange(exchange, TEMPERATURE_TOPIC, size));

        context.getRouteController().stopRoute("route1");
    }

    @Test
    void when_route2_isStarted_shouldReceiveMessagesFromRainTopic() throws Exception {
        int size = 10;
        MockEndpoint mockResult2 = getMockEndpoint("mock:result2");
        mockResult2.reset();
        mockResult2.expectedMinimumMessageCount(1);
        mockResult2.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);
        context.getRouteController().startRoute("route2");

        generateDataPoints(RAIN_PATH, size, 3, 7.5);
        MockEndpoint.assertIsSatisfied(context, 30, TimeUnit.SECONDS);
        mockResult2.getReceivedExchanges().forEach(exchange -> assertFromExchange(exchange, RAIN_TOPIC, size));

        context.getRouteController().stopRoute("route2");
    }

    @Test
    void when_aConsumerIsSubscribedToMultipleTopics_theMessagesShouldBeRoutedCorrectly() throws Exception {
        int size = 10;
        MockEndpoint mockResult3 = getMockEndpoint("mock:result3");
        mockResult3.reset();
        mockResult3.expectedMinimumMessageCount(1);
        mockResult3.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);

        MockEndpoint mockResult4 = getMockEndpoint("mock:result4");
        mockResult4.reset();
        mockResult4.expectedMinimumMessageCount(1);
        mockResult4.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);

        var routeController = context.getRouteController();
        routeController.startRoute("route3");

        generateDataPoints(TEMPERATURE_PATH, size, 20.5, 25.5);
        generateDataPoints(RAIN_PATH, size, 3, 7.5);
        MockEndpoint.assertIsSatisfied(context, 30, TimeUnit.SECONDS);

        mockResult3.getReceivedExchanges().forEach(exchange -> assertFromExchange(exchange, TEMPERATURE_TOPIC, size));
        mockResult4.getReceivedExchanges().forEach(exchange -> assertFromExchange(exchange, RAIN_TOPIC, size));

        routeController.stopRoute("route3");
    }

    @Test
    void when_moreConsumersAreSubscribedToTheSameTopic_theMessagesShouldBeRoutedCorrectly() throws Exception {
        int size = 10;
        MockEndpoint mockResult1 = getMockEndpoint("mock:result1");
        mockResult1.reset();
        mockResult1.expectedMinimumMessageCount(1);
        mockResult1.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);

        MockEndpoint mockResult5 = getMockEndpoint("mock:result5");
        mockResult5.reset();
        mockResult5.expectedMinimumMessageCount(1);
        mockResult5.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);

        var routeController = context.getRouteController();
        routeController.startRoute("route4");
        routeController.startRoute("route1");

        generateDataPoints(TEMPERATURE_PATH, size, 20.5, 25.5);
        MockEndpoint.assertIsSatisfied(context, 30, TimeUnit.SECONDS);

        Stream.concat(mockResult1.getReceivedExchanges().stream(), mockResult5.getReceivedExchanges().stream())
                .forEach(exchange -> assertFromExchange(exchange, TEMPERATURE_TOPIC, size));

        routeController.stopRoute("route4");
        routeController.stopRoute("route1");
    }

    @Test
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
        assertEquals(timeseriesCount, rowCount);
    }

    //    @Test
    //    void when_droppingATopic_theRoutes_shouldBeStoppedSndThenRemoved() {
    //        context.getRoutes().forEach(route -> {
    //            try {
    //                context.getRouteController().startRoute(route.getRouteId());
    //            } catch (Exception e) {
    //                throw new RuntimeException(e);
    //            }
    //        });
    //        template.sendBody(String.format("iotdb-subscription:%s?action=drop", TEMPERATURE_TOPIC), null);
    //        Awaitility.await().atMost(Duration.ofMillis(500)).untilAsserted(() -> {
    //            assertEquals(2, context.getRoutes().size());
    //            assertNull(context.getRoute("tempConsumerARoute"));
    //        });
    //    }
}
