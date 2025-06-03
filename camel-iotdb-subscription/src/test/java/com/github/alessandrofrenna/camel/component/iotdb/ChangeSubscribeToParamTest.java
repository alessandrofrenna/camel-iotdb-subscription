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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import com.github.alessandrofrenna.camel.component.iotdb.support.IoTDBTestSupport;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class ChangeSubscribeToParamTest extends IoTDBTestSupport {
    private final String TEMPERATURE_TOPIC = "temp_topic";
    private final String TEMPERATURE_PATH = "root.test.demo_device_1.sensor_1.temperature";
    private final String RAIN_TOPIC = "rain_topic";
    private final String RAIN_PATH = "root.test.demo_device_1.sensor_2.rain";

    @Test
    void when_aTopicIsAddedToARoute_theMessagesShouldBeReceivedFromBoth() throws Exception {
        createTimeseriesPathQuietly(TEMPERATURE_PATH);
        createTopicQuietly(TEMPERATURE_TOPIC, TEMPERATURE_PATH);

        MockEndpoint mockResult5 = getMockEndpoint("mock:result5");
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                // spotless:off
                from("iotdb-subscription:test_group_2:test_consumer_a2?subscribeTo=" + TEMPERATURE_TOPIC)
                        .routeId("route4")
                        .to(mockResult5);
                // spotless:on
            }
        });

        Awaitility.await()
                .atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> assertTrue(
                        context.getRouteController().getRouteStatus("route4").isStarted()));

        createTimeseriesPathQuietly(RAIN_PATH);
        createTopicQuietly(RAIN_TOPIC, RAIN_PATH);

        MockEndpoint mockResult6 = getMockEndpoint("mock:result6");
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                // spotless:off
                from(String.format("iotdb-subscription:test_group_2:test_consumer_a2?subscribeTo=%s,%s", TEMPERATURE_TOPIC, RAIN_TOPIC))
                        .routeId("route4")
                        .choice()
                            .when(header("topic").isEqualTo(TEMPERATURE_TOPIC)).to(mockResult5)
                            .when(header("topic").isEqualTo(RAIN_TOPIC)).to(mockResult6)
                        .end();
                // spotless:on
            }
        });

        int size = 10;

        mockResult5.expectedMinimumMessageCount(1);
        mockResult5.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);
        mockResult5.setResultWaitTime(30000); // Increased wait time
        generateDataPoints(TEMPERATURE_PATH, size, 20.5, 25.5);
        mockResult5.assertIsSatisfied();

        mockResult6.expectedMinimumMessageCount(1);
        mockResult6.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);
        mockResult6.setResultWaitTime(30000); // Increased wait time
        generateDataPoints(RAIN_PATH, size, 3, 7.5);
        mockResult6.assertIsSatisfied();

        mockResult5.getReceivedExchanges().forEach(exchange -> assertFromExchange(exchange, TEMPERATURE_TOPIC, size));
        mockResult6.getReceivedExchanges().forEach(exchange -> assertFromExchange(exchange, RAIN_TOPIC, size));

        context.removeRoute("route4");
        dropTimeseriesPathQuietly("root.test.demo_device_1.**");
    }

    @Test
    void when_aTopicIsRemovedFromARoute_theMessagesFromTheRemovedTopicShouldNotBeReceived() throws Exception {
        createTimeseriesPathQuietly(TEMPERATURE_PATH);
        createTopicQuietly(TEMPERATURE_TOPIC, TEMPERATURE_PATH);
        createTimeseriesPathQuietly(RAIN_PATH);
        createTopicQuietly(RAIN_TOPIC, RAIN_PATH);

        MockEndpoint mockResult1 = getMockEndpoint("mock:result1");
        MockEndpoint mockResult2 = getMockEndpoint("mock:result2");
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                // spotless:off
                from(String.format("iotdb-subscription:test_group:test_consumer?subscribeTo=%s,%s", TEMPERATURE_TOPIC, RAIN_TOPIC))
                        .routeId("route3")
                        .choice()
                            .when(header("topic").isEqualTo(TEMPERATURE_TOPIC)).to(mockResult1)
                            .when(header("topic").isEqualTo(RAIN_TOPIC)).to(mockResult2)
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
                        .when(header("topic").isEqualTo(TEMPERATURE_TOPIC)).to(mockResult1)
                        .when(header("topic").isEqualTo(RAIN_TOPIC)).to(mockResult2)
                        .end();
                // spotless:on
            }
        });

        int size = 10;
        mockResult1.expectedMinimumMessageCount(1);
        mockResult1.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);
        mockResult1.setResultWaitTime(30000); // Increased wait time
        generateDataPoints(TEMPERATURE_PATH, size, 20.5, 25.5);
        mockResult1.assertIsSatisfied();

        mockResult2.expectedMessageCount(0);
        mockResult2.setResultWaitTime(30000); // Increased wait time
        generateDataPoints(RAIN_PATH, size, 3, 7.5);
        mockResult2.assertIsSatisfied();

        mockResult1.getReceivedExchanges().forEach(exchange -> assertFromExchange(exchange, TEMPERATURE_TOPIC, size));
        assertTrue(mockResult2.getReceivedExchanges().isEmpty());

        context.removeRoute("route3");
        dropTimeseriesPathQuietly("root.test.demo_device_1.**");
    }
}
