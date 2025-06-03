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

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.junit.jupiter.api.Test;

import com.github.alessandrofrenna.camel.component.iotdb.support.IoTDBTestSupport;

public class SingleEndpointMultipleTopic extends IoTDBTestSupport {
    private final String TEMPERATURE_TOPIC = "temp_topic";
    private final String TEMPERATURE_PATH = "root.test.demo_device_1.sensor_1.temperature";
    private final String RAIN_TOPIC = "rain_topic";
    private final String RAIN_PATH = "root.test.demo_device_1.sensor_2.rain";

    @Test
    void when_aConsumerIsSubscribedToMultipleTopics_theMessagesShouldBeReceived() throws Exception {
        createTimeseriesPathQuietly(TEMPERATURE_PATH);
        createTopicQuietly(TEMPERATURE_TOPIC, TEMPERATURE_PATH);
        MockEndpoint mockTempTopic = getMockEndpoint("mock:temp_topic");

        createTimeseriesPathQuietly(RAIN_PATH);
        createTopicQuietly(RAIN_TOPIC, RAIN_PATH);

        MockEndpoint mockRainTopicEp = getMockEndpoint("mock:rain_topic");

        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                // spotless:off
                from(String.format("iotdb-subscription:test_group2:test_consumer2?subscribeTo=%s,%s", TEMPERATURE_TOPIC, RAIN_TOPIC))
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
        mockTempTopic.setResultWaitTime(3000); // Increased wait time

        generateDataPoints(TEMPERATURE_PATH, size, 20.5, 25.5);
        mockTempTopic.assertIsSatisfied();

        mockRainTopicEp.expectedMinimumMessageCount(1);
        mockRainTopicEp.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);
        mockTempTopic.setResultWaitTime(3000); // Increased wait time

        generateDataPoints(RAIN_PATH, size, 3, 7.5);
        mockRainTopicEp.assertIsSatisfied();

        mockTempTopic.getReceivedExchanges().forEach(exchange -> assertFromExchange(exchange, TEMPERATURE_TOPIC, size));
        mockRainTopicEp.getReceivedExchanges().forEach(exchange -> assertFromExchange(exchange, RAIN_TOPIC, size));

        context.removeRoute("route2");

        dropTimeseriesPathQuietly("root.test.demo_device_1.**");
    }
}
