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

public class SingleEndpointSingleTopicTest extends IoTDBTestSupport {
    private final String TEMPERATURE_TOPIC = "temp_topic";
    private final String TEMPERATURE_PATH = "root.test.demo_device_1.sensor_1.temperature";

    @Test
    void when_route1_isStarted_shouldReceiveMessagesFromTempTopic() throws Exception {
        createTimeseriesPathQuietly(TEMPERATURE_PATH);
        createTopicQuietly(TEMPERATURE_TOPIC, TEMPERATURE_PATH);

        MockEndpoint mockResult1 = getMockEndpoint("mock:result1");
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                // spotless:off
                from("iotdb-subscription:test_group:test_consumer_a?subscribeTo=" + TEMPERATURE_TOPIC)
                        .routeId("route1")
                        .to(mockResult1);
                // spotless:on
            }
        });

        int size = 10;
        mockResult1.expectedMinimumMessageCount(1);
        mockResult1.expectedMessagesMatches(exchange -> exchange.getIn().getBody() instanceof SubscriptionMessage);
        mockResult1.setResultWaitTime(30_000);

        generateDataPoints(TEMPERATURE_PATH, size, 20.5, 25.5);
        mockResult1.assertIsSatisfied();

        mockResult1.getReceivedExchanges().forEach(exchange -> assertFromExchange(exchange, TEMPERATURE_TOPIC, size));
        context.removeRoute("route1");

        dropTimeseriesPathQuietly("root.test.demo_device_1.**");
    }
}
