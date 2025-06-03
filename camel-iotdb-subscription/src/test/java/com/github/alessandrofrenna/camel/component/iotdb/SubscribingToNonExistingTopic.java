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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.apache.camel.RuntimeCamelException;
import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import com.github.alessandrofrenna.camel.component.iotdb.support.IoTDBTestSupport;

public class SubscribingToNonExistingTopic extends IoTDBTestSupport {
    @Test
    void when_subscribingToAMissingTopic_theRouteShouldBeStopped_afterThrow() {
        final String MISSING_TOPIC = "missing_topic";
        final String MISSING_TOPIC_ROUTE_ID = "routeOfMissingTopic";

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
}
