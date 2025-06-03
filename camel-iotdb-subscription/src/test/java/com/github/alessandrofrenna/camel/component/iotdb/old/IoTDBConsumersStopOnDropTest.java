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

package com.github.alessandrofrenna.camel.component.iotdb.old;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.spi.CamelEvent;
import org.apache.camel.spi.EventNotifier;
import org.apache.camel.support.EventNotifierSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import com.github.alessandrofrenna.camel.component.iotdb.old.event.IoTDBResumeAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.old.event.IoTDBStopAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.old.event.IoTDBTopicConsumerSubscribed;
import com.github.alessandrofrenna.camel.component.iotdb.old.event.IoTDBTopicDropped;
import com.github.alessandrofrenna.camel.component.iotdb.support.IoTDBTestSupport;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class IoTDBConsumersStopOnDropTest extends IoTDBTestSupport {
    private final String TEMPERATURE_TOPIC = "temp_topic";
    private final String RAIN_TOPIC = "rain_topic";

    private final Map<Class<? extends CamelEvent>, Integer> publishedEvents = new HashMap<>();
    private final EventNotifier eventNotifier = new EventNotifierSupport() {
        private void findAndIncrement(Class<? extends CamelEvent> evType) {
            var count = publishedEvents.getOrDefault(evType, 0);
            publishedEvents.put(evType, count + 1);
        }

        @Override
        public void notify(CamelEvent event) {
            if (event instanceof IoTDBTopicConsumerSubscribed) {
                findAndIncrement(IoTDBTopicConsumerSubscribed.class);
            } else if (event instanceof IoTDBStopAllTopicConsumers) {
                findAndIncrement(IoTDBStopAllTopicConsumers.class);
            } else if (event instanceof IoTDBTopicDropped) {
                findAndIncrement(IoTDBTopicDropped.class);
            } else if (event instanceof IoTDBResumeAllTopicConsumers) {
                findAndIncrement(IoTDBResumeAllTopicConsumers.class);
            } else if (event instanceof CamelEvent.RouteRemovedEvent) {
                findAndIncrement(CamelEvent.RouteRemovedEvent.class);
            }
        }
    };

    @BeforeEach
    void setUpTestSuite() throws Exception {
        final String TEMPERATURE_PATH = "root.test.demo_device_1.sensor_1.temperature";
        final String RAIN_PATH = "root.test.demo_device_1.sensor_2.rain";

        context().getManagementStrategy().addEventNotifier(eventNotifier);
        createTimeseriesPathQuietly(TEMPERATURE_PATH);
        createTimeseriesPathQuietly(RAIN_PATH);
        createTopicQuietly(TEMPERATURE_TOPIC, TEMPERATURE_PATH);
        createTopicQuietly(RAIN_TOPIC, RAIN_PATH);

        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("iotdb-subscription:" + RAIN_TOPIC + "?groupId=group_1&consumerId=first_consumer")
                        .to("mock:result");
                from("iotdb-subscription:" + RAIN_TOPIC + "?groupId=group_1&consumerId=second_consumer")
                        .to("mock:result");
                from("iotdb-subscription:" + RAIN_TOPIC + "?groupId=group_2&consumerId=first_consumer")
                        .to("mock:result");
                from("iotdb-subscription:" + TEMPERATURE_TOPIC + "?groupId=group_1&consumerId=first_consumer")
                        .to("mock:result");
            }
        });
    }

    @Test
    public void when_topicDropOperationIsSuccessful_consumersShouldBeClosedAndRemoved() {
        template.sendBody("iotdb-subscription:rain_topic?action=drop", null);
        assertTrue(publishedEvents.containsKey(IoTDBTopicConsumerSubscribed.class));
        assertEquals(4, publishedEvents.get(IoTDBTopicConsumerSubscribed.class));
        assertTrue(publishedEvents.containsKey(IoTDBStopAllTopicConsumers.class));
        assertEquals(1, publishedEvents.get(IoTDBStopAllTopicConsumers.class));

        Awaitility.await()
                .atMost(Duration.ofSeconds(3))
                .pollInterval(IoTDBTopicProducerConfiguration.PRE_DROP_DELAY)
                .untilAsserted(() -> {
                    assertTrue(publishedEvents.containsKey(IoTDBTopicDropped.class));
                    assertEquals(1, publishedEvents.get(IoTDBTopicDropped.class));
                });
    }
}
