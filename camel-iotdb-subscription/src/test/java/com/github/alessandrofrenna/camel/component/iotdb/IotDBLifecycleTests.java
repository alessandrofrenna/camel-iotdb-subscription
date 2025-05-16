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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBResumeAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBStopAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicConsumerSubscribed;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicDropped;
import com.github.alessandrofrenna.camel.component.iotdb.event.IotDBTopicConsumerUnsubscribed;
import com.github.alessandrofrenna.camel.component.iotdb.support.IoTDBTestSupport;
import java.util.HashMap;
import java.util.Map;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.Route;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.spi.CamelEvent;
import org.apache.camel.spi.EventNotifier;
import org.apache.camel.support.EventNotifierSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IotDBLifecycleTests extends IoTDBTestSupport {
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
            } else if (event instanceof IotDBTopicConsumerUnsubscribed) {
                findAndIncrement(IotDBTopicConsumerUnsubscribed.class);
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
    private ProducerTemplate producerTemplate;

    @BeforeEach
    void init() throws Exception {
        producerTemplate = context.createProducerTemplate();
        context().getManagementStrategy().addEventNotifier(eventNotifier);

        producerTemplate.sendBody("iotdb-subscription:rain_topic?action=create&path=root.test.demo_device.rain", null);
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("iotdb-subscription:rain_topic?groupId=group_1&consumerId=first_consumer")
                        .to("mock:consumer1");
                from("iotdb-subscription:rain_topic?groupId=group_1&consumerId=second_consume")
                        .to("mock:consume2");
                from("iotdb-subscription:rain_topic?groupId=group_2&consumerId=first_consume")
                        .to("mock:consumer3");
            }
        });
    }

    @AfterEach
    void tearDown() {
        publishedEvents.clear();
        producerTemplate.stop();
        producerTemplate = null;

        for (Route route : context.getRoutes()) {
            try {
                var routeId = route.getRouteId();
                context.removeRoute(routeId);
            } catch (Exception ignored) {
                // NO-OP
            }
        }
        context.getManagementStrategy().removeEventNotifier(eventNotifier);
    }

    @Test
    public void successful_topic_and_multiple_subscribers_creation_lifecycle() {
        producerTemplate.sendBody("iotdb-subscription:rain_topic?action=drop", null);
        assertTrue(
                publishedEvents.containsKey(IoTDBTopicConsumerSubscribed.class),
                "IoTDBTopicConsumerSubscribed events was published");
        assertEquals(
                3,
                publishedEvents.get(IoTDBTopicConsumerSubscribed.class),
                "3 IoTDBTopicConsumerSubscribed event was published");
        assertTrue(
                publishedEvents.containsKey(IoTDBStopAllTopicConsumers.class),
                "IoTDBStopAllTopicConsumers events was published");
        assertEquals(
                1,
                publishedEvents.get(IoTDBStopAllTopicConsumers.class),
                "only 1 IoTDBStopAllTopicConsumers event was published");
        //        assertTrue(
        //                publishedEvents.containsKey(IotDBTopicConsumerUnsubscribed.class),
        //                "IotDBTopicConsumerUnsubscribed events was published");
        //        assertEquals(
        //                3,
        //                publishedEvents.get(IotDBTopicConsumerUnsubscribed.class),
        //                "3 IotDBTopicConsumerUnsubscribed event was published");
        //        assertTrue(publishedEvents.containsKey(IoTDBTopicDropped.class), "IoTDBTopicDropped events was
        // published");
        //        assertEquals(1, publishedEvents.get(IoTDBTopicDropped.class), "only 1 IoTDBTopicDropped event was
        // published");
    }
}
