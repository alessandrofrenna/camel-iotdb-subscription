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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockConstruction;

import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBResumeAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBStopAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicDropped;
import com.github.alessandrofrenna.camel.component.iotdb.support.IoTDBTestSupport;
import java.util.HashMap;
import java.util.Map;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.spi.CamelEvent;
import org.apache.camel.spi.EventNotifier;
import org.apache.camel.support.EventNotifierSupport;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IoTDBTopicActionsTests extends IoTDBTestSupport {
    private final Map<Class<? extends CamelEvent>, Integer> publishedEvents = new HashMap<>();
    private final EventNotifier eventNotifier = new EventNotifierSupport() {
        @Override
        public void notify(CamelEvent event) {
            if (event instanceof IoTDBStopAllTopicConsumers) {
                publishedEvents.putIfAbsent(IoTDBStopAllTopicConsumers.class, 1);
            } else if (event instanceof IoTDBResumeAllTopicConsumers) {
                publishedEvents.putIfAbsent(IoTDBResumeAllTopicConsumers.class, 1);
            } else if (event instanceof IoTDBTopicDropped) {
                publishedEvents.putIfAbsent(IoTDBTopicDropped.class, 1);
            }
        }
    };
    private ProducerTemplate producerTemplate;

    @BeforeEach
    void init() {
        producerTemplate = context.createProducerTemplate();
        context().getManagementStrategy().addEventNotifier(eventNotifier);
    }

    @AfterEach
    void tearDownProducerTemplate() {
        publishedEvents.clear();
        producerTemplate.stop();
        producerTemplate = null;
        context.getManagementStrategy().removeEventNotifier(eventNotifier);
    }

    @Test
    public void create_rain_topic_on_iotdb() {
        doInSession((session) -> assertTrue(session.getTopic("rain_topic").isEmpty()));
        producerTemplate.sendBody("iotdb-subscription:rain_topic?action=create&path=root.test.demo_device.rain", null);

        doInSession((session) -> {
            var topics = session.getTopics();
            assertFalse(topics.isEmpty(), "topic set is not empty");
            assertEquals(1, topics.size(), "topic set size is one");
            assertTrue(session.getTopic("rain_topic").isPresent(), "topic with name 'rain_topic' found");
        });
    }

    @Test
    public void drop_rain_topic_from_iotdb() {
        producerTemplate.sendBody("iotdb-subscription:rain_topic?action=create&path=root.test.demo_device.rain", null);

        doInSession((session) ->
                assertTrue(session.getTopic("rain_topic").isPresent(), "topic with name 'rain_topic' found"));

        producerTemplate.sendBody("iotdb-subscription:rain_topic?action=drop", null);

        doInSession((session) -> {
            var topics = session.getTopics();
            assertTrue(topics.isEmpty(), "topic set is empty");
            assertTrue(session.getTopic("rain_topic").isEmpty(), "topic with name 'rain_topic' not found");
        });
    }

    @Test
    public void drop_action_success_events_publication() {
        producerTemplate.sendBody("iotdb-subscription:rain_topic?action=drop", null);

        assertEquals(2, publishedEvents.size());
        assertTrue(
                publishedEvents.containsKey(IoTDBStopAllTopicConsumers.class),
                "IoTDBStopAllTopicConsumers events was published");
        assertEquals(
                1,
                publishedEvents.get(IoTDBStopAllTopicConsumers.class),
                "only 1 IoTDBStopAllTopicConsumers event was published");
        assertTrue(publishedEvents.containsKey(IoTDBTopicDropped.class), "IoTDBTopicDropped events was published");
        assertEquals(1, publishedEvents.get(IoTDBTopicDropped.class), "only 1 IoTDBTopicDropped event was published");
    }

    @Test
    public void drop_action_fails_events_publication() {
        try (var ignored =
                mockConstruction(SubscriptionSession.class, (mock, ctx) -> doThrow(new IoTDBConnectionException(""))
                        .when(mock)
                        .open())) {
            producerTemplate.sendBody("iotdb-subscription:rain_topic?action=drop", null);
        }

        assertEquals(2, publishedEvents.size());
        assertTrue(
                publishedEvents.containsKey(IoTDBStopAllTopicConsumers.class),
                "IoTDBStopAllTopicConsumers events was published");
        assertEquals(
                1,
                publishedEvents.get(IoTDBStopAllTopicConsumers.class),
                "only 1 IoTDBStopAllTopicConsumers event was published");
        assertTrue(
                publishedEvents.containsKey(IoTDBResumeAllTopicConsumers.class),
                "IoTDBResumeAllTopicConsumers events was published");
        assertEquals(
                1,
                publishedEvents.get(IoTDBResumeAllTopicConsumers.class),
                "only 1 IoTDBResumeAllTopicConsumers event was published");
    }
}
