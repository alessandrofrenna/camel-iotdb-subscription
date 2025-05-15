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

import org.junit.jupiter.api.Test;

public class IoTDBTopicActionsTests extends IoTDBTestSupport {

    @Test
    public void create_rain_topic_on_iotdb() {
        doInSession((session) -> assertTrue(session.getTopic("rain_topic").isEmpty()));
        context.createProducerTemplate()
                .sendBody("iotdb-subscription:rain_topic?action=create&path=root.test.demo_device.rain", null);

        doInSession((session) -> {
            var topics = session.getTopics();
            assertFalse(topics.isEmpty(), "topic set is not empty");
            assertEquals(1, topics.size(), "topic set size is one");
            assertTrue(session.getTopic("rain_topic").isPresent(), "topic with name 'rain_topic' found");
        });
    }

    @Test
    public void drop_rain_topic_from_iotdb() {
        context.createProducerTemplate()
                .sendBody("iotdb-subscription:rain_topic?action=create&path=root.test.demo_device.rain", null);

        doInSession((session) ->
                assertTrue(session.getTopic("rain_topic").isPresent(), "topic with name 'rain_topic' found"));

        context.createProducerTemplate().sendBody("iotdb-subscription:rain_topic?action=drop", null);

        doInSession((session) -> {
            var topics = session.getTopics();
            assertTrue(topics.isEmpty(), "topic set is empty");
            assertTrue(session.getTopic("rain_topic").isEmpty(), "topic with name 'rain_topic' not found");
        });
    }
}
