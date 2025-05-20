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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.iotdb.session.subscription.consumer.ConsumeListener;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IoTDBTopicConsumerManagerTest {
    // A small subclass for testing that returns a mocked consumer
    static class TestableManager extends IoTDBTopicConsumerManager.Default {
        final SubscriptionPushConsumer mockConsumer = mock(SubscriptionPushConsumer.class);

        TestableManager(IoTDBSessionConfiguration cfg) {
            super(cfg);
        }

        @Override
        protected SubscriptionPushConsumer createNewPushConsumer(
                IoTDBTopicConsumerConfiguration cfg, ConsumeListener listener) {
            when(mockConsumer.getConsumerGroupId()).thenReturn(cfg.getGroupId().get());
            when(mockConsumer.getConsumerId()).thenReturn(cfg.getConsumerId().get());
            return mockConsumer;
        }
    }

    @SuppressWarnings("unchecked")
    static <T> T getField(Object container, String fieldName) {
        var parent = container.getClass().getSuperclass();
        try {
            Field field = parent.getDeclaredField(fieldName);
            field.setAccessible(true);
            return (T) field.get(container);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private TestableManager consumerManager;
    private ConsumeListener consumeListener;
    private IoTDBTopicConsumerConfiguration topicConsumerConfiguration;

    @BeforeEach
    void setUp() {
        consumeListener = mock(ConsumeListener.class);
        topicConsumerConfiguration = new IoTDBTopicConsumerConfiguration();
        topicConsumerConfiguration.setGroupId("group_a");
        topicConsumerConfiguration.setConsumerId("consumer_a");
        consumerManager = new TestableManager(new IoTDBSessionConfiguration("h", 1234, "u", "p"));
    }

    @Test
    void creating_multiple_consumer_with_the_same_key_returns_the_same_consumer() {
        Map<?, ?> registry = getField(consumerManager, "consumerRegistry");
        var c1 = consumerManager.createPushConsumer(topicConsumerConfiguration, consumeListener);
        var c2 = consumerManager.createPushConsumer(topicConsumerConfiguration, consumeListener);
        assertSame(consumerManager.mockConsumer, c1, "First call should return our mock");
        assertSame(consumerManager.mockConsumer, c2, "Second call with same key should return same mock");
        assertEquals(1, registry.size(), "Exactly one key registered");
    }

    @Test
    void destroying_an_existing_push_consumer_call_close_and_remove_the_consumer() {
        Map<?, ?> registry = getField(consumerManager, "consumerRegistry");
        consumerManager.createPushConsumer(topicConsumerConfiguration, consumeListener);
        assertEquals(1, registry.size(), "Exactly one key registered");
        // verify we called close() exactly once
        var key = new IoTDBTopicConsumerManager.PushConsumerKey("group_a", "consumer_a");
        consumerManager.destroyPushConsumer(key);
        verify(consumerManager.mockConsumer, times(1)).close();
        assertTrue(registry.isEmpty(), "Registry must be cleared after destroyPushConsumer");
    }

    @Test
    void clear_all_destroys_all_registered_consumers() {
        Map<?, ?> registry = getField(consumerManager, "consumerRegistry");
        // register two different keys
        var topicConsumerConfiguration2 = new IoTDBTopicConsumerConfiguration();
        topicConsumerConfiguration2.setGroupId("group_b");
        topicConsumerConfiguration2.setConsumerId("consumer_b");

        consumerManager.createPushConsumer(topicConsumerConfiguration, consumeListener);
        consumerManager.createPushConsumer(topicConsumerConfiguration2, consumeListener);
        assertEquals(2, registry.size(), "Exactly one key registered");

        consumerManager.clearAll();
        verify(consumerManager.mockConsumer, atLeast(2)).close();
        assertTrue(registry.isEmpty(), "Registry must be empty after clearAll");
    }
}
