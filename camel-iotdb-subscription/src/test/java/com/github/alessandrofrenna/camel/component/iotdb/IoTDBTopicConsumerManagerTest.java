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

import static com.github.alessandrofrenna.camel.component.iotdb.IoTDBTopicConsumerManager.PushConsumerKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.iotdb.session.subscription.consumer.ConsumeListener;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

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

    private AutoCloseable closeable;

    @Mock
    private ConsumeListener consumeListener;

    private IoTDBTopicConsumerConfiguration topicConsumerConfiguration;

    private TestableManager consumerManager;
    private Map<PushConsumerKey, SubscriptionPushConsumer> consumerRegistry;

    @BeforeEach
    void setUp() {
        closeable = MockitoAnnotations.openMocks(this);

        topicConsumerConfiguration = new IoTDBTopicConsumerConfiguration();
        topicConsumerConfiguration.setGroupId("group_a");
        topicConsumerConfiguration.setConsumerId("consumer_a");
        consumerManager = new TestableManager(new IoTDBSessionConfiguration("h", 1234, "u", "p"));
        consumerRegistry = getField(consumerManager, "consumerRegistry");
    }

    @AfterEach
    void tearDown() throws Exception {
        closeable.close();
        consumerRegistry.clear();
    }

    @Test
    void creating_multiple_consumers_with_the_same_key_should_produce_the_same_consumer() {
        final PushConsumerKey consumerKey = new PushConsumerKey(
                topicConsumerConfiguration.getGroupId().get(),
                topicConsumerConfiguration.getConsumerId().get());

        var c1 = consumerManager.createPushConsumer(topicConsumerConfiguration, consumeListener);
        var c2 = consumerManager.createPushConsumer(topicConsumerConfiguration, consumeListener);

        assertSame(consumerManager.mockConsumer, c1, "First call should return our mock");
        assertSame(consumerManager.mockConsumer, c2, "Second call with same key should return same mock");
        assertEquals(1, consumerRegistry.size(), "Exactly one key registered");
        assertTrue(consumerRegistry.containsKey(consumerKey));
    }

    @Test
    void creating_multiple_consumers_with_different_keys_should_produce_different_consumers() {
        IoTDBTopicConsumerConfiguration secondCfg = new IoTDBTopicConsumerConfiguration();
        secondCfg.setGroupId("group_b");
        secondCfg.setConsumerId("consumer_b");

        consumerManager.createPushConsumer(topicConsumerConfiguration, consumeListener);
        SubscriptionPushConsumer result = consumerManager.createPushConsumer(secondCfg, consumeListener);

        final PushConsumerKey firstConsumerKey = new PushConsumerKey(
                topicConsumerConfiguration.getGroupId().get(),
                topicConsumerConfiguration.getConsumerId().get());
        final PushConsumerKey secondConsumerKey = new PushConsumerKey(
                secondCfg.getGroupId().get(), secondCfg.getConsumerId().get());
        assertEquals(2, consumerRegistry.size());
        assertTrue(consumerRegistry.containsKey(firstConsumerKey));
        assertTrue(consumerRegistry.containsKey(secondConsumerKey));
        assertSame(consumerManager.mockConsumer, consumerRegistry.get(firstConsumerKey));
        assertSame(consumerManager.mockConsumer, consumerRegistry.get(secondConsumerKey));
    }

    @Test
    void destroy_push_consumer_should_invoke_close_method_and_remove_from_the_registry() {
        final PushConsumerKey consumerKey = new PushConsumerKey(
                topicConsumerConfiguration.getGroupId().get(),
                topicConsumerConfiguration.getConsumerId().get());
        consumerManager.createPushConsumer(topicConsumerConfiguration, consumeListener);
        consumerManager.destroyPushConsumer(consumerKey);
        verify(consumerManager.mockConsumer).close();
        assertFalse(consumerRegistry.containsKey(consumerKey));
        assertTrue(consumerRegistry.isEmpty(), "Registry must be cleared after destroyPushConsumer");
    }

    @Test
    void destroy_push_consumer_with_a_non_existing_consumer_key_should_do_nothing() {
        PushConsumerKey missingKey = new PushConsumerKey(null, null);
        consumerManager.destroyPushConsumer(missingKey);
        assertTrue(consumerRegistry.isEmpty());
    }

    @Test
    void destroy_push_consumer_when_subscribed_to_multiple_topic_should_throw_exception_and_is_not_removed() {
        final PushConsumerKey consumerKey = new PushConsumerKey(
                topicConsumerConfiguration.getGroupId().get(),
                topicConsumerConfiguration.getConsumerId().get());
        consumerManager.createPushConsumer(topicConsumerConfiguration, consumeListener);

        doThrow(new IllegalStateException("Close failed"))
                .when(consumerManager.mockConsumer)
                .close();
        consumerManager.destroyPushConsumer(consumerKey);

        verify(consumerManager.mockConsumer).close();
        assertFalse(consumerRegistry.isEmpty());
        assertTrue(consumerRegistry.containsKey(consumerKey));
    }

    @Test
    void clear_all_destroys_all_registered_consumers() {
        // register two different keys
        var secondCfg = new IoTDBTopicConsumerConfiguration();
        secondCfg.setGroupId("group_b");
        secondCfg.setConsumerId("consumer_b");

        consumerManager.createPushConsumer(topicConsumerConfiguration, consumeListener);
        consumerManager.createPushConsumer(secondCfg, consumeListener);
        consumerManager.close();

        verify(consumerManager.mockConsumer, times(2)).close();
        assertTrue(consumerRegistry.isEmpty(), "Registry must be empty after clearAll");
    }
}
