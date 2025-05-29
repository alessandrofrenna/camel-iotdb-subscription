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
import java.util.function.Supplier;

import org.apache.iotdb.session.subscription.consumer.ConsumeListener;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class IoTDBTopicConsumerManagerTest {
    // A small subclass for testing that returns a mocked consumer
    static class TestableManager extends IoTDBTopicConsumerManager.Default {
        final SubscriptionPushConsumer mockConsumer = mock(SubscriptionPushConsumer.class);

        TestableManager(Supplier<IoTDBSessionConfiguration> cfgSupplier) {
            super(cfgSupplier);
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

    @Mock
    private ConsumeListener consumeListener;

    private IoTDBTopicConsumerConfiguration topicConsumerConfiguration;

    private TestableManager consumerManager;
    private Map<PushConsumerKey, SubscriptionPushConsumer> consumerRegistry;
    private TopicAwareConsumeListener topicAwareConsumeListener;

    @BeforeEach
    void setUp() {
        topicConsumerConfiguration = new IoTDBTopicConsumerConfiguration();
        topicConsumerConfiguration.setGroupId("group_a");
        topicConsumerConfiguration.setConsumerId("consumer_a");
        consumerManager = new TestableManager(() -> new IoTDBSessionConfiguration("h", 1234, "u", "p"));
        consumerRegistry = getField(consumerManager, "consumerRegistry");
        topicAwareConsumeListener = new TopicAwareConsumeListener("test_topic1", consumeListener);
    }

    @AfterEach
    void tearDown() {
        consumerRegistry.clear();
    }

    @Test
    void when_multipleConsumersWithTheSameKeyAreCreated_theSameConsumerInstance_shouldBeReturned() {
        final PushConsumerKey consumerKey = new PushConsumerKey(
                topicConsumerConfiguration.getGroupId().get(),
                topicConsumerConfiguration.getConsumerId().get());

        var topicAwareConsumeListener2 = topicAwareConsumeListener.reuseWithTopic("test_topic2");
        var c1 = consumerManager.createPushConsumer(topicConsumerConfiguration, topicAwareConsumeListener);
        var c2 = consumerManager.createPushConsumer(topicConsumerConfiguration, topicAwareConsumeListener2);

        assertSame(consumerManager.mockConsumer, c1, "First call should return our mock");
        assertSame(consumerManager.mockConsumer, c2, "Second call with same key should return same mock");
        assertEquals(1, consumerRegistry.size(), "Exactly one key registered");
        assertTrue(consumerRegistry.containsKey(consumerKey));
    }

    @Test
    void when_multipleConsumersWithDifferentKeysAreCreated_differentConsumerInstances_shouldBeReturned() {
        IoTDBTopicConsumerConfiguration secondCfg = new IoTDBTopicConsumerConfiguration();
        secondCfg.setGroupId("group_b");
        secondCfg.setConsumerId("consumer_b");

        var topicAwareConsumeListener2 = topicAwareConsumeListener.reuseWithTopic("test_topic2");
        consumerManager.createPushConsumer(topicConsumerConfiguration, topicAwareConsumeListener);
        SubscriptionPushConsumer result = consumerManager.createPushConsumer(secondCfg, topicAwareConsumeListener2);

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
    void when_aConsumerIsDestroyed_theCloseMethodShouldBeInvoked_andTheConsumerShouldBeRemovedFromTheRegistry() {
        final PushConsumerKey consumerKey = new PushConsumerKey(
                topicConsumerConfiguration.getGroupId().get(),
                topicConsumerConfiguration.getConsumerId().get());
        consumerManager.createPushConsumer(topicConsumerConfiguration, topicAwareConsumeListener);
        consumerManager.destroyPushConsumer(consumerKey);
        verify(consumerManager.mockConsumer).close();
        assertFalse(consumerRegistry.containsKey(consumerKey));
        assertTrue(consumerRegistry.isEmpty(), "Registry must be cleared after destroyPushConsumer");
    }

    @Test
    void when_theConsumerKeyOfANonExistingConsumerIsUsed_nothingShouldBeDone() {
        PushConsumerKey missingKey = new PushConsumerKey(null, null);
        consumerManager.destroyPushConsumer(missingKey);
        assertTrue(consumerRegistry.isEmpty());
    }

    @Test
    void when_theConsumerToDestroyIsSubscribedToMultipleTopics_anExceptionShouldBeThrown_andItShouldNotBeRemoved() {
        final PushConsumerKey consumerKey = new PushConsumerKey(
                topicConsumerConfiguration.getGroupId().get(),
                topicConsumerConfiguration.getConsumerId().get());
        consumerManager.createPushConsumer(topicConsumerConfiguration, topicAwareConsumeListener);

        doThrow(new IllegalStateException("Close failed"))
                .when(consumerManager.mockConsumer)
                .close();
        consumerManager.destroyPushConsumer(consumerKey);

        verify(consumerManager.mockConsumer).close();
        assertFalse(consumerRegistry.isEmpty());
        assertTrue(consumerRegistry.containsKey(consumerKey));
    }

    @Test
    void when_clearAllIsCalled_allRegisteredConsumers_shouldBeDestroyed() {
        // register two different keys
        var secondCfg = new IoTDBTopicConsumerConfiguration();
        secondCfg.setGroupId("group_b");
        secondCfg.setConsumerId("consumer_b");

        consumerManager.createPushConsumer(topicConsumerConfiguration, topicAwareConsumeListener);
        consumerManager.createPushConsumer(secondCfg, topicAwareConsumeListener);
        consumerManager.close();

        verify(consumerManager.mockConsumer, times(2)).close();
        assertTrue(consumerRegistry.isEmpty(), "Registry must be empty after clearAll");
    }
}
