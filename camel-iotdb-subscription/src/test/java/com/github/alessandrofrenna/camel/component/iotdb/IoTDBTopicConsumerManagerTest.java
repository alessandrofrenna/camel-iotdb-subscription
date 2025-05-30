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

import static com.github.alessandrofrenna.camel.component.iotdb.IoTDBTopicConsumerManager.PullConsumerKey;
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
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
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
        final SubscriptionPullConsumer mockConsumer = mock(SubscriptionPullConsumer.class);

        TestableManager(Supplier<IoTDBSessionConfiguration> cfgSupplier) {
            super(cfgSupplier);
        }

        @Override
        protected SubscriptionPullConsumer createNewPullConsumer(IoTDBTopicConsumerConfiguration cfg) {
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
    private Map<PullConsumerKey, SubscriptionPullConsumer> consumerRegistry;

    @BeforeEach
    void setUp() {
        topicConsumerConfiguration = new IoTDBTopicConsumerConfiguration();
        topicConsumerConfiguration.setGroupId("group_a");
        topicConsumerConfiguration.setConsumerId("consumer_a");
        consumerManager = new TestableManager(() -> new IoTDBSessionConfiguration("h", 1234, "u", "p"));
        consumerRegistry = getField(consumerManager, "consumerRegistry");
    }

    @AfterEach
    void tearDown() {
        consumerRegistry.clear();
    }

    @Test
    void when_multipleConsumersWithTheSameKeyAreCreated_theSameConsumerInstance_shouldBeReturned() {
        final PullConsumerKey consumerKey = new PullConsumerKey(
                topicConsumerConfiguration.getGroupId().get(),
                topicConsumerConfiguration.getConsumerId().get());

        var c1 = consumerManager.createPullConsumer(topicConsumerConfiguration);
        var c2 = consumerManager.createPullConsumer(topicConsumerConfiguration);

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

        consumerManager.createPullConsumer(topicConsumerConfiguration);
        SubscriptionPullConsumer result = consumerManager.createPullConsumer(secondCfg);

        final PullConsumerKey firstConsumerKey = new PullConsumerKey(
                topicConsumerConfiguration.getGroupId().get(),
                topicConsumerConfiguration.getConsumerId().get());
        final PullConsumerKey secondConsumerKey = new PullConsumerKey(
                secondCfg.getGroupId().get(), secondCfg.getConsumerId().get());
        assertEquals(2, consumerRegistry.size());
        assertTrue(consumerRegistry.containsKey(firstConsumerKey));
        assertTrue(consumerRegistry.containsKey(secondConsumerKey));
        assertSame(consumerManager.mockConsumer, consumerRegistry.get(firstConsumerKey));
        assertSame(consumerManager.mockConsumer, consumerRegistry.get(secondConsumerKey));
    }

    @Test
    void when_aConsumerIsDestroyed_theCloseMethodShouldBeInvoked_andTheConsumerShouldBeRemovedFromTheRegistry() {
        final PullConsumerKey consumerKey = new PullConsumerKey(
                topicConsumerConfiguration.getGroupId().get(),
                topicConsumerConfiguration.getConsumerId().get());
        consumerManager.createPullConsumer(topicConsumerConfiguration);
        consumerManager.destroyPullConsumer(consumerKey);
        verify(consumerManager.mockConsumer).close();
        assertFalse(consumerRegistry.containsKey(consumerKey));
        assertTrue(consumerRegistry.isEmpty(), "Registry must be cleared after destroyPushConsumer");
    }

    @Test
    void when_theConsumerKeyOfANonExistingConsumerIsUsed_nothingShouldBeDone() {
        PullConsumerKey missingKey = new PullConsumerKey(null, null);
        consumerManager.destroyPullConsumer(missingKey);
        assertTrue(consumerRegistry.isEmpty());
    }

    @Test
    void when_theConsumerToDestroyIsSubscribedToMultipleTopics_anExceptionShouldBeThrown_andItShouldNotBeRemoved() {
        final PullConsumerKey consumerKey = new PullConsumerKey(
                topicConsumerConfiguration.getGroupId().get(),
                topicConsumerConfiguration.getConsumerId().get());
        consumerManager.createPullConsumer(topicConsumerConfiguration);

        doThrow(new IllegalStateException("Close failed"))
                .when(consumerManager.mockConsumer)
                .close();
        consumerManager.destroyPullConsumer(consumerKey);

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

        consumerManager.createPullConsumer(topicConsumerConfiguration);
        consumerManager.createPullConsumer(secondCfg);
        consumerManager.close();

        verify(consumerManager.mockConsumer, times(2)).close();
        assertTrue(consumerRegistry.isEmpty(), "Registry must be empty after clearAll");
    }
}
