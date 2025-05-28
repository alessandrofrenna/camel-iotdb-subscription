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

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.session.subscription.consumer.AckStrategy;
import org.apache.iotdb.session.subscription.consumer.ConsumeListener;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface IoTDBTopicConsumerManager extends AutoCloseable {
    SubscriptionPushConsumer createPushConsumer(
            IoTDBTopicConsumerConfiguration consumerCfg, TopicAwareConsumeListener consumeListener);

    void destroyPushConsumer(PushConsumerKey pushConsumerKey);

    void close();

    record PushConsumerKey(String groupId, String consumerId) {
        @Override
        public String toString() {
            return String.format(
                    "%s[consumerGroupId = %s, consumerId = %s]",
                    this.getClass().getSimpleName(), groupId(), consumerId());
        }
    }

    class Default implements IoTDBTopicConsumerManager {
        private static final Logger LOG = LoggerFactory.getLogger(IoTDBTopicConsumerManager.class);
        private final IoTDBSessionConfiguration sessionConfiguration;
        private final Map<PushConsumerKey, SubscriptionPushConsumer> consumerRegistry = new ConcurrentHashMap<>();
        private final Map<PushConsumerKey, RoutedConsumeListener> routedConsumeListenerRegistry =
                new ConcurrentHashMap<>();

        public Default(IoTDBSessionConfiguration sessionConfiguration) {
            this.sessionConfiguration = sessionConfiguration;
        }

        @Override
        public SubscriptionPushConsumer createPushConsumer(
                IoTDBTopicConsumerConfiguration consumerCfg, TopicAwareConsumeListener topicAwareConsumeListener) {
            final String topicName = topicAwareConsumeListener.topicName();
            final ConsumeListener topicConsumeListener = topicAwareConsumeListener.consumeListener();

            PushConsumerKey pushConsumerKey;
            if (consumerCfg.getGroupId().isPresent()
                    && consumerCfg.getConsumerId().isPresent()) {
                pushConsumerKey = new PushConsumerKey(
                        consumerCfg.getGroupId().get(),
                        consumerCfg.getConsumerId().get());
                if (consumerRegistry.containsKey(pushConsumerKey)) {
                    LOG.debug(
                            "Found an existing consumer with key {}. Registering consume listener for topic with name {}",
                            pushConsumerKey,
                            topicName);
                    routedConsumeListenerRegistry
                            .get(pushConsumerKey)
                            .routeConsumeListener(topicName, topicConsumeListener);
                    return consumerRegistry.get(pushConsumerKey);
                }
            }

            // register the new consume listener to use for the topic
            final RoutedConsumeListener routedConsumeListener = new RoutedConsumeListener.Default();
            routedConsumeListener.routeConsumeListener(topicName, topicConsumeListener);
            // create the subscriber
            final SubscriptionPushConsumer pushConsumer = createNewPushConsumer(consumerCfg, routedConsumeListener);
            pushConsumerKey = new PushConsumerKey(pushConsumer.getConsumerGroupId(), pushConsumer.getConsumerId());
            consumerRegistry.put(pushConsumerKey, pushConsumer);
            routedConsumeListenerRegistry.put(pushConsumerKey, routedConsumeListener);

            LOG.debug(
                    "Created a new push consumer with key {}. Registered consume listener for topic with name {}",
                    pushConsumerKey,
                    topicName);
            return pushConsumer;
        }

        @Override
        public void destroyPushConsumer(PushConsumerKey pushConsumerKey) {
            if (!consumerRegistry.containsKey(pushConsumerKey)) {
                LOG.warn("No consumer found for key {}", pushConsumerKey);
                return;
            }

            try {
                // The key should be removed only when close doesn't throw an exception
                consumerRegistry.get(pushConsumerKey).close();
                consumerRegistry.remove(pushConsumerKey);
                routedConsumeListenerRegistry.remove(pushConsumerKey).clear();
                LOG.debug("Consumer with key {} was destroyed. Removed routed consume listeners", pushConsumerKey);
            } catch (Exception e) {
                LOG.error("Error destroying consumer with key {}: {}", pushConsumerKey, e.getMessage());
            }
        }

        @Override
        public void close() {
            new ArrayList<>(consumerRegistry.keySet()).forEach(this::destroyPushConsumer);
            LOG.debug("Cleared all mapped consumers and routed consume listeners");
            consumerRegistry.clear();
        }

        SubscriptionPushConsumer createNewPushConsumer(
                IoTDBTopicConsumerConfiguration consumerCfg, ConsumeListener consumeListener) {
            var consumerBuilder = new StatefulSubscriptionPushConsumer.Builder()
                    .host(sessionConfiguration.host())
                    .port(sessionConfiguration.port())
                    .username(sessionConfiguration.user())
                    .password(sessionConfiguration.password())
                    .heartbeatIntervalMs(consumerCfg.getHeartbeatIntervalMs())
                    .endpointsSyncIntervalMs(consumerCfg.getSyncIntervalMs())
                    .ackStrategy(AckStrategy.AFTER_CONSUME)
                    .consumeListener(consumeListener);
            consumerCfg.getConsumerId().ifPresent(consumerBuilder::consumerId);
            consumerCfg.getGroupId().ifPresent(consumerBuilder::consumerGroupId);

            return new StatefulSubscriptionPushConsumer(consumerBuilder);
        }

        private static class StatefulSubscriptionPushConsumer extends SubscriptionPushConsumer {
            private static final Logger LOG = LoggerFactory.getLogger(StatefulSubscriptionPushConsumer.class);
            private final AtomicLong subscribedTopicCount = new AtomicLong(0);

            protected StatefulSubscriptionPushConsumer(Builder builder) {
                super(builder);
            }

            @Override
            public void subscribe(String topicName) throws SubscriptionException {
                super.subscribe(topicName);
                long count = subscribedTopicCount.incrementAndGet();
                LOG_COUNT(count);
            }

            @Override
            public void unsubscribe(String topicName) throws SubscriptionException {
                super.unsubscribe(topicName);
                long count = subscribedTopicCount.decrementAndGet();
                if (count < 0) {
                    subscribedTopicCount.set(0);
                    LOG_COUNT(0);
                } else {
                    LOG_COUNT(count);
                }
            }

            @Override
            public synchronized void close() {
                final var consumerKey = new PushConsumerKey(getConsumerGroupId(), getConsumerId());
                final var count = subscribedTopicCount.longValue();
                if (count == 0) {
                    super.close();
                    LOG.debug("Consumer with key {} closed", consumerKey);
                } else {
                    final String msg = String.format(
                            "Consumer with key %s is subscribed to %s and will not be closed", consumerKey, count);
                    throw new IllegalStateException(msg);
                }
            }

            private void LOG_COUNT(long count) {
                final var consumerKey = new PushConsumerKey(getConsumerGroupId(), getConsumerId());
                LOG.debug("Consumer with key {} is subscribed to #{} topics", consumerKey, count);
            }
        }
    }
}
