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

/**
 * The <b>IoTDBTopicConsumerManager</b> interface defines the methods to handle consumer operations.
 */
public interface IoTDBTopicConsumerManager extends AutoCloseable {
    /**
     * Create a {@link SubscriptionPushConsumer} instance.
     *
     * @param consumerCfg of the consumer
     * @param topicAwareConsumeListener wraps the listener that will be used to handle incoming message
     * @return a push consumer
     */
    SubscriptionPushConsumer createPushConsumer(
            IoTDBTopicConsumerConfiguration consumerCfg, TopicAwareConsumeListener topicAwareConsumeListener);

    /**
     * Destroy a {@link SubscriptionPushConsumer} instance by its id.
     *
     * @param pushConsumerKey that identifies a consumer
     */
    void destroyPushConsumer(PushConsumerKey pushConsumerKey);

    /**
     * Close the {@link IoTDBTopicConsumerManager} instance.<br>
     *
     * {@inheritDoc}
     */
    void close();

    /**
     * The <b>PushConsumerKey</b> record defines a {@link SubscriptionPushConsumer} id.
     *
     * @param groupId of which the consumer is member
     * @param consumerId is the id of the consumer
     */
    record PushConsumerKey(String groupId, String consumerId) {
        /**
         * Get a string version of the {@link PushConsumerKey} instance.
         *
         * @return a string version of the key
         */
        @Override
        public String toString() {
            return String.format(
                    "%s[consumerGroupId = %s, consumerId = %s]",
                    this.getClass().getSimpleName(), groupId(), consumerId());
        }
    }

    /**
     * The <b>Default</b> class implements {@link IoTDBTopicConsumerManager} interface.<br>
     * It is used as delegate to handle operations on {@link SubscriptionPushConsumer}.
     */
    class Default implements IoTDBTopicConsumerManager {
        private static final Logger LOG = LoggerFactory.getLogger(IoTDBTopicConsumerManager.class);
        private final IoTDBSessionConfiguration sessionConfiguration;
        private final Map<PushConsumerKey, SubscriptionPushConsumer> consumerRegistry = new ConcurrentHashMap<>();
        private final Map<PushConsumerKey, RoutedConsumeListener> routedConsumeListenerRegistry =
                new ConcurrentHashMap<>();

        /**
         * Create an {@link IoTDBTopicConsumerManager} instance.
         *
         * @param sessionConfiguration needed to create the consumer
         */
        public Default(IoTDBSessionConfiguration sessionConfiguration) {
            this.sessionConfiguration = sessionConfiguration;
        }

        /**
         * {@inheritDoc}
         *
         * This implementation will cache the created {@link SubscriptionPushConsumer} inside an in memory cache.<br>
         * An instance of {@link RoutedConsumeListener} will be also cached.
         * The {@link TopicAwareConsumeListener} will be used register a new {@link ConsumeListener} inside the cached
         * {@link RoutedConsumeListener}.
         *
         * @param consumerCfg of the consumer
         * @param topicAwareConsumeListener wraps the listener that will be used to handle incoming message
         * @return a push consumer
         */
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

        /**
         * {@inheritDoc}
         *
         * This implementation proceeds to remove {@link SubscriptionPushConsumer} after
         * {@link SubscriptionPushConsumer#close()}.<br>
         * After that it will remove {@link RoutedConsumeListener} from the cache as well.<br>
         * When {@link SubscriptionPushConsumer#close()} fails nothing will be done.
         * The fail could happen when the consumer is subscribed to other topics or for other reasons.
         *
         * @param pushConsumerKey that identifies a consumer
         */
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

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() {
            new ArrayList<>(consumerRegistry.keySet()).forEach(this::destroyPushConsumer);
            LOG.debug("Cleared all mapped consumers and routed consume listeners");
            consumerRegistry.clear();
            routedConsumeListenerRegistry.clear();
        }

        /**
         * Handle the creation of a new  {@link SubscriptionPushConsumer}.
         *
         * @param consumerCfg of the consumer
         * @param consumeListener the routed consume listener
         * @return the new push consumer
         */
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

        /**
         * The <b>StatefulSubscriptionPushConsumer</b> class extends a {@link SubscriptionPushConsumer}.<br>
         * This custom version of a {@link SubscriptionPushConsumer} is a decorator that keeps the reference count
         * of the subscribed topics.
         */
        private static class StatefulSubscriptionPushConsumer extends SubscriptionPushConsumer {
            private static final Logger LOG = LoggerFactory.getLogger(StatefulSubscriptionPushConsumer.class);
            private final AtomicLong subscribedTopicCount = new AtomicLong(0);

            /**
             * Create a {@link StatefulSubscriptionPushConsumer} instance.
             *
             * @param builder to create the consumer
             */
            protected StatefulSubscriptionPushConsumer(Builder builder) {
                super(builder);
            }

            /**
             * {@inheritDoc}
             *
             * Subscribe to a topic by its name. When subscription succeed increase the reference count.
             *
             * @param topicName to subscribe to
             * @throws SubscriptionException when subscription fails
             */
            @Override
            public void subscribe(String topicName) throws SubscriptionException {
                super.subscribe(topicName);
                long count = subscribedTopicCount.incrementAndGet();
                LOG_COUNT(count);
            }

            /**
             * {@inheritDoc}
             *
             * Unsubscribe from a topic by its name. When un-subscription succeed decrement the reference count.
             * If the reference count become negative, restore its value to 0.
             *
             * @param topicName to unsubscribe from
             */
            @Override
            public void unsubscribe(String topicName) {
                try {
                    super.unsubscribe(topicName);
                    long count = subscribedTopicCount.decrementAndGet();
                    if (count < 0) {
                        subscribedTopicCount.set(0);
                        LOG_COUNT(0);
                    } else {
                        LOG_COUNT(count);
                    }
                    LOG.info("Consumer with key {} unsubscribed from topic '{}'", consumerId, topicName);
                } catch (SubscriptionException e) {
                    LOG.error(
                            "Unsubscribe operation for consumer with key {} from topic '{}', failed: {}",
                            consumerId,
                            topicName,
                            e.getMessage());
                }
            }

            /**
             * {@inheritDoc}
             *
             * Close a {@link SubscriptionPushConsumer} only if its reference count is 0.<br>
             * When the reference count is > 0 this method will throw an {@link IllegalArgumentException}.
             */
            @Override
            public synchronized void close() {
                final var consumerKey = new PushConsumerKey(getConsumerGroupId(), getConsumerId());
                final var count = subscribedTopicCount.longValue();
                if (count == 0) {
                    super.close();
                    LOG.info("Consumer with key {} closed", consumerKey);
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
