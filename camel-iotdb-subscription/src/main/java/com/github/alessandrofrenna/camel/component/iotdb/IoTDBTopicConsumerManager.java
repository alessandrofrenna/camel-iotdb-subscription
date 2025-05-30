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
import java.util.function.Supplier;

import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <b>IoTDBTopicConsumerManager</b> interface defines the methods to handle consumer operations.
 */
public interface IoTDBTopicConsumerManager extends AutoCloseable {
    /**
     * Create a {@link SubscriptionPullConsumer} instance.
     *
     * @param consumerCfg of the consumer
     * @return a pull consumer
     */
    SubscriptionPullConsumer createPullConsumer(IoTDBTopicConsumerConfiguration consumerCfg);

    /**
     * Destroy a {@link SubscriptionPullConsumer} instance by its id.
     *
     * @param pullConsumerKey that identifies a consumer
     */
    void destroyPullConsumer(PullConsumerKey pullConsumerKey);

    /**
     * Close the {@link IoTDBTopicConsumerManager} instance.<br>
     *
     * {@inheritDoc}
     */
    void close();

    /**
     * The <b>PullConsumerKey</b> record defines a {@link SubscriptionPullConsumer} id.
     *
     * @param groupId of which the consumer is member
     * @param consumerId is the id of the consumer
     */
    record PullConsumerKey(String groupId, String consumerId) {
        /**
         * Get a string version of the {@link PullConsumerKey} instance.
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
     * It is used as delegate to handle operations on {@link SubscriptionPullConsumer}.
     */
    class Default implements IoTDBTopicConsumerManager {
        private static final Logger LOG = LoggerFactory.getLogger(IoTDBTopicConsumerManager.class);
        private final Supplier<IoTDBSessionConfiguration> sessionConfigurationSupplier;
        private final Map<PullConsumerKey, SubscriptionPullConsumer> consumerRegistry = new ConcurrentHashMap<>();

        /**
         * Create an {@link IoTDBTopicConsumerManager} instance.
         *
         * @param sessionConfigurationSupplier config supplier needed to create the consumer
         */
        public Default(Supplier<IoTDBSessionConfiguration> sessionConfigurationSupplier) {
            this.sessionConfigurationSupplier = sessionConfigurationSupplier;
        }

        /**
         * {@inheritDoc}
         *
         * This implementation will cache the created {@link SubscriptionPullConsumer} inside an in memory cache.<br>
         *
         * @param consumerCfg of the consumer
         * @return a pull consumer
         */
        @Override
        public SubscriptionPullConsumer createPullConsumer(IoTDBTopicConsumerConfiguration consumerCfg) {
            PullConsumerKey pullConsumerKey;
            if (consumerCfg.getGroupId().isPresent()
                    && consumerCfg.getConsumerId().isPresent()) {
                pullConsumerKey = new PullConsumerKey(
                        consumerCfg.getGroupId().get(),
                        consumerCfg.getConsumerId().get());
                if (consumerRegistry.containsKey(pullConsumerKey)) {
                    LOG.debug("Returning an already found an existing consumer with key {}", pullConsumerKey);
                    return consumerRegistry.get(pullConsumerKey);
                }
            }

            // register the new consume listener to use for the topic
            final SubscriptionPullConsumer pullConsumer = createNewPullConsumer(consumerCfg);
            pullConsumerKey = new PullConsumerKey(pullConsumer.getConsumerGroupId(), pullConsumer.getConsumerId());
            consumerRegistry.put(pullConsumerKey, pullConsumer);

            LOG.debug("Created a new pull consumer with key {}.", pullConsumerKey);
            return pullConsumer;
        }

        /**
         * {@inheritDoc}
         *
         * This implementation proceeds to remove {@link SubscriptionPullConsumer} after
         * {@link SubscriptionPullConsumer#close()}.<br>
         * When {@link SubscriptionPullConsumer#close()} fails nothing will be done.
         * The fail could happen when the consumer is subscribed to other topics or for other reasons.
         *
         * @param pullConsumerKey that identifies a consumer
         */
        @Override
        public void destroyPullConsumer(PullConsumerKey pullConsumerKey) {
            if (!consumerRegistry.containsKey(pullConsumerKey)) {
                LOG.warn("No consumer found for key {}", pullConsumerKey);
                return;
            }

            try {
                // The key should be removed only when close doesn't throw an exception
                consumerRegistry.get(pullConsumerKey).close();
                consumerRegistry.remove(pullConsumerKey);
                LOG.debug("Consumer with key {} was destroyed.", pullConsumerKey);
            } catch (Exception e) {
                LOG.error("Error destroying consumer with key {}: {}", pullConsumerKey, e.getMessage());
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() {
            new ArrayList<>(consumerRegistry.keySet()).forEach(this::destroyPullConsumer);
            LOG.debug("Cleared all mapped consumers and routed consume listeners");
            consumerRegistry.clear();
        }

        /**
         * Handle the creation of a new  {@link SubscriptionPullConsumer}.
         *
         * @param consumerCfg of the consumer
         * @return the new pull consumer
         */
        SubscriptionPullConsumer createNewPullConsumer(IoTDBTopicConsumerConfiguration consumerCfg) {
            var sessionConfiguration = sessionConfigurationSupplier.get();
            var consumerBuilder = new StatefulSubscriptionPullConsumer.Builder()
                    .host(sessionConfiguration.host())
                    .port(sessionConfiguration.port())
                    .username(sessionConfiguration.user())
                    .password(sessionConfiguration.password())
                    .heartbeatIntervalMs(consumerCfg.getHeartbeatIntervalMs())
                    .autoCommit(true)
                    .autoCommitIntervalMs(ConsumerConstant.AUTO_COMMIT_INTERVAL_MS_DEFAULT_VALUE)
                    .maxPollParallelism(Runtime.getRuntime().availableProcessors() / 2);

            consumerCfg.getConsumerId().ifPresent(consumerBuilder::consumerId);
            consumerCfg.getGroupId().ifPresent(consumerBuilder::consumerGroupId);

            return new StatefulSubscriptionPullConsumer(consumerBuilder);
        }

        /**
         * The <b>StatefulSubscriptionPullConsumer</b> class extends a {@link SubscriptionPullConsumer}.<br>
         * This custom version of a {@link SubscriptionPullConsumer} is a decorator that keeps the reference count
         * of the subscribed topics.
         */
        private static class StatefulSubscriptionPullConsumer extends SubscriptionPullConsumer {
            private static final Logger LOG = LoggerFactory.getLogger(StatefulSubscriptionPullConsumer.class);
            private final AtomicLong subscribedTopicCount = new AtomicLong(0);

            /**
             * Create a {@link StatefulSubscriptionPullConsumer} instance.
             *
             * @param builder to create the consumer
             */
            protected StatefulSubscriptionPullConsumer(Builder builder) {
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
             * Close a {@link SubscriptionPullConsumer} only if its reference count is 0.<br>
             * When the reference count is > 0 this method will throw an {@link IllegalArgumentException}.
             */
            @Override
            public synchronized void close() {
                final var consumerKey = new PullConsumerKey(getConsumerGroupId(), getConsumerId());
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
                final var consumerKey = new PullConsumerKey(getConsumerGroupId(), getConsumerId());
                LOG.debug("Consumer with key {} is subscribed to #{} topics", consumerKey, count);
            }
        }
    }
}
