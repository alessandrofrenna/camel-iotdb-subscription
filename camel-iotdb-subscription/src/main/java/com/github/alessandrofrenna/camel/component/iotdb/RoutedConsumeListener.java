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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.iotdb.session.subscription.consumer.ConsumeListener;
import org.apache.iotdb.session.subscription.consumer.ConsumeResult;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;

/**
 * The <b>RoutedConsumeListener</b> interface extends the {@link ConsumeListener} interface functionalities.</br>
 * Since an instance of {@link org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer} is shared between multiple topic,
 * there is a need for a mechanism that routes the incoming messages to the correct {@link IoTDBTopicConsumer}.</br>
 * An instance of this interface is used inside {@link IoTDBTopicConsumerManager#createPushConsumer(IoTDBTopicConsumerConfiguration, TopicAwareConsumeListener)}.
 */
interface RoutedConsumeListener extends ConsumeListener {

    /**
     * Register a new routed {@link ConsumeListener}.
     *
     * @param topicId as route path
     * @param consumeListener to invoke
     */
    void routeConsumeListener(String topicId, ConsumeListener consumeListener);

    /**
     * Remove an existing routed {@link ConsumeListener}.
     *
     * @param topicId as route path
     */
    void removeRouted(String topicId);

    /**
     * Remove all existing routed {@link ConsumeListener}s.
     */
    void clear();

    /**
     * This is the default and only provided implementation of the {@link RoutedConsumeListener} interface
     */
    class Default implements RoutedConsumeListener {
        private final Map<String, ConsumeListener> perTopicConsumeListeners = new ConcurrentHashMap<>();

        /**
         * {@inheritDoc}
         *
         * @param topicId as route path
         * @param consumeListener to invoke
         */
        @Override
        public void routeConsumeListener(String topicId, ConsumeListener consumeListener) {
            perTopicConsumeListeners.putIfAbsent(topicId, consumeListener);
        }

        /**
         * {@inheritDoc}
         *
         * @param topicId as route path
         */
        @Override
        public void removeRouted(String topicId) {
            perTopicConsumeListeners.remove(topicId);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void clear() {
            perTopicConsumeListeners.clear();
        }

        /**
         * This implementation of onReceive routes the messages to the appropriate {@link ConsumeListener}.</br>
         * It extracts the topic name from the message context obtained calling {@link SubscriptionMessage#getCommitContext()}.
         *
         * @param message incoming message
         * @return the result
         */
        @Override
        public ConsumeResult onReceive(SubscriptionMessage message) {
            final String topicName = message.getCommitContext().getTopicName();
            final ConsumeListener consumeListener = perTopicConsumeListeners.get(topicName);
            return consumeListener.onReceive(message);
        }
    }
}
