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

interface RoutedConsumeListener extends ConsumeListener {
    void routeConsumeListener(String topicId, ConsumeListener consumeListener);
    void removeRouted(String topicId);
    void clear();

    class Default implements RoutedConsumeListener {
        private final Map<String, ConsumeListener> perTopicConsumeListeners = new ConcurrentHashMap<>();

        @Override
        public void routeConsumeListener(String topicId, ConsumeListener consumeListener) {
            perTopicConsumeListeners.putIfAbsent(topicId, consumeListener);
        }

        @Override
        public void removeRouted(String topicId) {
            perTopicConsumeListeners.remove(topicId);
        }

        @Override
        public void clear() {
            perTopicConsumeListeners.clear();
        }

        @Override
        public ConsumeResult onReceive(SubscriptionMessage message) {
            final String topicName = message.getCommitContext().getTopicName();
            final ConsumeListener consumeListener = perTopicConsumeListeners.get(topicName);
            return consumeListener.onReceive(message);
        }
    }

}
