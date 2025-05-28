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

import org.apache.iotdb.session.subscription.consumer.ConsumeListener;

/**
 * <b>TopicAwareConsumeListener</b> allow to bind together the topic name and a {@link ConsumeListener}.<br>
 * It will be used inside {@link IoTDBTopicConsumerManager#createPushConsumer(IoTDBTopicConsumerConfiguration, TopicAwareConsumeListener)}
 * to register a {@link RoutedConsumeListener} for a new or an existing {@link org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer}.
 *
 * @param topicName subject of the subscription
 * @param consumeListener that will process the messages received by a consumer
 */
public record TopicAwareConsumeListener(String topicName, ConsumeListener consumeListener) {
    /**
     * Create an instance of {@link TopicAwareConsumeListener} with a different topic name.<br>
     * It could be useful to share the same {@link ConsumeListener} with multiple topic.
     *
     * @param otherTopicName that will use the consume listener
     * @return a new instance of {@link TopicAwareConsumeListener}
     */
    TopicAwareConsumeListener reuseWithTopic(String otherTopicName) {
        return new TopicAwareConsumeListener(otherTopicName, consumeListener());
    }
}
