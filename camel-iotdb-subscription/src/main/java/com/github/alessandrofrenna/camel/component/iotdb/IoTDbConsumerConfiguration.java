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

import java.util.Objects;

/**
 * Configuration record needed to create an instance of a {@link org.apache.iotdb.session.subscription.consumer.ISubscriptionTreePullConsumer}
 *
 * @param consumerGroupId is the group id
 * @param consumerId is the id of the consumer
 * @param pollTimeoutMs is the timeout to use with during poll
 * @param heartbeatIntervalMs to ping IoTDb
 */
public record IoTDbConsumerConfiguration(
        String consumerGroupId, String consumerId, long pollTimeoutMs, long heartbeatIntervalMs) {

    /**
     * Default record constructor. It validates the provided parameters.
     *
     * @param consumerGroupId is the group id
     * @param consumerId is the id of the consumer
     * @param pollTimeoutMs is the timeout to use with during poll
     * @param heartbeatIntervalMs to ping IoTDb
     */
    public IoTDbConsumerConfiguration {
        Objects.requireNonNull(consumerGroupId);
        Objects.requireNonNull(consumerId);
    }
}
