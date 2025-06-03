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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.camel.Category;
import org.apache.camel.Component;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriPath;
import org.apache.camel.support.ScheduledPollEndpoint;

/**
 * The <b>IoTDbConsumerEndpoint</b> extend the camel {@link ScheduledPollEndpoint}.<br>
 * It is used by camel to create the poll consumers of routes for {@link IoTDbSubscriptionComponent}
 */
@UriEndpoint(
        firstVersion = "0.0.1",
        scheme = "iotdb-subscription",
        title = "IoTDbTopicSubscribeEndpoint",
        syntax = "iotdb-subscription:consumerGroupId:consumerId",
        category = {Category.IOT})
public class IoTDbConsumerEndpoint extends ScheduledPollEndpoint {

    @UriPath(name = "consumerGroupId", description = "The consumer group id that contains the topic consumer")
    private String consumerGroupId;

    @UriPath(name = "consumerId", description = "The id of the topic consumer that belongs to a consumer group")
    private String consumerId;

    @UriParam(name = "subscribeTo", description = "Comma-separated list of topics, the source of the polling operation")
    private String subscribeTo;

    @UriParam(
            name = "heartbeatIntervalMs",
            description =
                    "The consumer will ping IoTDB to check health after this interval of time in millisecond passes",
            defaultValue = "30000")
    private Long heartbeatIntervalMs = 30_000L;

    @UriParam(
            name = "pollTimeoutMs",
            description = "The poll consumer will use this timeout on polling",
            defaultValue = "10000")
    private Long pollTimeoutMs = 10_000L;

    private IoTDbPollConsumer internalSharedConsumer;
    private final Lock internalSharedConsumerLock = new ReentrantLock();

    /**
     * Create an {@link IoTDbConsumerEndpoint} instance.
     *
     * @param endpointUri created by camel
     * @param component instance
     */
    public IoTDbConsumerEndpoint(String endpointUri, Component component) {
        super(endpointUri, component);
    }

    /**
     * Get the consumer group id that contains the topic consumer.
     *
     * @return the consumer group id
     */
    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    /**
     * Set the consumer group id that contains the topic consumer.
     *
     * @param consumerGroupId the consumer group id
     */
    public void setConsumerGroupId(String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
    }

    /**
     * Get the id of the topic consumer that belongs to a consumer group.
     *
     * @return the consumer id
     */
    public String getConsumerId() {
        return consumerId;
    }

    /**
     * Set the id of the topic consumer that belongs to a consumer group.
     *
     * @param consumerId of the topic
     */
    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    /**
     * Get a comma-separated list of topics, the source of the polling operation.
     *
     * @return a comma-separated list of strings
     */
    public String getSubscribeTo() {
        return subscribeTo;
    }

    /**
     * Set a comma-separated list of topics, the source of the polling operation.
     *
     * @param subscribeTo a list of topic
     */
    public void setSubscribeTo(String subscribeTo) {
        this.subscribeTo = subscribeTo;
        if (internalSharedConsumer != null) {
            internalSharedConsumer.setTopics(new HashSet<>(Arrays.asList(subscribeTo.split(","))));
        }
    }

    /**
     * Get the heartbeat interval used by the consumer to perform healthcheck.<br> This value is optional. If not
     * provided this method returns the default value of 30_000ms (30s).
     *
     * @return the heartbeat interval in milliseconds
     */
    public Long getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    /**
     * Set the heartbeat interval from the route uri parameters.
     *
     * @param heartbeatIntervalMs milliseconds interval used by the consumer to perform healthcheck against IoTDb
     */
    public void setHeartbeatIntervalMs(Long heartbeatIntervalMs) {
        if (heartbeatIntervalMs <= 0) {
            throw new IllegalArgumentException("heartbeatIntervalMs must be a positive, non zero, value");
        }
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    /**
     * Get the topic poll timeout.<br> This value is optional. If not
     * provided this method returns the default value of 10_000ms (10s).
     *
     * @return the time in milliseconds
     */
    public Long getPollTimeoutMs() {
        return pollTimeoutMs;
    }

    /**
     * Set the topic poll timeout.
     *
     * @param pollTimeoutMs time in milliseconds
     */
    public void setPollTimeoutMs(Long pollTimeoutMs) {
        if (pollTimeoutMs <= 0) {
            throw new IllegalArgumentException("pollTimeoutMs must be a positive, non zero, value");
        }
        this.pollTimeoutMs = pollTimeoutMs;
    }

    /**
     * This operation is not supported. No producer will be created for this endpoint.
     *
     * @return UnsupportedOperationException
     */
    @Override
    public Producer createProducer() {
        throw new UnsupportedOperationException("");
    }

    /**
     * Create a poll consumer.
     *
     * @param processor the given processor
     * @return the created consumer
     */
    @Override
    public Consumer createConsumer(Processor processor) {
        internalSharedConsumerLock.lock();
        try {
            if (internalSharedConsumer == null) {
                internalSharedConsumer = new IoTDbPollConsumer(this, processor);
                internalSharedConsumer.setTopics(extractAndGetTopics());
                configureConsumer(internalSharedConsumer);
            }
            return new IoTDbPollConsumerDelegate(internalSharedConsumer);
        } catch (Exception e) {
            throw new RuntimeCamelException(e);
        } finally {
            internalSharedConsumerLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws IOException per contract of {@link AutoCloseable} if {@link org.apache.camel.Service#stop()} fails
     */
    @Override
    public void close() throws IOException {
        super.close();
    }

    @Override
    protected void doStop() {
        internalSharedConsumerLock.lock();
        try {
            internalSharedConsumer.stop();
            if (internalSharedConsumer.isStoppingOrStopped()) {
                internalSharedConsumer = null;
            }
        } finally {
            internalSharedConsumerLock.unlock();
        }
    }

    private Set<String> extractAndGetTopics() {
        return new HashSet<>(Arrays.asList(subscribeTo.split(",")));
    }
}
