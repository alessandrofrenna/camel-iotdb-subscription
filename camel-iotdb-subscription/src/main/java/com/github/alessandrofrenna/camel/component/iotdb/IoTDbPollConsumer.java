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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.support.ScheduledPollConsumer;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTreePullConsumer;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumerBuilder;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <b>IoTDbTopicPollConsumer</b> extends the camel {@link ScheduledPollConsumer}. <br> It is used to create a
 * {@link ISubscriptionTreePullConsumer} used to get messages from an IoTDb topics after subscription.<br>
 */
class IoTDbPollConsumer extends ScheduledPollConsumer {
    private static final String TOPIC_HEADER_NAME = "topic";
    private final Logger LOG = LoggerFactory.getLogger(IoTDbPollConsumer.class);
    private final IoTDbConsumerEndpoint endpoint;
    private final Set<String> topics = new CopyOnWriteArraySet<>();
    private final ReentrantLock defaultLock = new ReentrantLock();
    private ISubscriptionTreePullConsumer pullConsumer;

    /**
     * Create an <b>IoTDbTopicPollConsumer</b> instance.
     *
     * @param endpoint source that create the consumer
     */
    IoTDbPollConsumer(IoTDbConsumerEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
    }

    /**
     * Return a copy of the topics {@link ISubscriptionTreePullConsumer} is subscribed to.
     *
     * @return a set of topics
     */
    public Set<String> getTopics() {
        return Set.copyOf(topics);
    }

    /**
     * Set the topics subject of the subscription.
     *
     * @param topics to subscribe to
     */
    public void setTopics(Set<String> topics) {
        defaultLock.lock();
        try {
            if (isStarted() && pullConsumer != null) {
                // call unsubscribe with the value of this.topics before its update
                // this will remove the subscribed topics that are not inside the
                // updated version of topics provided to this method
                pullConsumer.unsubscribe(this.topics);
                // subscribe to the updated topics
                pullConsumer.subscribe(topics);
            }
        } finally {
            defaultLock.unlock();
        }

        this.topics.clear();
        this.topics.addAll(topics);
    }

    protected ISubscriptionTreePullConsumer createPullConsumer(
            IoTDbSessionConfiguration sessionCfg, IoTDbConsumerConfiguration consumerCfg) {
        return new SubscriptionTreePullConsumerBuilder()
                .host(sessionCfg.host())
                .port(sessionCfg.port())
                .username(sessionCfg.user())
                .password(sessionCfg.password())
                .consumerGroupId(consumerCfg.consumerGroupId())
                .consumerId(consumerCfg.consumerId())
                .heartbeatIntervalMs(consumerCfg.heartbeatIntervalMs())
                .autoCommit(false)
                .maxPollParallelism(Runtime.getRuntime().availableProcessors() / 2)
                .buildPullConsumer();
    }

    @Override
    protected void doStart() throws Exception {
        var component = endpoint.getComponent(IoTDbSubscriptionComponent.class);
        var sessionCfg = component.getSessionConfig();
        var consumerCfg = new IoTDbConsumerConfiguration(
                endpoint.getConsumerGroupId(),
                endpoint.getConsumerId(),
                endpoint.getPollTimeoutMs(),
                endpoint.getHeartbeatIntervalMs());

        super.doStart();
        pullConsumer = createPullConsumer(sessionCfg, consumerCfg);
        pullConsumer.open();
        LOG.debug("SubscriptionPullConsumer created and opened");

        if (topics.isEmpty()) {
            LOG.info("SubscriptionPullConsumer consumer started. No IoTDb topics to subscribe to provided");
            return;
        }

        try {
            LOG.debug("SubscriptionPullConsumer subscribing to: {}", topics);
            pullConsumer.subscribe(topics);
            LOG.info("SubscriptionPullConsumer consumer started. IoTDb topics subscribed to: {}", topics);
        } catch (Exception e) {
            String message = String.format("SubscriptionPullConsumer#subscribe call failed: %s", e.getMessage());
            doFail(new RuntimeCamelException(message, e));
        }
    }

    @Override
    protected void doStop() throws Exception {
        if (pullConsumer == null) {
            super.doStop();
            return;
        }
        LOG.debug("Stopping SubscriptionPullConsumer");
        try {
            LOG.debug("SubscriptionPullConsumer unsubscribing from: {}", topics);
            pullConsumer.unsubscribe(topics);
            pullConsumer.close();
            LOG.info("SubscriptionPullConsumer unsubscribed from: {}", topics);
            LOG.info("SubscriptionPullConsumer closed");
        } catch (SubscriptionException e) {
            LOG.error("SubscriptionPullConsumer#unsubscribe failed: {}", e.getMessage());
        }
        pullConsumer = null;
        super.doStop();
    }

    @Override
    protected int poll() {
        if (topics.isEmpty()) {
            return 0;
        }

        LOG.info("SubscriptionPullConsumer polling from IoTDb topics: {}", topics);
        List<SubscriptionMessage> messages = pollMessages(endpoint.getPollTimeoutMs());
        List<SubscriptionMessage> ackableMessages = processMessages(messages);
        if (!ackableMessages.isEmpty()) {
            pullConsumer.commitSync(ackableMessages);
            LOG.info("SubscriptionPullConsumer acknowledged messages: #{}", ackableMessages.size());
            return ackableMessages.size();
        }
        return 0;
    }

    private List<SubscriptionMessage> pollMessages(long pollTimeout) {
        List<SubscriptionMessage> messages = Collections.emptyList();
        defaultLock.lock();
        try {
            messages = pullConsumer.poll(pollTimeout);
        } catch (SubscriptionException e) {
            LOG.error("SubscriptionPullConsumer#poll failed: {}", e.getMessage());
        } finally {
            defaultLock.unlock();
        }
        LOG.info("SubscriptionPullConsumer#poll total received messages: #{}", messages.size());
        return messages;
    }

    private List<SubscriptionMessage> processMessages(List<SubscriptionMessage> messages) {
        List<SubscriptionMessage> ackableMessages = new ArrayList<>();
        for (SubscriptionMessage message : messages) {
            if (processMessage(message)) {
                ackableMessages.add(message);
            }
        }
        return ackableMessages;
    }

    private boolean processMessage(SubscriptionMessage message) {
        final Exchange exchange = endpoint.createExchange();
        final Processor processor = getProcessor();
        exchange.getIn().setHeader(TOPIC_HEADER_NAME, message.getCommitContext().getTopicName());
        exchange.getIn().setBody(message);
        try {
            getProcessor().process(exchange);
            if (exchange.getException() != null) {
                LOG.error("Error processing the message", exchange.getException());
                getExceptionHandler()
                        .handleException("Error processing IoTDB message in route", exchange, exchange.getException());
                return false;
            }
            return true;
        } catch (Exception e) {
            LOG.error("Unexpected error processing the message", e);
            getExceptionHandler().handleException("Unexpected error processing IoTDB message", exchange, e);
            return false;
        } finally {
            releaseExchange(exchange, false);
        }
    }
}
