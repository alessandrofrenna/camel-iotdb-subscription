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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.support.ScheduledPollConsumer;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTreePullConsumer;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumerBuilder;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <b>IoTDbTopicPollConsumer</b> extends the camel {@link ScheduledPollConsumer}. <br> It is used to create a
 * {@link ISubscriptionTablePullConsumer} used to get messages from an IoTDb topics after subscription.<br>
 */
class IoTDbPollConsumer extends ScheduledPollConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(IoTDbPollConsumer.class);
    private static final String TOPIC_HEADER_NAME = "topic";

    private final IoTDbConsumerEndpoint endpoint;
    private final Set<String> topics = new CopyOnWriteArraySet<>();
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
        this.topics.clear();
        this.topics.addAll(topics);
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

        pullConsumer = createPullConsumer(sessionCfg, consumerCfg);
        pullConsumer.open();
        super.doStart();
        LOG.debug("PullConsumer created and opened");
        subscribeToTopics(pullConsumer, topics);
    }

    @Override
    protected void doStop() throws Exception {
        if (pullConsumer == null) {
            super.doStop();
            return;
        }
        LOG.debug("Closing PullConsumer");
        unsubscribeFromTopics(pullConsumer, topics);
        try {
            pullConsumer.close();
            LOG.info("PullConsumer closed");
        } catch (Exception e) {
            LOG.error("PullConsumer close failed: {}", e.getMessage());
        }
        pullConsumer = null;
        super.doStop();
    }

    @Override
    protected int poll() throws Exception {
        final Set<String> topicsSnapshot = Set.copyOf(topics);
        if (isStoppingOrStopped() || topicsSnapshot.isEmpty()) {
            processEmptyMessage();
            return 0;
        }

        subscribeToTopics(pullConsumer, topicsSnapshot);
        List<SubscriptionMessage> messages = pollMessages(topicsSnapshot, endpoint.getPollTimeoutMs());
        Set<SubscriptionMessage> ackableMessages = processMessages(messages);

        if (ackableMessages.isEmpty()) {
            processEmptyMessage();
            return 0;
        }

        pullConsumer.commitSync(ackableMessages);
        LOG.info("PullConsumer acknowledged messages: #{}", ackableMessages.size());
        unsubscribeFromTopics(pullConsumer, topicsSnapshot);
        return ackableMessages.size();
    }

    private void subscribeToTopics(ISubscriptionTreePullConsumer pullConsumer, Set<String> topics) {
        if (topics.isEmpty()) {
            LOG.info("PullConsumer consumer started. No IoTDb topics to subscribe to provided");
            return;
        }

        try {
            LOG.debug("PullConsumer subscribing to: {}", topics);
            pullConsumer.subscribe(topics);
            LOG.info("PullConsumer consumer started. IoTDb topics subscribed to: {}", topics);
        } catch (Exception e) {
            String message = String.format("SubscriptionPullConsumer#subscribe call failed: %s", e.getMessage());
            doFail(new RuntimeCamelException(message, e));
        }
    }

    private void unsubscribeFromTopics(ISubscriptionTreePullConsumer pullConsumer, Set<String> topics) {
        if (topics.isEmpty()) {
            LOG.info("PullConsumer unsubscribe won't be called with empty topic set");
            return;
        }
        try {
            LOG.debug("PullConsumer unsubscribing from: {}", topics);
            pullConsumer.unsubscribe(topics);
            LOG.info("PullConsumer unsubscribed from: {}", topics);
        } catch (SubscriptionException e) {
            LOG.error("PullConsumer unsubscribe failed: {}", e.getMessage());
        }
    }

    private ISubscriptionTreePullConsumer createPullConsumer(
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

    private List<SubscriptionMessage> pollMessages(Set<String> topics, long pollTimeout) {
        LOG.info("PullConsumer polling from IoTDb topics: {}", topics);
        List<SubscriptionMessage> messages = Collections.emptyList();
        try {
            messages = pullConsumer.poll(topics, pollTimeout);
        } catch (SubscriptionException e) {
            LOG.error("PullConsumer poll failed: {}", e.getMessage());
        }
        LOG.info("PullConsumer poll succeeded. Total received messages: #{}", messages.size());
        return messages;
    }

    private Set<SubscriptionMessage> processMessages(List<SubscriptionMessage> messages) {
        Set<SubscriptionMessage> ackableMessages = new CopyOnWriteArraySet<>();
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
        }
    }
}
