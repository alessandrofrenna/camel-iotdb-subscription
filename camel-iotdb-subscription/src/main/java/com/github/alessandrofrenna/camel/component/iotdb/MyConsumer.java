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

import static com.github.alessandrofrenna.camel.component.iotdb.IoTDBTopicConsumerManager.PullConsumerKey;

import java.util.ArrayList;
import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.support.DefaultConsumer;
import org.apache.camel.support.ScheduledPollConsumer;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.alessandrofrenna.camel.component.iotdb.event.EventPublisher;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicConsumerSubscribed;

/**
 * The <b>IoTDBTopicConsumer</b> extends the camel {@link DefaultConsumer}. <br> It is used to create a
 * {@link SubscriptionPullConsumer} used to get message from an IoTDB topic after subscription.<br> This class
 * implements the {@link EventPublisher} interface because it is able to publish events: <br>
 *
 * <ul>
 *   <li>
 *       <b>{@link IoTDBTopicConsumerSubscribed}</b>: after {@link IoTDBTopicConsumer#doStart()} invocation, when a new
 *       {@link SubscriptionPullConsumer} is created.
 *   </li>
 * </ul>
 */
public class MyConsumer extends ScheduledPollConsumer implements EventPublisher {
    private final Logger LOG = LoggerFactory.getLogger(MyConsumer.class);

    private final IoTDBTopicEndpoint endpoint;
    private final IoTDBTopicConsumerManager consumerManager;
    private SubscriptionPullConsumer pullConsumer;

    /**
     * Create an <b>IoTDBTopicConsumer</b> instance.
     *
     * @param endpoint source that create the consumer
     * @param processor that will be used by the exchange to process incoming data
     * @param consumerManager dependency that handle consumer operations
     */
    MyConsumer(IoTDBTopicEndpoint endpoint, Processor processor, IoTDBTopicConsumerManager consumerManager) {
        super(endpoint, processor);
        this.endpoint = endpoint;
        this.consumerManager = consumerManager;
    }

    /**
     * Get the push consumer key of the push consumer created by an instance of this class.
     *
     * @return the push consumer key
     */
    public PullConsumerKey getPullConsumerKey() {
        if (pullConsumer == null) {
            throw new IllegalStateException("getPullConsumerKey was called before doStart");
        }
        return new IoTDBTopicConsumerManager.PullConsumerKey(
                pullConsumer.getConsumerGroupId(), pullConsumer.getConsumerId());
    }

    @Override
    protected void doStart() {
        if (isStarted()) {
            return;
        }

        final String topic = endpoint.getTopic();
        pullConsumer = consumerManager.createPullConsumer(endpoint.getConsumerCfg());
        try {
            super.doStart();
            pullConsumer.open();
            pullConsumer.subscribe(topic);
            final String routeId = getRouteId();
            publishEvent(new IoTDBTopicConsumerSubscribed(this, topic, routeId));
            LOG.info(
                    "IoTDBTopicConsumer consumer started and subscribed to event published for '{}' IOTDB topic",
                    topic);
        } catch (Exception e) {
            String message = String.format(
                    "IoTDBTopicConsumer consumer subscription to topic with name '%s' failed: %s",
                    topic, e.getMessage());
            doFail(new RuntimeCamelException(message, e));
        }
    }

    @Override
    protected void doStop() throws Exception {
        final String topic = endpoint.getTopic();
        if (pullConsumer == null) {
            super.doStop();
            return;
        }
        LOG.debug("Stopping IoTDBTopicConsumer for topic: '{}'", topic);
        try {
            LOG.debug("Unsubscribing IoTDB pull consumer for topic: '{}'", endpoint.getTopic());
            pullConsumer.unsubscribe(topic);
            pullConsumer.close();
        } catch (IllegalStateException e) {
            LOG.info("Cannot unsubscribe/close from topic '{}': {}", topic, e.getMessage());
        }
        super.doStop();
    }

    @Override
    protected int poll() {
        final PullConsumerKey consumerKey = getPullConsumerKey();
        final String topic = endpoint.getTopic();
        LOG.info("Polling with {} from IoTDB topic with name: '{}'", consumerKey, topic);

        final long pollTimeout = endpoint.getConsumerCfg().getPollTimeoutMs();
        List<SubscriptionMessage> messages = pullConsumer.poll(pollTimeout);
        List<SubscriptionMessage> filteredMessages = messages.stream()
                .filter(msg -> msg.getCommitContext().getTopicName().equals(topic))
                .toList();

        if (filteredMessages.isEmpty()) {
            return 0; // No messages polled
        }

        List<SubscriptionMessage> ackableMessages = new ArrayList<>();
        int processedCount = 0;
        for (SubscriptionMessage iotdbMessage : filteredMessages) {
            final boolean processed = processMessage(iotdbMessage);
            if (processed) {
                ackableMessages.add(iotdbMessage);
                processedCount++;
            }
        }
        pullConsumer.commitSync(ackableMessages);
        LOG.debug("Acknowledged {} messages to IoTDB", ackableMessages.size());
        LOG.info(
                "{} received {} messages from IoTDB topic with name '{}'. {} processable, {} processed",
                consumerKey,
                messages.size(),
                topic,
                filteredMessages.size(),
                processedCount);
        return processedCount;
    }

    private boolean processMessage(SubscriptionMessage message) {
        final Exchange exchange = endpoint.createExchange();
        final Processor processor = getProcessor();
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

    //    @Override
    //    protected int poll() throws Exception {
    //        // create a new exchange and set a simple body
    //        Exchange exchange = getEndpoint().createExchange();
    //        exchange.getIn().setBody("Polled message at: " + Instant.now());
    //
    //        // process the exchange
    //        getProcessor().process(exchange);
    //
    //        // return 1 to indicate one message was polled
    //        return 1;
    //    }
}
