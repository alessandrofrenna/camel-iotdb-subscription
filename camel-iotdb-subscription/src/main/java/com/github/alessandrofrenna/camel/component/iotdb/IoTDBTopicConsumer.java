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

import static com.github.alessandrofrenna.camel.component.iotdb.IoTDBTopicConsumerManager.PushConsumerKey;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Route;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.support.DefaultConsumer;
import org.apache.iotdb.session.subscription.consumer.ConsumeListener;
import org.apache.iotdb.session.subscription.consumer.ConsumeResult;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.alessandrofrenna.camel.component.iotdb.event.EventPublisher;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicConsumerSubscribed;

/**
 * The <b>IoTDBTopicConsumer</b> extends the camel {@link DefaultConsumer}. </br> It is used to create a
 * {@link SubscriptionPushConsumer} used to get message from an IoTDB topic after subscription.</br> This class
 * implements the {@link EventPublisher} interface because it is able to publish events: </br>
 *
 * <ul>
 *   <li>
 *       <b>{@link IoTDBTopicConsumerSubscribed}</b>: after {@link IoTDBTopicConsumer#doStart()} invocation, when a new
 *       {@link SubscriptionPushConsumer} is created.
 *   </li>
 * </ul>
 */
class IoTDBTopicConsumer extends DefaultConsumer implements EventPublisher {
    private final Logger LOG = LoggerFactory.getLogger(IoTDBTopicConsumer.class);

    private final IoTDBTopicEndpoint endpoint;
    private final IoTDBTopicConsumerManager consumerManager;
    private SubscriptionPushConsumer pushConsumer;

    /**
     * Create an <b>IoTDBTopicConsumer</b> instance.
     *
     * @param endpoint source that create the consumer
     * @param processor that will be used by the exchange to process incoming data
     * @param consumerManager dependency that handle consumer operations
     */
    IoTDBTopicConsumer(IoTDBTopicEndpoint endpoint, Processor processor, IoTDBTopicConsumerManager consumerManager) {
        super(endpoint, processor);
        this.endpoint = endpoint;
        this.consumerManager = consumerManager;
    }

    /**
     * Get the id of the route associated to the endpoint.
     *
     * @return the camel route id
     * @throws RuntimeCamelException if the route is null
     */
    public String getRouteId() {
        Route route = getRoute();
        if (route == null) {
            throw new RuntimeCamelException("route is null, it should not be null");
        }
        return route.getRouteId();
    }

    /**
     * Get the push consumer key of the push consumer created by an instance of this class.
     *
     * @return the push consumer key
     */
    public PushConsumerKey getPushConsumerKey() {
        return new PushConsumerKey(pushConsumer.getConsumerGroupId(), pushConsumer.getConsumerId());
    }

    @Override
    protected void doStart() {
        final String topic = endpoint.getTopic();
        var consumeListener = new TopicAwareConsumeListener(endpoint.getTopic(), this.defaultConsumeListener());
        pushConsumer = consumerManager.createPushConsumer(endpoint.getConsumerCfg(), consumeListener);

        try {
            super.doStart();
            pushConsumer.open();
            pushConsumer.subscribe(topic);
            publishEvent(new IoTDBTopicConsumerSubscribed(this, topic, getRouteId()));
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
        if (pushConsumer == null) {
            super.doStop();
            return;
        }
        LOG.debug("Stopping IoTDBTopicConsumer for topic: '{}'", topic);
        try {
            LOG.debug("Unsubscribing IoTDB push consumer for topic: '{}'", endpoint.getTopic());
            pushConsumer.unsubscribe(topic);
            pushConsumer.close();
        } catch (IllegalStateException e) {
            LOG.info("Cannot unsubscribe/close from topic '{}': {}", topic, e.getMessage());
        }
        super.doStop();
    }

    private ConsumeListener defaultConsumeListener() {
        return message -> {
            final Exchange exchange = endpoint.createExchange();
            final Processor processor = getProcessor();
            exchange.getIn().setBody(message);
            try {
                getProcessor().process(exchange);
                if (exchange.getException() != null) {
                    LOG.error("Error processing the message", exchange.getException());
                    getExceptionHandler()
                            .handleException(
                                    "Error processing IoTDB message in route", exchange, exchange.getException());
                    return ConsumeResult.FAILURE;
                }
            } catch (Exception e) {
                LOG.error("Unexpected error processing the message", e);
                getExceptionHandler().handleException("Unexpected error processing IoTDB message", exchange, e);
                return ConsumeResult.FAILURE;
            }
            return ConsumeResult.SUCCESS;
        };
    }
}
