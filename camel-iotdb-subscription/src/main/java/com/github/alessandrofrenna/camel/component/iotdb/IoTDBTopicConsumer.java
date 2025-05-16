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

import com.github.alessandrofrenna.camel.component.iotdb.event.EventPublisher;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicConsumerSubscribed;
import com.github.alessandrofrenna.camel.component.iotdb.event.IotDBTopicConsumerUnsubscribed;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Route;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.support.DefaultConsumer;
import org.apache.iotdb.session.subscription.consumer.AckStrategy;
import org.apache.iotdb.session.subscription.consumer.ConsumeListener;
import org.apache.iotdb.session.subscription.consumer.ConsumeResult;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <b>IoTDBTopicConsumer</b> extends the camel {@link DefaultConsumer}. </br> It is used to create a
 * {@link SubscriptionPushConsumer} used to get message from an IoTDB topic after subscription.</br> This class
 * implements the {@link EventPublisher} interface because it is able to publish events: </br>
 *
 * <ol>
 *   <li><b>{@link IoTDBTopicConsumerSubscribed}</b>: after {@link IoTDBTopicConsumer#doStart()} invocation, when a new
 *       {@link SubscriptionPushConsumer} is created;
 *   <li><b>{@link IotDBTopicConsumerUnsubscribed}</b>: after {@link IoTDBTopicConsumer#doStop()} invocation, when the
 *       {@link SubscriptionPushConsumer} is closed;
 * </ol>
 */
public class IoTDBTopicConsumer extends DefaultConsumer implements EventPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(IoTDBTopicConsumer.class);
    private static final String TOPIC_HEADER_KEY = "IoTDBTopic";

    private final IoTDBTopicEndpoint endpoint;
    private SubscriptionPushConsumer pushConsumer;

    public IoTDBTopicConsumer(IoTDBTopicEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
    }

    /**
     * Get the id of the route associated to the endpoint
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

    @Override
    protected void doStart() throws Exception {
        super.doStart();

        final String topic = endpoint.getTopic();
        var consumerCfg = endpoint.getConsumerCfg();
        var sessionCfg = endpoint.getSessionCfg();

        var consumerBuilder = new SubscriptionPushConsumer.Builder()
                .host(sessionCfg.host())
                .port(sessionCfg.port())
                .username(sessionCfg.user())
                .password(sessionCfg.password())
                .heartbeatIntervalMs(consumerCfg.getHeartbeatIntervalMs())
                .endpointsSyncIntervalMs(consumerCfg.getSyncIntervalMs())
                .ackStrategy(AckStrategy.AFTER_CONSUME)
                .consumeListener(this.getConsumeListener());

        consumerCfg.getConsumerId().ifPresent(consumerBuilder::consumerId);
        consumerCfg.getGroupId().ifPresent(consumerBuilder::consumerGroupId);

        pushConsumer = consumerBuilder.buildPushConsumer();
        pushConsumer.open();
        pushConsumer.subscribe(topic);
        publishEvent(new IoTDBTopicConsumerSubscribed(this, topic, getRouteId()));
        LOG.info("IoTDBTopicConsumer consumer started and subscribed to event published for '{}' IOTDB topic", topic);
    }

    @Override
    protected void doStop() throws Exception {

        if (pushConsumer != null) {
            final String topic = endpoint.getTopic();
            LOG.debug("Stopping IoTDBTopicConsumer for topic: '{}'", topic);
            try {
                LOG.debug("Unsubscribing IoTDB push consumer for topic: '{}'", endpoint.getTopic());
                pushConsumer.unsubscribe(topic);
                pushConsumer.close();
                pushConsumer = null;
                publishEvent(new IotDBTopicConsumerUnsubscribed(this, topic, getRouteId()));
                LOG.info("Unsubscribed and closed IoTDB push consumer for topic: '{}'", topic);
            } catch (Exception e) {
                LOG.error("Error during IoTDB unsubscribe/close for topic '{}'", topic, e);
            }
        }
        super.doStop();
    }

    private ConsumeListener getConsumeListener() {
        return message -> {
            LOG.debug("Received message from: {}", message.getCommitContext());

            final Exchange exchange = endpoint.createExchange();
            exchange.getIn()
                    .setHeader(TOPIC_HEADER_KEY, message.getCommitContext().getTopicName());
            exchange.getIn().setBody(message);

            try {
                getProcessor().process(exchange);
                LOG.trace("Message from {} processed successfully", message.getCommitContext());
                return ConsumeResult.SUCCESS;
            } catch (Exception e) {
                LOG.error("Error processing message received from {}", message.getCommitContext());
                return ConsumeResult.FAILURE;
            }
        };
    }
}
