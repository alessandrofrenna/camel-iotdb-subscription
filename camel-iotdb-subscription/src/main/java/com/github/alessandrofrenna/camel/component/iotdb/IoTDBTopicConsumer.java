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

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.support.DefaultConsumer;
import org.apache.iotdb.session.subscription.consumer.AckStrategy;
import org.apache.iotdb.session.subscription.consumer.ConsumeListener;
import org.apache.iotdb.session.subscription.consumer.ConsumeResult;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <b>IoTDBTopicConsumer</b> extends the camel {@link DefaultConsumer}. </br> It is used to create a
 * {@link SubscriptionPushConsumer} used to get message from an IoTDB topic after subscription
 */
public class IoTDBTopicConsumer extends DefaultConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(IoTDBTopicConsumer.class);
    private static final String TOPIC_HEADER_KEY = "IoTDBTopic";

    private final IoTDBTopicEndpoint endpoint;
    private SubscriptionPushConsumer pushConsumer;

    public IoTDBTopicConsumer(IoTDBTopicEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
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
        LOG.trace("Push consumer created. Subscribed to {} topic", topic);
    }

    @Override
    protected void doStop() throws Exception {
        if (pushConsumer != null) {
            final String topic = endpoint.getTopic();
            pushConsumer.unsubscribe(topic);
            pushConsumer.close();
            LOG.trace("Unsubscribed from {} topic. Consumer closed", topic);
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
                LOG.debug("Message from {} processed successfully", message.getCommitContext());
                return ConsumeResult.SUCCESS;
            } catch (Exception e) {
                LOG.error("Error processing message received from {}", message.getCommitContext());
                return ConsumeResult.FAILURE;
            }
        };
    }
}
