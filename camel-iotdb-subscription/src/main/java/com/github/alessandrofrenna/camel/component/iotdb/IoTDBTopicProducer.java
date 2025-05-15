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

import static org.apache.iotdb.rpc.subscription.config.TopicConstant.*;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Properties;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.support.DefaultProducer;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <b>IoTDBTopicProducer</b> extends the camel {@link DefaultProducer}. </br> It is used to create and/or drop topic
 * using an IoTDB {@link SubscriptionSession}. </br> On topic drop it fires an {@link IoTDBTopicDropped} event.</br>
 */
public class IoTDBTopicProducer extends DefaultProducer {
    private static final Logger LOG = LoggerFactory.getLogger(IoTDBTopicProducer.class);
    private final IoTDBTopicEndpoint endpoint;
    private final IoTDBTopicProducerConfiguration producerCfg;

    public IoTDBTopicProducer(IoTDBTopicEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
        this.producerCfg = endpoint.getProducerCfg();
    }

    /**
     * Dispatch the operation received in {@link IoTDBTopicProducerConfiguration#getAction()}.</br> If action="create"
     * an IoTDB topic will be created it doesn't exist. If action="drop" an IoTDB topic will be dropped if it does
     * exist.
     *
     * @param exchange the message exchange
     */
    @Override
    public void process(Exchange exchange) {
        final String action = producerCfg.getAction();
        switch (action.toLowerCase()) {
            case "create" -> createTopic();
            case "drop" -> dropTopic();
            default -> throw new UnsupportedOperationException(String.format("%s action is not supported", action));
        }
    }

    private SubscriptionSession getSession() {
        var sessionCfg = endpoint.getSessionCfg();
        return new SubscriptionSession(
                sessionCfg.host(),
                sessionCfg.port(),
                sessionCfg.user(),
                sessionCfg.password(),
                SessionConfig.DEFAULT_MAX_FRAME_SIZE);
    }

    private void createTopic() {
        final String topic = endpoint.getTopic();
        final String path = producerCfg
                .getPath()
                .filter(p -> !p.isBlank())
                .orElseThrow(() -> new IllegalArgumentException("path is required when action=create"));

        Properties topicProperties = new Properties();
        topicProperties.put(PATH_KEY, path);
        topicProperties.put(START_TIME_KEY, NOW_TIME_VALUE);
        topicProperties.put(MODE_KEY, MODE_DEFAULT_VALUE);
        topicProperties.put(FORMAT_KEY, FORMAT_SESSION_DATA_SETS_HANDLER_VALUE);

        try (var session = getSession()) {
            session.open();
            session.createTopicIfNotExists(endpoint.getTopic(), topicProperties);
            LOG.debug("Created topic {} bound to {} timeseries", topic, path);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            LOG.error("Creation of topic {} bound to {} timeseries failed", topic, path);
            throw new RuntimeCamelException(e);
        }
    }

    private void dropTopic() {
        if (producerCfg.getPath().filter(p -> !p.isBlank()).isPresent()) {
            LOG.debug("action=drop invoked with path, it will be ignored");
        }

        String topic = endpoint.getTopic();
        try (var session = getSession()) {
            session.open();
            session.dropTopicIfExists(topic);
            LOG.debug("Topic {} successfully dropped", topic);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            LOG.error("Topic {} drop failed", topic);
            throw new RuntimeCamelException(e);
        }

        publishTopicDropEvent(topic);
    }

    private void publishTopicDropEvent(String deletedTopic) {
        final CamelContext camelContext = endpoint.getCamelContext();
        if (camelContext == null) {
            LOG.warn("CamelContext is not available. Cannot publish IoTDBTopicDropped");
            return;
        }

        final var timestamp = ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();
        final var dropEvent = new IoTDBTopicDropped(this, deletedTopic);
        dropEvent.setTimestamp(timestamp);

        try {
            LOG.debug("Dispatching {}", dropEvent);
            camelContext.getManagementStrategy().notify(dropEvent);
        } catch (Exception e) {
            LOG.error("Failed to publish IoTDBTopicDrop event for topic {}: {}", deletedTopic, e.getMessage(), e);
        }
    }
}
