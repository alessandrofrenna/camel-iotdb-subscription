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

import com.github.alessandrofrenna.camel.component.iotdb.event.EventPublisher;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBResumeAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBStopAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicDropped;
import java.util.Objects;
import java.util.Properties;
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
 * This class implements the {@link EventPublisher} interface because it is able to publish events: </br>
 *
 * <ol>
 *   <li><b>{@link IoTDBStopAllTopicConsumers}</b>: after {@link IoTDBTopicProducer#process(Exchange)} invocation and
 *       action is "drop";
 *   <li><b>{@link IoTDBTopicDropped}</b>: after {@link IoTDBTopicProducer#process(Exchange)} invocation and action is
 *       "drop". Drop operation must be successful;
 * </ol>
 */
public class IoTDBTopicProducer extends DefaultProducer implements EventPublisher {
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
        Objects.requireNonNull(action, "action is null");

        LOG.debug("Processing action '{}' for topic '{}'", action, endpoint.getTopic());
        switch (action.toLowerCase()) {
            case "create" -> createTopic();
            case "drop" -> dropTopic();
            default -> throw new UnsupportedOperationException(String.format("Action %s is not supported", action));
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
            LOG.debug("Attempting to create topic '{}' for path '{}'", topic, path);
            session.createTopicIfNotExists(endpoint.getTopic(), topicProperties);
            LOG.info("Topic {} bound to {} timeseries path", topic, path);
        } catch (IoTDBConnectionException e) {
            throw new RuntimeCamelException("Failed to connect to IoTDB for topic creation: " + e.getMessage(), e);
        } catch (StatementExecutionException e) {
            String errorMessage =
                    String.format("Execution failed during creation/check of topic '%s' for path '%s'", topic, path);
            LOG.error(errorMessage, e);
            throw new RuntimeCamelException(errorMessage, e);
        }
    }

    private void dropTopic() {
        if (producerCfg.getPath().filter(p -> !p.isBlank()).isPresent()) {
            LOG.warn("action=drop invoked with path, it will be ignored");
        }

        String topic = endpoint.getTopic();
        // Step 1: stop all consumers
        LOG.debug("Publishing IoTDBStopAllTopicConsumers for topic '{}'", topic);
        publishEvent(new IoTDBStopAllTopicConsumers(this, topic));
        try {
            LOG.trace("Pausing briefly after commanding consumer stop for topic '{}'...", topic);
            Thread.sleep(IoTDBTopicProducerConfiguration.PRE_DROP_DELAY.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while pausing for consumers to begin stopping for topic '{}'", topic);
        }
        // Step 2: try to drop topic from IoTDB if exists
        try (var session = getSession()) {
            session.open();
            LOG.debug("Attempting to drop IoTDB topic '{}' (if it exists)", topic);
            session.dropTopicIfExists(topic);
            LOG.info("IoTDB topic '{}' successfully dropped (if it existed).", topic);
            // Step 3: delete the previously stopped routes
            LOG.debug("Publishing IoTDBTopicDropped for topic '{}'", topic);
            publishEvent(new IoTDBTopicDropped(this, topic));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            // Fallback: id drop fails restart the previously stopped routes
            LOG.error("Drop of IoTDB topic '{}' failed. IoTDBTopicDropped event will not be sent", topic, e);
            LOG.debug("Publishing IoTDBResumeAllTopicConsumers for topic '{}' to resume all stopped consumers", topic);
            publishEvent(new IoTDBResumeAllTopicConsumers(this, topic));
        }
    }
}
