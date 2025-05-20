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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.camel.Exchange;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.support.DefaultProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.alessandrofrenna.camel.component.iotdb.event.EventPublisher;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBResumeAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBStopAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicDropped;

/**
 * The <b>IoTDBTopicProducer</b> extends the camel {@link DefaultProducer}. </br> It is used to create and/or drop topic
 * using a {@link IoTDBTopicManager}. </br> On topic drop it fires an {@link IoTDBTopicDropped} event.</br> This class
 * implements the {@link EventPublisher} interface because it is able to publish events: </br>
 *
 * <ol>
 *   <li><b>{@link IoTDBStopAllTopicConsumers}</b>: after {@link IoTDBTopicProducer#process(Exchange)} invocation and
 *       action is "drop";
 *   <li><b>{@link IoTDBTopicDropped}</b>: after {@link IoTDBTopicProducer#process(Exchange)} invocation and action is
 *       "drop". Drop operation must be successful;
 * </ol>
 */
class IoTDBTopicProducer extends DefaultProducer implements EventPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(IoTDBTopicProducer.class);
    private final IoTDBTopicEndpoint endpoint;
    private final IoTDBTopicManager topicManager;
    private final ScheduledExecutorService scheduler;

    IoTDBTopicProducer(IoTDBTopicEndpoint endpoint, IoTDBTopicManager topicManager) {
        super(endpoint);
        this.endpoint = endpoint;
        this.topicManager = topicManager;

        String schedulerName = "IoTDBTopicProducer-" + endpoint.getTopic().replaceAll("[^a-zA-Z0-9\\-]", "_");
        scheduler = endpoint.getCamelContext()
                .getExecutorServiceManager()
                .newSingleThreadScheduledExecutor(this, schedulerName);
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
        final String action = endpoint.getProducerCfg().getAction();
        Objects.requireNonNull(action, "action is null");

        LOG.debug("Processing action '{}' for topic '{}'", action, endpoint.getTopic());
        switch (action.toLowerCase()) {
            case "create" -> createTopic();
            case "drop" -> dropTopic();
            default -> throw new UnsupportedOperationException(String.format("Action %s is not supported", action));
        }
    }

    @Override
    protected void doStop() throws Exception {
        if (scheduler != null) {
            getEndpoint()
                    .getCamelContext()
                    .getExecutorServiceManager()
                    .shutdownGraceful(scheduler, Duration.ofSeconds(3).toMillis());
        }
        super.doStop();
    }

    private void createTopic() {
        final String topicName = endpoint.getTopic();
        final String path = endpoint.getProducerCfg()
                .getPath()
                .filter(p -> !p.isBlank())
                .orElseThrow(() -> new IllegalArgumentException("path is required when action=create"));
        try {
            topicManager.createTopicIfNotExists(topicName, path);
        } catch (RuntimeException e) {
            throw new RuntimeCamelException(e);
        }
    }

    private void dropTopic() {
        final String topicName = endpoint.getTopic();
        if (endpoint.getProducerCfg().getPath().filter(p -> !p.isBlank()).isPresent()) {
            LOG.warn("action=drop invoked with path, it will be ignored");
        }
        // Step 1: stop all consumers
        LOG.debug("Publishing {} for topic '{}'", IoTDBStopAllTopicConsumers.class.getSimpleName(), topicName);
        publishEvent(new IoTDBStopAllTopicConsumers(this, topicName));

        // Step 2: try to drop topic from IoTDB if exists. Delay execution using the scheduler
        long delay = IoTDBTopicProducerConfiguration.PRE_DROP_DELAY.toMillis();
        scheduler.schedule(
                () -> {
                    try {
                        topicManager.dropTopicIfExists(topicName);
                        // Step 3: delete the previously stopped routes
                        LOG.debug("Publishing {} for topic '{}'", IoTDBTopicDropped.class.getSimpleName(), topicName);
                        publishEvent(new IoTDBTopicDropped(this, topicName));
                    } catch (RuntimeException e) {
                        // Fallback: id drop fails restart the previously stopped routes
                        LOG.debug(
                                "Publishing {} for topic '{}' to resume all stopped consumers",
                                IoTDBResumeAllTopicConsumers.class.getSimpleName(),
                                topicName);
                        publishEvent(new IoTDBResumeAllTopicConsumers(this, topicName));
                    }
                },
                delay,
                TimeUnit.MILLISECONDS);
    }
}
