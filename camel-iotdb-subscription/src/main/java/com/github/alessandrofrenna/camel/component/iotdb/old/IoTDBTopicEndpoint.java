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
package com.github.alessandrofrenna.camel.component.iotdb.old;

import org.apache.camel.Category;
import org.apache.camel.Component;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriPath;
import org.apache.camel.support.DefaultEndpoint;
import org.apache.camel.support.ScheduledPollEndpoint;

/**
 * The <b>IoTDBTopicEndpoint</b> extend the camel {@link DefaultEndpoint}.<br>It is used by camel to create producer
 * and consumers of routes of the "iotdb-subscription" namespace
 */
@UriEndpoint(
        firstVersion = "1.0.0-SNAPSHOT",
        scheme = "iotdb-subscription",
        title = "IoTDBSubscription",
        syntax = "iotdb-subscription:topic",
        category = {Category.IOT})
class IoTDBTopicEndpoint extends ScheduledPollEndpoint {

    @UriPath(description = "The IoTDB topic name")
    @Metadata(
            title = "IoTDB topic name",
            description = "A topic is a channel that emit message when a data point is added to an IoTDB timeseries",
            required = true)
    private String topic;

    @UriParam
    @Metadata(
            title = "IoTDB consumer configuration",
            description = "It contains the route parameters passed on the consumer route declaration")
    private final IoTDBTopicConsumerConfiguration consumerCfg = new IoTDBTopicConsumerConfiguration();

    @UriParam
    @Metadata(
            title = "IoTDB producer configuration",
            description = "It contains the route parameters passed on the producer route declaration")
    private final IoTDBTopicProducerConfiguration producerCfg = new IoTDBTopicProducerConfiguration();

    IoTDBTopicEndpoint(String uri, Component component) {
        super(uri, component);
    }

    /**
     * Create a {@link IoTDBTopicProducer}. It will be used to create IoTDB topics with apache camel.
     *
     * @return an instance of {@link IoTDBTopicProducer}
     */
    @Override
    public Producer createProducer() {
        return new IoTDBTopicProducer(this, ((IoTDBSubscriptionComponent) getComponent()).getTopicManager());
    }

    /**
     * Create a {@link IoTDBTopicConsumer}. It will be used to create IoTDB topic subscribers that listens for new
     * messages.
     *
     * @param processor the given processor
     * @return an instance of {@link IoTDBTopicConsumer}
     * @throws Exception if the creation of the consumer fails
     */
    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        final var iotdbComponent = ((IoTDBSubscriptionComponent) getComponent());
        final var consumer = new IoTDBTopicConsumer(this, processor, iotdbComponent.getConsumerManager());
        configureConsumer(consumer);
        return consumer;
    }

    /**
     * Each Camel 'from' using this component should result in a distinct IoTDB consumer instance, especially when
     * consumerId/groupId are auto-generated or when users define multiple routes that might otherwise normalize to the
     * same URI but expect independent behavior.
     *
     * @return false by default
     */
    @Override
    public boolean isSingleton() {
        return false;
    }

    /**
     * Get the topic name provided in the route path.
     *
     * @return a string containing the IoTDB topic name
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Set the topic name provided in the route path.
     *
     * @param topic subject of the subscription
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    protected IoTDBTopicConsumerConfiguration getConsumerCfg() {
        return consumerCfg;
    }

    protected IoTDBTopicProducerConfiguration getProducerCfg() {
        return producerCfg;
    }
}
