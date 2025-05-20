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

import static com.github.alessandrofrenna.camel.component.iotdb.IoTDBSessionConfiguration.*;

import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.spi.EventNotifier;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.annotations.Component;
import org.apache.camel.support.HealthCheckComponent;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the <b>IoTDBSubscriptionComponent</b> extends the HealthCheckComponent. The component allows the integration
 * between IoTDB data subscription API and apache camel
 */
@Component("iotdb-subscription")
public class IoTDBSubscriptionComponent extends HealthCheckComponent {
    private static final Logger LOG = LoggerFactory.getLogger(IoTDBSubscriptionComponent.class);

    @Metadata(title = "IoTDB host", required = true, defaultValue = DEFAULT_HOST)
    private String host;

    @Metadata(title = "IoTDB port", required = true, defaultValue = DEFAULT_PORT, javaType = "Integer")
    private Integer port;

    @Metadata(title = "IoTDB username", required = true, secret = true, defaultValue = DEFAULT_USERNAME)
    private String user;

    @Metadata(title = "IoTDB password", required = true, secret = true, defaultValue = DEFAULT_PASSWORD)
    private String password;

    private IoTDBTopicManager topicManager;
    private IoTDBTopicConsumerManager consumerManager;
    private IoTDBRoutesRegistry routesRegistry;
    private IoTDBSubscriptionEventListener eventListener;

    private static void configureEventNotifier(EventNotifier eventNotifier) {
        eventNotifier.setIgnoreCamelContextEvents(true);
        eventNotifier.setIgnoreCamelContextInitEvents(true);
        eventNotifier.setIgnoreExchangeEvents(true);
        eventNotifier.setIgnoreExchangeCreatedEvent(true);
        eventNotifier.setIgnoreExchangeCompletedEvent(true);
        eventNotifier.setIgnoreExchangeFailedEvents(true);
        eventNotifier.setIgnoreExchangeRedeliveryEvents(true);
        eventNotifier.setIgnoreExchangeAsyncProcessingStartedEvents(true);
        eventNotifier.setIgnoreExchangeSendingEvents(true);
        eventNotifier.setIgnoreExchangeSentEvents(true);
        eventNotifier.setIgnoreServiceEvents(true);
        eventNotifier.setIgnoreStepEvents(true);
        eventNotifier.setIgnoreRouteEvents(false);
    }

    public IoTDBSubscriptionComponent() {}

    /**
     * Get the IoTDB host name
     *
     * @return host
     */
    public String getHost() {
        return host;
    }

    /**
     * Set the IoTDB host name
     *
     * @param host name
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Get the IoTDB server port
     *
     * @return the server port
     */
    public int getPort() {
        return port;
    }

    /**
     * Set the IoTDB server port
     *
     * @param port number
     */
    public void setPort(Integer port) {
        this.port = port;
    }

    /**
     * Get the IoTDB user name
     *
     * @return the username
     */
    public String getUser() {
        return user;
    }

    /**
     * Set the IoTDB user name
     *
     * @param user name
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Get the IoTDB user password
     *
     * @return the password associated with the value returned by {@link #getUser()}
     */
    public String getPassword() {
        return password;
    }

    /**
     * The IoTDB user password
     *
     * @param password associated with the value returned by {@link #getUser()}
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Get the topic manager that handles topic creation and drop.
     *
     * @return the topic manager instance.
     */
    public IoTDBTopicManager getTopicManager() {
        return topicManager;
    }

    /**
     * Get the consumer manager that handles consumers.
     *
     * @return the consumer manager instance.
     */
    public IoTDBTopicConsumerManager getConsumerManager() {
        return consumerManager;
    }

    /**
     * Get the routes registry that handles the routes registered by the component for each topic.
     *
     * @return the routes registry instance.
     */
    public IoTDBRoutesRegistry getRoutesRegistry() {
        return routesRegistry;
    }

    protected Endpoint createEndpoint(String uri, String topic, Map<String, Object> parameters) throws Exception {
        IoTDBTopicEndpoint endpoint = new IoTDBTopicEndpoint(uri, this);
        setProperties(endpoint, parameters);
        endpoint.setTopic(topic);

        LOG.debug("A new IoTDBTopicEndpoint has been created");
        return endpoint;
    }

    @Override
    protected void doInit() throws Exception {
        super.doInit();
        final var sessionConfiguration = new IoTDBSessionConfiguration(getHost(), getPort(), getUser(), getPassword());
        consumerManager = new IoTDBTopicConsumerManager.Default(sessionConfiguration);
        routesRegistry = new IoTDBRoutesRegistry.Default(consumerManager);
        eventListener = new IoTDBSubscriptionEventListener(routesRegistry);
        configureEventNotifier(eventListener);

        topicManager = new IoTDBTopicManager.Default(() -> new SubscriptionSession(
                sessionConfiguration.host(),
                sessionConfiguration.port(),
                sessionConfiguration.user(),
                sessionConfiguration.password(),
                SessionConfig.DEFAULT_MAX_FRAME_SIZE));

        final CamelContext camelContext = getCamelContext();
        if (camelContext == null) {
            LOG.warn("CamelContext is null. Cannot register IoTDBSubscriptionEventListener");
            return;
        }
        routesRegistry.setCamelContext(camelContext);
        if (camelContext.getManagementStrategy().getEventNotifiers().contains(eventListener)) {
            LOG.warn("IoTDBTopicEventListener already registered in CamelContext [{}].", camelContext.getName());
        } else {
            camelContext.getManagementStrategy().addEventNotifier(eventListener);
            LOG.debug("Registered IoTDBTopicEventListener with CamelContext [{}].", camelContext.getName());
        }
    }

    @Override
    protected void doStop() throws Exception {
        final CamelContext camelContext = getCamelContext();
        if (camelContext == null || eventListener == null) {
            return;
        }

        routesRegistry.close();
        consumerManager.close();

        final var removed = camelContext.getManagementStrategy().removeEventNotifier(eventListener);
        if (removed) {
            LOG.debug("Unregistered IoTDBTopicEventListener from CamelContext [{}].", camelContext.getName());
        }
        // EventNotifierSupport is a Service, Camel should stop it.
        // Explicitly stop if necessary, though usually managed by Camel.
        if (eventListener.isStarted()) {
            eventListener.stop();
        }

        routesRegistry = null;
        consumerManager = null;
        topicManager = null;

        super.doStop();
    }
}
