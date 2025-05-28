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

import org.apache.camel.Endpoint;
import org.apache.camel.Route;
import org.apache.camel.spi.CamelEvent;
import org.apache.camel.support.EventNotifierSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBResumeAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBStopAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicConsumerSubscribed;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicDropped;

/**
 * The <b>IoTDBSubscriptionEventListener</b> extends {@link EventNotifierSupport}.<br> It handles events coming from
 * {@link IoTDBTopicProducer} and {@link IoTDBTopicConsumer} instances.
 */
class IoTDBSubscriptionEventListener extends EventNotifierSupport {
    private static final Logger LOG = LoggerFactory.getLogger(IoTDBSubscriptionEventListener.class);

    private static void LOG_EVENT(CamelEvent event) {
        LOG.debug("Received event: {}", event);
    }

    private final IoTDBRoutesRegistry routesRegistry;

    IoTDBSubscriptionEventListener(IoTDBRoutesRegistry routesRegistry) {
        this.routesRegistry = routesRegistry;
    }

    /**
     * Handle events coming from {@link IoTDBTopicProducer} and {@link IoTDBTopicConsumer} instances. The supported
     * events are:
     *
     * <ul>
     *   <li>{@link IoTDBTopicConsumerSubscribed}</li>
     *   <li>{@link IoTDBStopAllTopicConsumers}</li>
     *   <li>{@link IoTDBTopicDropped}</li>
     *   <li>{@link IoTDBResumeAllTopicConsumers}</li>
     *   <li>{@link CamelEvent.RouteRemovedEvent}</li>
     * </ul>
     *
     * @param event to handle
     */
    @Override
    public void notify(CamelEvent event) {
        LOG_EVENT(event);
        if (event instanceof IoTDBTopicConsumerSubscribed subscribedEvent) {
            routesRegistry.registerRouteAfterConsumerSubscription(subscribedEvent);
        } else if (event instanceof IoTDBStopAllTopicConsumers stopAllTopicConsumers) {
            routesRegistry.stopAllConsumedTopicRoutes(stopAllTopicConsumers);
        } else if (event instanceof IoTDBTopicDropped dropEvent) {
            routesRegistry.removeRoutesAfterTopicDrop(dropEvent);
        } else if (event instanceof IoTDBResumeAllTopicConsumers resumeEvent) {
            routesRegistry.resumeAllStoppedConsumedTopicRoutes(resumeEvent);
        } else if (event instanceof CamelEvent.RouteRemovedEvent routeRemovedEvent) {
            final Route route = routeRemovedEvent.getRoute();
            final Endpoint endpoint = route.getEndpoint();
            if (endpoint instanceof IoTDBTopicEndpoint topicEndpoint) {
                routesRegistry.removeTopicMappedRoutes(topicEndpoint.getTopic(), route.getRouteId());
            }
        }
    }

    /**
     * Check if the listener is enabled for a specific event type.
     *
     * @param event the received event
     * @return true if the event instance match the predicate
     */
    @Override
    public boolean isEnabled(CamelEvent event) {
        return event instanceof IoTDBTopicConsumerSubscribed
                || event instanceof IoTDBStopAllTopicConsumers
                || event instanceof IoTDBResumeAllTopicConsumers
                || event instanceof IoTDBTopicDropped
                || event instanceof CamelEvent.RouteRemovedEvent;
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
    }
}
