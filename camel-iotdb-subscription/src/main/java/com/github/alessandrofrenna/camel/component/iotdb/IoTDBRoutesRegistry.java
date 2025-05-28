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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelContextAware;
import org.apache.camel.Route;
import org.apache.camel.ServiceStatus;
import org.apache.camel.spi.CamelEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBResumeAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBStopAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicConsumerSubscribed;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicDropped;

/**
 * The <b>IoTDBRoutesRegistry</b> interface extends {@link CamelContextAware}.<br>
 * The interface defines methods that are used inside {@link IoTDBSubscriptionEventListener#notify(CamelEvent)}
 * to handle the received events.
 */
public interface IoTDBRoutesRegistry extends CamelContextAware, AutoCloseable {

    /**
     * Register a route after a {@link org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer} is created.
     *
     * @param event of type {@link IoTDBTopicConsumerSubscribed}
     */
    void registerRouteAfterConsumerSubscription(IoTDBTopicConsumerSubscribed event);

    /**
     * Stop all routes that consume from a topic.
     * The routes can be stopped only if their status is {@link ServiceStatus#Started}.
     *
     * @param event of type {@link IoTDBStopAllTopicConsumers}
     */
    void stopAllConsumedTopicRoutes(IoTDBStopAllTopicConsumers event);

    /**
     * Resume all consumers that were previously stopped.
     * The routes can be resumed only if their status is {@link ServiceStatus#Stopped}.
     *
     * @param event of type {@link IoTDBResumeAllTopicConsumers}
     */
    void resumeAllStoppedConsumedTopicRoutes(IoTDBResumeAllTopicConsumers event);

    /**
     * Remove all the routes that were tight to a topic that was dropped.<br>
     * The routes can be deleted only if their status is {@link ServiceStatus#Stopped}.
     *
     * @param event {@link IoTDBTopicDropped}
     */
    void removeRoutesAfterTopicDrop(IoTDBTopicDropped event);

    /**
     * Remove the mapped routeId after a {@link CamelEvent.RouteRemovedEvent}.
     *
     * @param topicName to remove
     * @param routeId tighted to the topic name
     */
    void removeTopicMappedRoutes(String topicName, String routeId);

    /**
     * {@inheritDoc}
     */
    void close();

    /**
     * The <b>Default</b> class implements {@link IoTDBRoutesRegistry} interface.<br>
     * It is used as delegate to handle registry operations.
     */
    class Default implements IoTDBRoutesRegistry {
        private static final Logger LOG = LoggerFactory.getLogger(IoTDBRoutesRegistry.class);
        private final Map<String, Set<String>> routesIdByTopic = new ConcurrentHashMap<>();
        private final IoTDBTopicConsumerManager consumerManager;
        private CamelContext camelContext;

        /**
         * Create a {@link IoTDBRoutesRegistry} instance.
         *
         * @param consumerManager dependency to manage consumers
         */
        public Default(IoTDBTopicConsumerManager consumerManager) {
            Objects.requireNonNull(consumerManager, "consumer manager is null");
            this.consumerManager = consumerManager;
        }

        /**
         * {@inheritDoc}
         *
         * @param event of type {@link IoTDBTopicConsumerSubscribed}
         */
        @Override
        public void registerRouteAfterConsumerSubscription(IoTDBTopicConsumerSubscribed event) {
            final String topicName = event.getTopicName();
            final String routeId = event.getRouteId();
            final var topicRoutes = routesIdByTopic.computeIfAbsent(topicName, k -> ConcurrentHashMap.newKeySet());
            topicRoutes.add(routeId);

            LOG.debug(
                    "Route '{}' now actively tracked for topic '{}'. Current routes for topic: {}",
                    routeId,
                    topicName,
                    topicRoutes);
        }

        /**
         * {@inheritDoc}
         *
         * @param event of type {@link IoTDBStopAllTopicConsumers}
         */
        @Override
        public void stopAllConsumedTopicRoutes(IoTDBStopAllTopicConsumers event) {
            final String topicName = event.getTopicName();
            if (!routesIdByTopic.containsKey(topicName)) {
                LOG.warn("No active consumer routes found in tracking map for topic '{}' to stop.", topicName);
                return;
            }
            new ArrayList<>(routesIdByTopic.get(topicName)).forEach(this::stopRoute);
        }

        /**
         * {@inheritDoc}
         *
         * @param event of type {@link IoTDBResumeAllTopicConsumers}
         */
        @Override
        public void resumeAllStoppedConsumedTopicRoutes(IoTDBResumeAllTopicConsumers event) {
            final String topicName = event.getTopicName();
            if (!routesIdByTopic.containsKey(topicName)) {
                LOG.warn("No stopped consumer routes found in tracking map for topic '{}' to start.", topicName);
                return;
            }
            new ArrayList<>(routesIdByTopic.get(topicName)).forEach(this::restartRoute);
        }

        /**
         * {@inheritDoc}
         *
         * @param event {@link IoTDBTopicDropped}
         */
        @Override
        public void removeRoutesAfterTopicDrop(IoTDBTopicDropped event) {
            String topicName = event.getTopicName();
            if (!routesIdByTopic.containsKey(topicName)) {
                LOG.warn("No stopped consumer routes found in tracking map for topic '{}' to remove.", topicName);
                return;
            }
            new ArrayList<>(routesIdByTopic.get(topicName)).forEach(this::removeStopped);
        }

        /**
         * {@inheritDoc}
         *
         * @param topicName to remove
         * @param routeId tighted to the topic name
         */
        @Override
        public void removeTopicMappedRoutes(String topicName, String routeId) {
            routesIdByTopic.computeIfPresent(topicName, (key, routes) -> {
                if (routes.remove(routeId)) {
                    LOG.debug(
                            "Topic '{}' mapped route {} removed. Route still associated: {}",
                            topicName,
                            routeId,
                            routes);
                }
                // If the set for this topic is now empty, remove the topic entry from the map
                return routes.isEmpty() ? null : routes;
            });
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() {
            final var camelContext = getCamelContext();
            new ArrayList<>(routesIdByTopic.values())
                    .stream().flatMap(Collection::stream).forEach(routeId -> {
                        try {
                            camelContext.removeRoute(routeId);
                        } catch (Exception ignore) {
                            // NO-OP
                        }
                    });
            LOG.debug("Cleared all topic mapped routes from registry");
            routesIdByTopic.clear();
        }

        /**
         * {@inheritDoc}
         *
         * @param camelContext the Camel context
         */
        @Override
        public void setCamelContext(CamelContext camelContext) {
            this.camelContext = camelContext;
        }

        /**
         * {@inheritDoc}
         *
         * @return the Camel context
         */
        @Override
        public CamelContext getCamelContext() {
            if (camelContext == null) {
                throw new IllegalStateException("camel context is null but is needed by the registry");
            }
            return camelContext;
        }

        /**
         * Stop a route when its state is {@link ServiceStatus#Started}.
         *
         * @param routeId to stop
         */
        void stopRoute(String routeId) {
            final CamelContext ctx = getCamelContext();
            final ServiceStatus routeStatus = ctx.getRouteController().getRouteStatus(routeId);
            // Let's make our intent clear! Only started and yet stoppable routes could be stopped.
            // Already stopping or stopped routes don't require further processing
            if (routeStatus != null
                    && routeStatus.isStarted()
                    && routeStatus.isStoppable()
                    && !routeStatus.isStopping()
                    && !routeStatus.isStopped()) {
                try {
                    ctx.getRouteController().stopRoute(routeId);
                    LOG.debug("Required 'stop' request for route '{}' ", routeId);
                } catch (Exception e) {
                    LOG.error("Failed to stop route '{}': {}", routeId, e.getMessage(), e);
                }
                return;
            }
            LOG.warn(
                    "Route '{}' is not in a stoppable state (Status: {}). Skipping stop command.",
                    routeId,
                    routeStatus);
        }

        /**
         * Restart/Resume a route when its state is {@link ServiceStatus#Stopped}.
         *
         * @param routeId to restart/resume
         */
        void restartRoute(String routeId) {
            final CamelContext ctx = getCamelContext();
            final ServiceStatus routeStatus = ctx.getRouteController().getRouteStatus(routeId);
            // Let's make our intent clear! Only stopped and yet startable routes could be restarted.
            // Already starting or started routes don't require further processing
            if (routeStatus != null
                    && routeStatus.isStopped()
                    && routeStatus.isStartable()
                    && !routeStatus.isStarting()
                    && !routeStatus.isStarted()) {
                try {
                    ctx.getRouteController().startRoute(routeId);
                    LOG.debug("Required 'start' request for route '{}' ", routeId);
                } catch (Exception e) {
                    LOG.error("Failed to start route '{}': {}", routeId, e.getMessage(), e);
                }
                return;
            }
            LOG.warn(
                    "Route '{}' is not in a startable state (Status: {}). Skipping start command.",
                    routeId,
                    routeStatus);
        }

        /**
         * Remove a route when its state is {@link ServiceStatus#Stopped}.
         *
         * @param routeId to remove
         */
        void removeStopped(String routeId) {
            final CamelContext ctx = getCamelContext();
            final ServiceStatus routeStatus = ctx.getRouteController().getRouteStatus(routeId);
            // Let's make our intent clear! Only stopped routes could be removed when calling this method
            if (routeStatus != null && routeStatus.isStopped()) {
                try {
                    PushConsumerKey consumerKey = null;
                    final Route route = ctx.getRoute(routeId);
                    if (route == null) {
                        LOG.warn(
                                "Route '{}' was not found in CamelContext during removal attempt. It might have been removed concurrently",
                                routeId);
                        return;
                    }
                    if (route.getConsumer() != null
                            && route.getConsumer() instanceof IoTDBTopicConsumer topicConsumer) {
                        consumerKey = topicConsumer.getPushConsumerKey();
                    }
                    ctx.removeRoute(routeId);
                    LOG.debug("Required 'remove' request for route '{}' ", routeId);
                    if (consumerKey != null) {
                        consumerManager.destroyPushConsumer(consumerKey);
                    }
                } catch (Exception e) {
                    LOG.error("Failed to remove route '{}': {}", routeId, e.getMessage(), e);
                }
                return;
            }
            LOG.warn(
                    "Route '{}' is not in a removable state (Status: {}). Skipping remove command.",
                    routeId,
                    routeStatus);
        }
    }
}
