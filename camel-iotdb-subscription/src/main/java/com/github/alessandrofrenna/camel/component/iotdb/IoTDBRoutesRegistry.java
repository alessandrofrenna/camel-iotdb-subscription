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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBResumeAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBStopAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicConsumerSubscribed;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicDropped;

public interface IoTDBRoutesRegistry extends CamelContextAware, AutoCloseable {
    void registerRouteAfterConsumerSubscription(IoTDBTopicConsumerSubscribed event);

    void stopAllConsumedTopicRoutes(IoTDBStopAllTopicConsumers event);

    void resumeAllStoppedConsumedTopicRoutes(IoTDBResumeAllTopicConsumers event);

    void removeRoutesAfterTopicDrop(IoTDBTopicDropped event);

    void removeTopicMappedRoutes(String topicName, String routeId);

    void close();

    class Default implements IoTDBRoutesRegistry {
        private static final Logger LOG = LoggerFactory.getLogger(IoTDBRoutesRegistry.class);
        private final Map<String, Set<String>> routesIdByTopic = new ConcurrentHashMap<>();
        private final IoTDBTopicConsumerManager consumerManager;
        private CamelContext camelContext;

        public Default(IoTDBTopicConsumerManager consumerManager) {
            Objects.requireNonNull(consumerManager, "consumer manager is null");
            this.consumerManager = consumerManager;
        }

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

        @Override
        public void stopAllConsumedTopicRoutes(IoTDBStopAllTopicConsumers event) {
            final String topicName = event.getTopicName();
            if (!routesIdByTopic.containsKey(topicName)) {
                LOG.warn("No active consumer routes found in tracking map for topic '{}' to stop.", topicName);
                return;
            }
            new ArrayList<>(routesIdByTopic.get(topicName)).forEach(this::stopRoute);
        }

        @Override
        public void resumeAllStoppedConsumedTopicRoutes(IoTDBResumeAllTopicConsumers event) {
            final String topicName = event.getTopicName();
            if (!routesIdByTopic.containsKey(topicName)) {
                LOG.warn("No stopped consumer routes found in tracking map for topic '{}' to start.", topicName);
                return;
            }
            new ArrayList<>(routesIdByTopic.get(topicName)).forEach(this::restartRoute);
        }

        @Override
        public void removeRoutesAfterTopicDrop(IoTDBTopicDropped event) {
            String topicName = event.getTopicName();
            if (!routesIdByTopic.containsKey(topicName)) {
                LOG.warn("No stopped consumer routes found in tracking map for topic '{}' to remove.", topicName);
                return;
            }
            new ArrayList<>(routesIdByTopic.get(topicName)).forEach(this::removeStopped);
        }

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

        @Override
        public void setCamelContext(CamelContext camelContext) {
            this.camelContext = camelContext;
        }

        @Override
        public CamelContext getCamelContext() {
            if (camelContext == null) {
                throw new IllegalStateException("camel context is null but is needed by the registry");
            }
            return camelContext;
        }

        void stopRoute(String routeId) {
            final CamelContext ctx = getCamelContext();
            final ServiceStatus routeStatus = ctx.getRouteController().getRouteStatus(routeId);
            // Let's make our intent clear! Only started and yet stoppable routes could be stopped.
            // Already stopping or stopped routes don't require further processing
            if (routeStatus != null && routeStatus.isStarted()
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

        void restartRoute(String routeId) {
            final CamelContext ctx = getCamelContext();
            final ServiceStatus routeStatus = ctx.getRouteController().getRouteStatus(routeId);
            // Let's make our intent clear! Only stopped and yet startable routes could be restarted.
            // Already starting or started routes don't require further processing
            if (routeStatus != null && routeStatus.isStopped()
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

        void removeStopped(String routeId) {
            final CamelContext ctx = getCamelContext();
            final ServiceStatus routeStatus = ctx.getRouteController().getRouteStatus(routeId);
            // Let's make our intent clear! Only stopped routes could be removed when calling this method
            if (routeStatus != null && routeStatus.isStopped()) {
                try {
                    PushConsumerKey consumerKey = null;
                    final Route route = ctx.getRoute(routeId);
                    if (route == null) {
                        LOG.warn("Route '{}' was not found in CamelContext during removal attempt. It might have been removed concurrently", routeId);
                        return;
                    }
                    if (route.getConsumer() != null && route.getConsumer() instanceof IoTDBTopicConsumer topicConsumer) {
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
