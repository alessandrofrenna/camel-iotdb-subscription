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

import org.apache.camel.spi.CamelEvent;
import org.apache.camel.support.EventNotifierSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <b>IoTDBSubscriptionEventListener</b> extends {@link EventNotifierSupport}.</br> It handles events coming from
 * {@link IoTDBTopicProducer} and {@link IoTDBTopicConsumer} instances.
 */
public class IoTDBSubscriptionEventListener extends EventNotifierSupport {
    private static final Logger LOG = LoggerFactory.getLogger(IoTDBSubscriptionEventListener.class);

    /**
     * Handle events coming from {@link IoTDBTopicProducer} and {@link IoTDBTopicConsumer} instances. The supported
     * events are:
     *
     * <ol>
     *   <li>{@link IoTDBTopicDropped}
     * </ol>
     *
     * @param event to handle
     */
    @Override
    public void notify(CamelEvent event) {
        if (event instanceof IoTDBTopicDropped dropEvent) {
            handleDropEvent(dropEvent);
        }
    }

    private void handleDropEvent(IoTDBTopicDropped dropEvent) {
        LOG.debug("{}", dropEvent);
    }

    //    private void stopRelatedConsumers(String deletedTopicName) {
    //        CamelContext camelContext = getCamelContext();
    //        if (camelContext == null) {
    //            LOG.debug("Cannot stop related consumers for topic '{}': CamelContext is not available.",
    // deletedTopicName);
    //            return;
    //        }
    //
    //        LOG.debug("Attempting to find and stop Camel consumers for deleted IoTDB topic: {}", deletedTopicName);
    //        int stoppedCount = 0;
    //
    //        // Iterate over a snapshot of route IDs to avoid ConcurrentModificationException
    //        // if stopping a route triggers some event that modifies the routes list.
    //        List<String> routeIds = new ArrayList<>();
    //        for (Route route : camelContext.getRoutes()) {
    //            routeIds.add(route.getId());
    //        }
    //
    //        for (String routeId : routeIds) {
    //            try {
    //                // It's possible the route was removed by another thread/process between getRoutes and now
    //                Route route = camelContext.getRoute(routeId);
    //                if (route == null) {
    //                    LOG.warn("Route with ID '{}' no longer exists, skipping for consumer stop check.", routeId);
    //                    continue;
    //                }
    //
    //                Endpoint consumerEndpoint = route.getConsumer().getEndpoint();
    //                // Check if this endpoint is an instance of your IoTDBTopicEndpoint
    //                // And if it's for the topic that was just deleted.
    //                if (consumerEndpoint instanceof IoTDBTopicEndpoint specificEndpoint) {
    //                    if (deletedTopicName.equals(specificEndpoint.getTopic())) {
    //                        stopAndRemoveRoute(camelContext, deletedTopicName, routeId);
    //                        stoppedCount++;
    //                    }
    //                } else {
    //                    // Fallback for more generic URI checking if direct type matching isn't always possible
    //                    // This is less precise and more prone to error if URI structures are complex.
    //                    String endpointUri = consumerEndpoint.getEndpointUri();
    //                    if (endpointUri.startsWith("iotdb-subscription:" + deletedTopicName)) {
    //                        // Basic check: "iotdb-subscription:deletedTopic?param=value"
    //                        // More robust parsing might be needed if topic names can contain special chars
    //                        // that affect splitting.
    //                        String topicPart = endpointUri.substring("iotdb-subscription:".length());
    //                        // Further check to ensure it's not a partial match, e.g. topic "foo" vs "foobar"
    //                        if (topicPart.startsWith(deletedTopicName)
    //                                && (topicPart.equals(deletedTopicName)
    //                                || topicPart.startsWith(deletedTopicName + "?"))) {
    //                            stopAndRemoveRoute(camelContext, deletedTopicName, routeId);
    //                            stoppedCount++;
    //                        }
    //                    }
    //                }
    //            } catch (Exception e) {
    //                LOG.error(
    //                        "Error while trying to stop consumer route '{}' for deleted topic '{}': {}",
    //                        routeId,
    //                        deletedTopicName,
    //                        e.getMessage(),
    //                        e);
    //            }
    //        }
    //
    //        if (stoppedCount > 0) {
    //            LOG.info(
    //                    "Successfully stopped {} consumer route(s) associated with deleted IoTDB topic '{}'.",
    //                    stoppedCount,
    //                    deletedTopicName);
    //        } else {
    //            LOG.info("No active Camel consumer routes found for the deleted IoTDB topic '{}'.", deletedTopicName);
    //        }
    //    }
    //
    //    private void stopAndRemoveRoute(CamelContext camelContext, String deletedTopicName, String routeId)
    //            throws Exception {
    //        LOG.trace(
    //                "Route '{}' is consuming from the deleted topic '{}'. Attempting to stop route.",
    //                routeId,
    //                deletedTopicName);
    //        // Stops the route and its consumer
    //        camelContext.getRouteController().stopRoute(routeId);
    //        // Remove the route definition because it should not be restartable
    //        boolean removed = camelContext.removeRoute(routeId);
    //        LOG.trace("Route '{}' removed: {}", routeId, removed);
    //    }
}
