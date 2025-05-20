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

package com.github.alessandrofrenna.camel.component.iotdb.event;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.apache.camel.CamelContext;
import org.apache.camel.EndpointAware;
import org.apache.camel.spi.CamelEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <b>EventPublisher</b> interface extending {@link EndpointAware} interface, allows producers and consumers to
 * publish events
 */
@FunctionalInterface
public interface EventPublisher extends EndpointAware {
    Logger LOG = LoggerFactory.getLogger(EventPublisher.class);

    /**
     * Notify events to {@link CamelContext}
     *
     * @param event to notify
     */
    default void publishEvent(CamelEvent event) {
        final String eventTypeName = event.getType().getClass().getSimpleName();
        final CamelContext ctx = getEndpoint().getCamelContext();
        if (ctx == null) {
            LOG.warn("CamelContext is not available. Cannot publish {}", eventTypeName);
            return;
        }

        final var timestamp = ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();
        event.setTimestamp(timestamp);

        try {
            LOG.trace("Publishing {}", eventTypeName);
            ctx.getManagementStrategy().notify(event);
        } catch (Exception e) {
            LOG.error("Failed to publish {} event: {}", eventTypeName, e.getMessage(), e);
        }
    }
}
