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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.camel.impl.event.RouteRemovedEvent;
import org.apache.camel.spi.CamelEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBResumeAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBStopAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicConsumerSubscribed;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicDropped;

public class IoTDBSubscriptionEventListenerTest {
    private IoTDBRoutesRegistry registry;
    private IoTDBSubscriptionEventListener listener;

    @BeforeEach
    void setUp() {
        registry = mock(IoTDBRoutesRegistry.class);
        listener = new IoTDBSubscriptionEventListener(registry);
    }

    @Test
    void when_consumer_subscribes_a_route_is_registered() {
        var event = mock(IoTDBTopicConsumerSubscribed.class);
        listener.notify(event);
        verify(registry).registerRouteAfterConsumerSubscription(event);
        verifyNoMoreInteractions(registry);
    }

    @Test
    void when_all_on_topic_consumer_are_stopped_routes_should_stop() {
        var event = mock(IoTDBStopAllTopicConsumers.class);
        listener.notify(event);
        verify(registry).stopAllConsumedTopicRoutes(event);
        verifyNoMoreInteractions(registry);
    }

    @Test
    void when_a_topic_is_dropped_routes_should_be_removed() {
        var dropEvent = mock(IoTDBTopicDropped.class);
        var removeEvent = mock(CamelEvent.RouteRemovedEvent.class);
        listener.notify(dropEvent);
        verify(registry).removeRoutesAfterTopicDrop(dropEvent);
        verifyNoMoreInteractions(registry);
    }

    @Test
    void when_resume_all_topic_consumers_routes_should_be_resumed() {
        var event = mock(IoTDBResumeAllTopicConsumers.class);
        listener.notify(event);
        verify(registry).resumeAllStoppedConsumedTopicRoutes(event);
        verifyNoMoreInteractions(registry);
    }

    @Test
    void when_routes_removed_from_camel_mappings_are_removed_from_registry() {
        // prepare a fake endpoint with topic & routeId
        var endpoint = mock(IoTDBTopicEndpoint.class);
        when(endpoint.getTopic()).thenReturn("rain_topic");
        var route = mock(org.apache.camel.Route.class);
        when(route.getEndpoint()).thenReturn(endpoint);
        when(route.getRouteId()).thenReturn("rain_topic_route");
        CamelEvent.RouteRemovedEvent event = new RouteRemovedEvent(route);

        listener.notify(event);
        verify(registry).removeTopicMappedRoutes("rain_topic", "rain_topic_route");
        verifyNoMoreInteractions(registry);
    }
}
