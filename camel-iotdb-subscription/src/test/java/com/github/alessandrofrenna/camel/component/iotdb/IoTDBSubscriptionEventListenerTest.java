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

import static org.mockito.Mockito.*;

import org.apache.camel.Endpoint;
import org.apache.camel.Route;
import org.apache.camel.spi.CamelEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBResumeAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBStopAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicConsumerSubscribed;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicDropped;

public class IoTDBSubscriptionEventListenerTest {

    private AutoCloseable closeable;

    @Mock
    private IoTDBRoutesRegistry registry;

    @InjectMocks
    private IoTDBSubscriptionEventListener eventListener;

    @BeforeEach
    void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    void tearDown() throws Exception {
        closeable.close();
    }

    @Test
    void when_subscribed_event_is_published_the_registry_should_handle_the_event() {
        final IoTDBTopicConsumerSubscribed subscribedEvent = mock(IoTDBTopicConsumerSubscribed.class);
        eventListener.notify(subscribedEvent);
        verify(registry).registerRouteAfterConsumerSubscription(subscribedEvent);
    }

    @Test
    void when_stop_all_event_is_published_the_registry_should_handle_the_event() {
        final IoTDBStopAllTopicConsumers stopAllEvent = mock(IoTDBStopAllTopicConsumers.class);
        eventListener.notify(stopAllEvent);
        verify(registry).stopAllConsumedTopicRoutes(stopAllEvent);
    }

    @Test
    void when_topic_dropped_event_is_published_the_registry_should_handle_the_event() {
        final IoTDBTopicDropped topicDroppedEvent = mock(IoTDBTopicDropped.class);
        eventListener.notify(topicDroppedEvent);
        verify(registry).removeRoutesAfterTopicDrop(topicDroppedEvent);
    }

    @Test
    void when_resume_all_event_is_published_the_registry_should_handle_the_event() {
        final IoTDBResumeAllTopicConsumers resumeAllStoppedEvent = mock(IoTDBResumeAllTopicConsumers.class);
        eventListener.notify(resumeAllStoppedEvent);
        verify(registry).resumeAllStoppedConsumedTopicRoutes(resumeAllStoppedEvent);
    }

    @Test
    void when_removed_route_event_is_published_the_registry_should_handle_the_event() {
        final String TOPIC_NAME = "topic1";
        final String ROUTE_ID = "routeId1";

        final Route route = mock(Route.class);
        final CamelEvent.RouteRemovedEvent routeRemovedEvent = mock(CamelEvent.RouteRemovedEvent.class);
        final IoTDBTopicEndpoint topicEndpoint = mock(IoTDBTopicEndpoint.class);
        when(routeRemovedEvent.getRoute()).thenReturn(route);
        when(route.getRouteId()).thenReturn(ROUTE_ID);
        when(route.getEndpoint()).thenReturn(topicEndpoint);
        when(topicEndpoint.getTopic()).thenReturn(TOPIC_NAME);

        eventListener.notify(routeRemovedEvent);
        verify(registry).removeTopicMappedRoutes(TOPIC_NAME, ROUTE_ID);
    }

    @Test
    void removed_route_from_any_other_endpoint_should_not_be_handled() {
        final Route route = mock(Route.class);
        final CamelEvent.RouteRemovedEvent routeRemovedEvent = mock(CamelEvent.RouteRemovedEvent.class);
        final Endpoint endpoint = mock(Endpoint.class); // Not an IoTDBTopicEndpoint
        when(routeRemovedEvent.getRoute()).thenReturn(route);
        when(route.getEndpoint()).thenReturn(endpoint);

        eventListener.notify(routeRemovedEvent);
        verify(registry, never()).removeTopicMappedRoutes(anyString(), anyString());
    }

    @Test
    void any_other_event_should_not_be_handled() {
        CamelEvent unrelatedEvent = mock(CamelEvent.class); // An event not handled by the listener
        eventListener.notify(unrelatedEvent);
        verifyNoInteractions(registry); // No methods on registry should be called
    }
}
