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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBResumeAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBStopAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicConsumerSubscribed;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicDropped;

@ExtendWith(MockitoExtension.class)
public class IoTDBSubscriptionEventListenerTest {

    @Mock
    private IoTDBRoutesRegistry registry;

    @InjectMocks
    private IoTDBSubscriptionEventListener eventListener;

    @Test
    void when_subscribedEventisPublished_theRegistryShouldHandleTheEvent() {
        final IoTDBTopicConsumerSubscribed subscribedEvent = mock(IoTDBTopicConsumerSubscribed.class);
        eventListener.notify(subscribedEvent);
        verify(registry).registerRouteAfterConsumerSubscription(subscribedEvent);
    }

    @Test
    void when_stopAllEventIsPublished_theRegistryShouldHandleTheEvent() {
        final IoTDBStopAllTopicConsumers stopAllEvent = mock(IoTDBStopAllTopicConsumers.class);
        eventListener.notify(stopAllEvent);
        verify(registry).stopAllConsumedTopicRoutes(stopAllEvent);
    }

    @Test
    void when_topicDroppedEventIsPublished_theRegistryShouldHandleTheEvent() {
        final IoTDBTopicDropped topicDroppedEvent = mock(IoTDBTopicDropped.class);
        eventListener.notify(topicDroppedEvent);
        verify(registry).removeRoutesAfterTopicDrop(topicDroppedEvent);
    }

    @Test
    void when_resumeAllEventIsPublished_theRegistryShouldHandleTheEvent() {
        final IoTDBResumeAllTopicConsumers resumeAllStoppedEvent = mock(IoTDBResumeAllTopicConsumers.class);
        eventListener.notify(resumeAllStoppedEvent);
        verify(registry).resumeAllStoppedConsumedTopicRoutes(resumeAllStoppedEvent);
    }

    @Test
    void when_removedRouteEventIsPublished_theRegistryShouldHandleTheEvent() {
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
    void when_routesAreRemovedFromContextNotUsingTheProducer_theyShouldBeRemovedFromRegistry() {
        final Route route = mock(Route.class);
        final CamelEvent.RouteRemovedEvent routeRemovedEvent = mock(CamelEvent.RouteRemovedEvent.class);
        final Endpoint endpoint = mock(Endpoint.class); // Not an IoTDBTopicEndpoint
        when(routeRemovedEvent.getRoute()).thenReturn(route);
        when(route.getEndpoint()).thenReturn(endpoint);

        eventListener.notify(routeRemovedEvent);
        verify(registry, never()).removeTopicMappedRoutes(anyString(), anyString());
    }

    @Test
    void anyOtherEventShouldNotBeHandled() {
        CamelEvent unrelatedEvent = mock(CamelEvent.class); // An event not handled by the listener
        eventListener.notify(unrelatedEvent);
        verifyNoInteractions(registry); // No methods on registry should be called
    }
}
