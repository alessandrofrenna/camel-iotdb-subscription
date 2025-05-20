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
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;

import org.apache.camel.CamelContext;
import org.apache.camel.Route;
import org.apache.camel.ServiceStatus;
import org.apache.camel.spi.RouteController;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class IoTDBRoutesRegistryTest {
    private IoTDBTopicConsumerManager consumerManager;
    private IoTDBRoutesRegistry.Default routesRegistry;
    private CamelContext camelContext;
    private RouteController routeController;

    @BeforeEach
    void setUp() {
        consumerManager = mock(IoTDBTopicConsumerManager.class);
        routeController = mock(RouteController.class);
        camelContext = mock(CamelContext.class);
        routesRegistry = new IoTDBRoutesRegistry.Default(consumerManager);
        routesRegistry.setCamelContext(camelContext);
        when(camelContext.getRouteController()).thenReturn(routeController);
    }

    @Test
    void when_status_is_valid_route_should_stop() throws Exception {
        // simulate a started & stoppable route
        final String ROUTE_ID = "first_route";
        ServiceStatus status = mock(ServiceStatus.class);
        when(status.isStarted()).thenReturn(true);
        when(status.isStoppable()).thenReturn(true);
        when(status.isStopping()).thenReturn(false);
        when(status.isStopped()).thenReturn(false);
        when(routeController.getRouteStatus(ROUTE_ID)).thenReturn(status);

        routesRegistry.stopRoute(ROUTE_ID);
        InOrder inOrder = inOrder(routeController);
        inOrder.verify(routeController).getRouteStatus(ROUTE_ID);
        inOrder.verify(routeController).stopRoute(ROUTE_ID);
        verifyNoMoreInteractions(routeController);
    }

    @Test
    void when_status_is_not_valid_route_stop_is_skipped() throws Exception {
        final String ROUTE_ID = "first_route";
        ServiceStatus status = mock(ServiceStatus.class);
        when(status.isStarted()).thenReturn(false);
        when(routeController.getRouteStatus(ROUTE_ID)).thenReturn(status);

        routesRegistry.stopRoute(ROUTE_ID);
        verify(routeController).getRouteStatus(ROUTE_ID);
        verify(routeController, never()).stopRoute(any());
    }

    @Test
    void when_status_is_valid_route_should_restart() throws Exception {
        // simulate a started & stoppable route
        final String ROUTE_ID = "first_route";
        ServiceStatus status = mock(ServiceStatus.class);
        when(status.isStopped()).thenReturn(true);
        when(status.isStartable()).thenReturn(true);
        when(status.isStarting()).thenReturn(false);
        when(status.isStarted()).thenReturn(false);
        when(routeController.getRouteStatus(ROUTE_ID)).thenReturn(status);

        routesRegistry.restartRoute(ROUTE_ID);
        InOrder inOrder = inOrder(routeController);
        inOrder.verify(routeController).getRouteStatus(ROUTE_ID);
        inOrder.verify(routeController).startRoute(ROUTE_ID);
        verifyNoMoreInteractions(routeController);
    }

    @Test
    void when_status_is_not_valid_route_restart_is_skipped() throws Exception {
        final String ROUTE_ID = "first_route";
        ServiceStatus status = mock(ServiceStatus.class);
        when(status.isStarted()).thenReturn(true);
        when(routeController.getRouteStatus(ROUTE_ID)).thenReturn(status);

        routesRegistry.restartRoute(ROUTE_ID);
        verify(routeController).getRouteStatus(ROUTE_ID);
        verify(routeController, never()).resumeRoute(any());
    }

    @Test
    void when_stopped_routes_are_removed_close_and_destroy_the_consumer() throws Exception {
        final String ROUTE_ID = "first_route";

        ServiceStatus status = mock(ServiceStatus.class);
        when(status.isStopped()).thenReturn(true);
        when(routeController.getRouteStatus(ROUTE_ID)).thenReturn(status);

        IoTDBTopicConsumer topicConsumer = mock(IoTDBTopicConsumer.class);
        PushConsumerKey key = new PushConsumerKey("group_a", "consumer_a");
        when(topicConsumer.getPushConsumerKey()).thenReturn(key);

        Route route = mock(Route.class);
        when(route.getConsumer()).thenReturn(topicConsumer);
        when(camelContext.getRoute(ROUTE_ID)).thenReturn(route);

        // invoke private removeStopped via reflection
        Method removeStopped = IoTDBRoutesRegistry.Default.class.getDeclaredMethod("removeStopped", String.class);
        removeStopped.setAccessible(true);
        removeStopped.invoke(routesRegistry, ROUTE_ID);
        verify(camelContext).removeRoute(ROUTE_ID);
        verify(consumerManager).destroyPushConsumer(key);
    }

    @Test
    void when_trying_to_remove_non_stopped_routes_do_nothing() throws Exception {
        final String ROUTE_ID = "first_route";
        ServiceStatus status = mock(ServiceStatus.class);
        when(status.isStopped()).thenReturn(false);
        when(routeController.getRouteStatus(ROUTE_ID)).thenReturn(status);

        // invoke private removeStopped via reflection
        Method removeStopped = IoTDBRoutesRegistry.Default.class.getDeclaredMethod("removeStopped", String.class);
        removeStopped.setAccessible(true);
        removeStopped.invoke(routesRegistry, ROUTE_ID);

        // verify no interactions
        verify(camelContext, never()).removeRoute(any());
        verify(consumerManager, never()).destroyPushConsumer(any());
    }
}
