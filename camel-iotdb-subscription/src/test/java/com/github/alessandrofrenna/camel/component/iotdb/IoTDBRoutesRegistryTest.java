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
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

import org.apache.camel.CamelContext;
import org.apache.camel.Route;
import org.apache.camel.ServiceStatus;
import org.apache.camel.spi.RouteController;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBResumeAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBStopAllTopicConsumers;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicConsumerSubscribed;
import com.github.alessandrofrenna.camel.component.iotdb.event.IoTDBTopicDropped;

class IoTDBRoutesRegistryTest {
    private final String TOPIC_1 = "topic1";
    private final String ROUTE_ID_1 = "routeId1";
    private final String ROUTE_ID_2 = "routeId2";
    private final String TOPIC_2 = "topic2";
    private final String ROUTE_ID_3 = "routeId3";

    private AutoCloseable closeable;

    @Mock
    private IoTDBTopicConsumerManager consumerManager;

    @Mock
    private IoTDBTopicConsumer topicConsumer;

    @Mock
    private Route route;

    @Mock
    private CamelContext camelContext;

    @Mock
    private RouteController routeController;

    private IoTDBRoutesRegistry.Default routesRegistry;
    private Map<String, Set<String>> routesIdByTopicSpy;

    @BeforeEach
    void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
        routesRegistry = new IoTDBRoutesRegistry.Default(consumerManager);
        routesRegistry.setCamelContext(camelContext);
        when(camelContext.getRouteController()).thenReturn(routeController);
        routesIdByTopicSpy = getField(routesRegistry, "routesIdByTopic");
    }

    @AfterEach
    void tearDown() throws Exception {
        routesIdByTopicSpy.clear();
        closeable.close();
    }

    @SuppressWarnings("unchecked")
    static <T> T getField(Object container, String fieldName) {
        var parent = container.getClass();
        try {
            Field field = parent.getDeclaredField(fieldName);
            field.setAccessible(true);
            return (T) field.get(container);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void route_registration_after_subscription_event_should_succeed() {
        IoTDBTopicConsumerSubscribed event = new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_1);
        routesRegistry.registerRouteAfterConsumerSubscription(event);
        assertEquals(1, routesIdByTopicSpy.size());
        assertEquals(1, routesIdByTopicSpy.get(TOPIC_1).size());
        assertTrue(routesIdByTopicSpy.containsKey(TOPIC_1));
        assertTrue(routesIdByTopicSpy.get(TOPIC_1).contains(ROUTE_ID_1));
    }

    @Test
    void route_registration_after_subscription_events_for_the_same_topic_should_succeed() {
        IoTDBTopicConsumerSubscribed event1 = new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_1);
        IoTDBTopicConsumerSubscribed event2 = new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_2);
        routesRegistry.registerRouteAfterConsumerSubscription(event1);
        routesRegistry.registerRouteAfterConsumerSubscription(event2);

        assertEquals(1, routesIdByTopicSpy.size());
        assertTrue(routesIdByTopicSpy.containsKey(TOPIC_1));
        assertEquals(2, routesIdByTopicSpy.get(TOPIC_1).size());
        assertTrue(routesIdByTopicSpy.get(TOPIC_1).contains(ROUTE_ID_1));
        assertTrue(routesIdByTopicSpy.get(TOPIC_1).contains(ROUTE_ID_2));
    }

    @Test
    void route_registration_after_subscription_events_for_different_topics_should_succeed() {
        IoTDBTopicConsumerSubscribed event1 = new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_1);
        IoTDBTopicConsumerSubscribed event2 = new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_2, ROUTE_ID_3);
        routesRegistry.registerRouteAfterConsumerSubscription(event1);
        routesRegistry.registerRouteAfterConsumerSubscription(event2);

        assertEquals(2, routesIdByTopicSpy.size());
        assertTrue(routesIdByTopicSpy.containsKey(TOPIC_1));
        assertEquals(1, routesIdByTopicSpy.get(TOPIC_1).size());
        assertTrue(routesIdByTopicSpy.get(TOPIC_1).contains(ROUTE_ID_1));
        assertTrue(routesIdByTopicSpy.containsKey(TOPIC_2));
        assertEquals(1, routesIdByTopicSpy.get(TOPIC_2).size());
        assertTrue(routesIdByTopicSpy.get(TOPIC_2).contains(ROUTE_ID_3));
    }

    @Test
    void when_status_is_valid_route_should_stop() throws Exception {
        // simulate a started & stoppable route
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_1));
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_2));

        ServiceStatus status = mock(ServiceStatus.class);
        when(status.isStarted()).thenReturn(true);
        when(status.isStoppable()).thenReturn(true);
        when(status.isStopping()).thenReturn(false);
        when(status.isStopped()).thenReturn(false);

        when(routeController.getRouteStatus(ROUTE_ID_1)).thenReturn(status); // Stoppable
        when(routeController.getRouteStatus(ROUTE_ID_2)).thenReturn(status); // Stoppable

        IoTDBStopAllTopicConsumers event = new IoTDBStopAllTopicConsumers(new Object(), TOPIC_1);
        routesRegistry.stopAllConsumedTopicRoutes(event);

        verify(routeController).stopRoute(ROUTE_ID_1);
        verify(routeController).stopRoute(ROUTE_ID_2);
    }

    @Test
    void when_status_is_not_valid_route_stop_should_be_skipped() throws Exception {
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_1));

        when(routeController.getRouteStatus(ROUTE_ID_1)).thenReturn(ServiceStatus.Stopped);

        IoTDBStopAllTopicConsumers event = new IoTDBStopAllTopicConsumers(new Object(), TOPIC_1);
        verify(routeController, never()).stopRoute(any());
    }

    @Test
    void test_stop_of_routes_of_the_same_topic_with_different_statuses() throws Exception {
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_1));
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_2));

        when(routeController.getRouteStatus(ROUTE_ID_1)).thenReturn(ServiceStatus.Started); // Stoppable
        when(routeController.getRouteStatus(ROUTE_ID_2))
                .thenReturn(ServiceStatus.Stopped); // Not stoppable by stopRoute logic

        IoTDBStopAllTopicConsumers event = new IoTDBStopAllTopicConsumers(new Object(), TOPIC_1);
        routesRegistry.stopAllConsumedTopicRoutes(event);

        verify(routeController).stopRoute(ROUTE_ID_1);
        verify(routeController, never()).stopRoute(ROUTE_ID_2); // Because it's already stopped
    }

    @Test
    void calling_stop_all_for_a_topic_that_does_not_exist() throws Exception {
        IoTDBStopAllTopicConsumers event = new IoTDBStopAllTopicConsumers(new Object(), "missing_topic");
        routesRegistry.stopAllConsumedTopicRoutes(event);
        verify(routeController, never()).stopRoute(anyString());
    }

    @Test
    void when_status_is_valid_route_should_restart() throws Exception {
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_1));
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_2));

        ServiceStatus status = mock(ServiceStatus.class);
        when(status.isStopped()).thenReturn(true);
        when(status.isStartable()).thenReturn(true);
        when(status.isStarting()).thenReturn(false);
        when(status.isStarted()).thenReturn(false);

        when(routeController.getRouteStatus(ROUTE_ID_1)).thenReturn(status); // Restartable
        when(routeController.getRouteStatus(ROUTE_ID_2)).thenReturn(status); // Restartable

        IoTDBResumeAllTopicConsumers event = new IoTDBResumeAllTopicConsumers(new Object(), TOPIC_1);
        routesRegistry.resumeAllStoppedConsumedTopicRoutes(event);

        verify(routeController).startRoute(ROUTE_ID_1);
        verify(routeController).startRoute(ROUTE_ID_2);
    }

    @Test
    void when_status_is_not_valid_route_restart_is_skipped() throws Exception {
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_2, ROUTE_ID_3));
        when(routeController.getRouteStatus(ROUTE_ID_3)).thenReturn(ServiceStatus.Started);

        IoTDBResumeAllTopicConsumers event = new IoTDBResumeAllTopicConsumers(new Object(), TOPIC_2);
        routesRegistry.resumeAllStoppedConsumedTopicRoutes(event);

        verify(routeController, never()).startRoute(ROUTE_ID_3);
    }

    @Test
    void test_resume_of_routes_of_the_same_topic_with_different_statuses() throws Exception {
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_1));
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_2));

        when(routeController.getRouteStatus(ROUTE_ID_1)).thenReturn(ServiceStatus.Started); // Non restartable
        when(routeController.getRouteStatus(ROUTE_ID_2)).thenReturn(ServiceStatus.Stopped); // Restartable

        IoTDBResumeAllTopicConsumers event = new IoTDBResumeAllTopicConsumers(new Object(), TOPIC_1);
        routesRegistry.resumeAllStoppedConsumedTopicRoutes(event);

        verify(routeController, never()).startRoute(ROUTE_ID_1); // Because it's already started
        verify(routeController).startRoute(ROUTE_ID_2);
    }

    @Test
    void calling_resume_all_for_a_topic_that_does_not_exist() throws Exception {
        IoTDBResumeAllTopicConsumers event = new IoTDBResumeAllTopicConsumers(new Object(), "missing_topic");
        routesRegistry.resumeAllStoppedConsumedTopicRoutes(event);
        verify(routeController, never()).startRoute(anyString());
    }

    @Test
    void when_topic_is_dropped_routes_should_be_removed_close_and_consumers_destroyed() throws Exception {
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_1));
        when(routeController.getRouteStatus(ROUTE_ID_1)).thenReturn(ServiceStatus.Stopped); // Removable

        when(camelContext.getRoute(ROUTE_ID_1)).thenReturn(route);
        when(route.getConsumer()).thenReturn(topicConsumer);
        PushConsumerKey key1 = new PushConsumerKey("group1", "consumer1");
        when(topicConsumer.getPushConsumerKey()).thenReturn(key1);

        routesRegistry.removeRoutesAfterTopicDrop(new IoTDBTopicDropped(new Object(), TOPIC_1));
        verify(camelContext).removeRoute(ROUTE_ID_1);
        verify(consumerManager).destroyPushConsumer(key1);
    }

    @Test
    void when_topic_is_dropped_with_non_stopped_routes_nothing_should_be_done() throws Exception {
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_1));
        when(routeController.getRouteStatus(ROUTE_ID_1)).thenReturn(ServiceStatus.Started);

        routesRegistry.removeRoutesAfterTopicDrop(new IoTDBTopicDropped(new Object(), TOPIC_1));

        verify(camelContext, never()).removeRoute(anyString());
        verify(consumerManager, never()).destroyPushConsumer(any());
    }

    @Test
    void calling_drop_all_for_a_topic_that_does_not_exist() throws Exception {
        IoTDBTopicDropped event = new IoTDBTopicDropped(new Object(), "missing_topic");
        routesRegistry.removeRoutesAfterTopicDrop(event);
        verify(camelContext, never()).removeRoute(anyString());
    }

    @Test
    void remove_topic_with_routes_should_remove_a_single_route_entry_from_the_map() {
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_1));
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_2));
        assertTrue(routesIdByTopicSpy.containsKey(TOPIC_1));
        assertEquals(2, routesIdByTopicSpy.get(TOPIC_1).size());

        routesRegistry.removeTopicMappedRoutes(TOPIC_1, ROUTE_ID_1);

        assertTrue(routesIdByTopicSpy.containsKey(TOPIC_1));
        assertEquals(1, routesIdByTopicSpy.get(TOPIC_1).size());
        assertFalse(routesIdByTopicSpy.get(TOPIC_1).contains(ROUTE_ID_1));
        assertTrue(routesIdByTopicSpy.get(TOPIC_1).contains(ROUTE_ID_2));
    }

    @Test
    void remove_topic_with_single_mapped_route_should_remove_entry_from_the_map() {
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_1));
        routesRegistry.removeTopicMappedRoutes(TOPIC_1, ROUTE_ID_1);
        assertFalse(routesIdByTopicSpy.containsKey(TOPIC_1));
        assertNull(routesIdByTopicSpy.get(TOPIC_1), "Topic entry should be removed from map");
        assertTrue(routesIdByTopicSpy.isEmpty());
    }

    @Test
    void calling_remove_topic_routes_for_a_topic_that_does_not_exist() {
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_1));
        routesRegistry.removeTopicMappedRoutes(TOPIC_1, "missing_topic");

        assertTrue(routesIdByTopicSpy.containsKey(TOPIC_1));
        assertEquals(1, routesIdByTopicSpy.get(TOPIC_1).size());
        assertTrue(routesIdByTopicSpy.get(TOPIC_1).contains(ROUTE_ID_1));
    }

    @Test
    void clear_all_should_clear_the_map() {
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_1));
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_2));
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_2, ROUTE_ID_3));
        routesRegistry.close();
        assertTrue(routesIdByTopicSpy.isEmpty());
    }

    @Test
    void creating_registry_with_null_consumer_manager_should_throw_exception() {
        assertThrows(NullPointerException.class, () -> new IoTDBRoutesRegistry.Default(null));
    }

    @Test
    void when_camel_context_is_null_get_context_should_throw_exception() {
        IoTDBRoutesRegistry.Default newRegistry = new IoTDBRoutesRegistry.Default(consumerManager);
        assertThrows(IllegalStateException.class, newRegistry::getCamelContext);
    }

    // --- Tests for stopRoute, restartRoute, removeStopped internal helper methods ---

    @Test
    void when_call_to_route_controller_stop_route_fails_registry_stop_route_should_not_throw() throws Exception {
        when(routeController.getRouteStatus(ROUTE_ID_1)).thenReturn(ServiceStatus.Started);
        doThrow(new Exception("Simulated stop error")).when(routeController).stopRoute(ROUTE_ID_1);
        assertDoesNotThrow(() -> routesRegistry.stopRoute(ROUTE_ID_1));
        verify(routeController).stopRoute(ROUTE_ID_1); // Ensure attempt was made
    }

    @Test
    void when_call_to_route_controller_start_route_fails_registry_restart_route_should_not_throw() throws Exception {
        when(routeController.getRouteStatus(ROUTE_ID_1)).thenReturn(ServiceStatus.Stopped);
        doThrow(new Exception("Simulated start error")).when(routeController).startRoute(ROUTE_ID_1);

        assertDoesNotThrow(() -> routesRegistry.restartRoute(ROUTE_ID_1));
        verify(routeController).startRoute(ROUTE_ID_1); // Ensure attempt was made
    }

    @Test
    void when_call_to_route_controller_remove_route_fails_registry_remove_stopped_should_not_throw() throws Exception {
        when(routeController.getRouteStatus(ROUTE_ID_1)).thenReturn(ServiceStatus.Stopped);
        when(camelContext.getRoute(ROUTE_ID_1)).thenReturn(route);
        when(route.getConsumer()).thenReturn(mock(org.apache.camel.Consumer.class)); // Generic
        when(camelContext.removeRoute(ROUTE_ID_1)).thenThrow(new RuntimeException("Simulated remove error"));

        assertDoesNotThrow(() -> routesRegistry.removeStopped(ROUTE_ID_1));
        verify(camelContext).removeRoute(ROUTE_ID_1); // Ensure attempt
        verify(consumerManager, never()).destroyPushConsumer(any(PushConsumerKey.class));
    }
}
