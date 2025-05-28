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
    void routeRegistration_afterSubscriptionEvent_shouldSucceed() {
        IoTDBTopicConsumerSubscribed event = new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_1);
        routesRegistry.registerRouteAfterConsumerSubscription(event);
        assertEquals(1, routesIdByTopicSpy.size());
        assertEquals(1, routesIdByTopicSpy.get(TOPIC_1).size());
        assertTrue(routesIdByTopicSpy.containsKey(TOPIC_1));
        assertTrue(routesIdByTopicSpy.get(TOPIC_1).contains(ROUTE_ID_1));
    }

    @Test
    void routeRegistrationForTheSameTopic_afterSubscriptionEvent_shouldSucceed() {
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
    void routeRegistrationForDifferentTopics_afterSubscriptionEvents_shouldSucceed() {
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
    void when_statusIsValid_routeShouldStop() throws Exception {
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
    void when_statusIsNotValid_routeStopShouldBeSkipped() throws Exception {
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_1));

        when(routeController.getRouteStatus(ROUTE_ID_1)).thenReturn(ServiceStatus.Stopped);

        IoTDBStopAllTopicConsumers event = new IoTDBStopAllTopicConsumers(new Object(), TOPIC_1);
        verify(routeController, never()).stopRoute(any());
    }

    @Test
    void stopRoutesOfConsumersOnTheSameTopic_withRoutesOnDifferentStatuses() throws Exception {
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
    void stoppingAllRoutes_ofATopicThatDoesNotExist_shouldSucceedBecauseNothingShouldBeDone() throws Exception {
        IoTDBStopAllTopicConsumers event = new IoTDBStopAllTopicConsumers(new Object(), "missing_topic");
        routesRegistry.stopAllConsumedTopicRoutes(event);
        verify(routeController, never()).stopRoute(anyString());
    }

    @Test
    void when_statusIsValid_routeShouldRestart() throws Exception {
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
    void when_statusIsNotValid_routeRestartIsSkipped() throws Exception {
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_2, ROUTE_ID_3));
        when(routeController.getRouteStatus(ROUTE_ID_3)).thenReturn(ServiceStatus.Started);

        IoTDBResumeAllTopicConsumers event = new IoTDBResumeAllTopicConsumers(new Object(), TOPIC_2);
        routesRegistry.resumeAllStoppedConsumedTopicRoutes(event);

        verify(routeController, never()).startRoute(ROUTE_ID_3);
    }

    @Test
    void resumingRoutesOfConsumerOnTheSameTopic_withRoutesOnDifferentStatuses() throws Exception {
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
    void callingResumeAll_forATopicThatDoesNotExist_shouldNeverResultInACallOfStartRoute() throws Exception {
        IoTDBResumeAllTopicConsumers event = new IoTDBResumeAllTopicConsumers(new Object(), "missing_topic");
        routesRegistry.resumeAllStoppedConsumedTopicRoutes(event);
        verify(routeController, never()).startRoute(anyString());
    }

    @Test
    void when_topicIsDropped_routesShouldBeRemoved_andConsumersShouldBeClosedAndDestroyed() throws Exception {
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
    void when_topicIsDroppedWithNonStoppedRoutes_nothingShouldBeDone() throws Exception {
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_1));
        when(routeController.getRouteStatus(ROUTE_ID_1)).thenReturn(ServiceStatus.Started);

        routesRegistry.removeRoutesAfterTopicDrop(new IoTDBTopicDropped(new Object(), TOPIC_1));

        verify(camelContext, never()).removeRoute(anyString());
        verify(consumerManager, never()).destroyPushConsumer(any());
    }

    @Test
    void callingDropAll_onANonExistingTopic_shouldNotEndUpWithACallOnRemoveRoute() throws Exception {
        IoTDBTopicDropped event = new IoTDBTopicDropped(new Object(), "missing_topic");
        routesRegistry.removeRoutesAfterTopicDrop(event);
        verify(camelContext, never()).removeRoute(anyString());
    }

    @Test
    void when_aTopicWithDifferentMappedRoutesIsRemoved_onlyTheRouteShouldBeRemovedFromTheRegistry() {
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
    void when_aTopicWithASingleMappedRouteIsRemoved_itsEntryShouldBeRemovedFromTheRegistry() {
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_1));
        routesRegistry.removeTopicMappedRoutes(TOPIC_1, ROUTE_ID_1);
        assertFalse(routesIdByTopicSpy.containsKey(TOPIC_1));
        assertNull(routesIdByTopicSpy.get(TOPIC_1), "Topic entry should be removed from map");
        assertTrue(routesIdByTopicSpy.isEmpty());
    }

    @Test
    void callingRemoveTopicRoutes_onANonExistingTopic_shouldNotChangeTheRegistry() {
        routesRegistry.registerRouteAfterConsumerSubscription(
                new IoTDBTopicConsumerSubscribed(new Object(), TOPIC_1, ROUTE_ID_1));
        routesRegistry.removeTopicMappedRoutes(TOPIC_1, "missing_topic");

        assertTrue(routesIdByTopicSpy.containsKey(TOPIC_1));
        assertEquals(1, routesIdByTopicSpy.get(TOPIC_1).size());
        assertTrue(routesIdByTopicSpy.get(TOPIC_1).contains(ROUTE_ID_1));
    }

    @Test
    void clearAll_shouldClearTheMap() {
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
    void creatingTheRegistry_withANullConsumerManager_shouldThrowAnException() {
        assertThrows(NullPointerException.class, () -> new IoTDBRoutesRegistry.Default(null));
    }

    @Test
    void when_camelContextIsNull_getContext_shouldThrowAnException() {
        IoTDBRoutesRegistry.Default newRegistry = new IoTDBRoutesRegistry.Default(consumerManager);
        assertThrows(IllegalStateException.class, newRegistry::getCamelContext);
    }

    // --- Tests for stopRoute, restartRoute, removeStopped internal helper methods ---

    @Test
    void when_routeControllerStopRouteFails_registryStopRouteCall_shouldNotThrow() throws Exception {
        when(routeController.getRouteStatus(ROUTE_ID_1)).thenReturn(ServiceStatus.Started);
        doThrow(new Exception("Simulated stop error")).when(routeController).stopRoute(ROUTE_ID_1);
        assertDoesNotThrow(() -> routesRegistry.stopRoute(ROUTE_ID_1));
        verify(routeController).stopRoute(ROUTE_ID_1); // Ensure attempt was made
    }

    @Test
    void when_routeControllerStartRouteFails_registryRestartRouteCall_shouldNotThrow() throws Exception {
        when(routeController.getRouteStatus(ROUTE_ID_1)).thenReturn(ServiceStatus.Stopped);
        doThrow(new Exception("Simulated start error")).when(routeController).startRoute(ROUTE_ID_1);

        assertDoesNotThrow(() -> routesRegistry.restartRoute(ROUTE_ID_1));
        verify(routeController).startRoute(ROUTE_ID_1); // Ensure attempt was made
    }

    @Test
    void when_routeControllerRemoveRouteFails_registryRemoveStopped_shouldNotThrow() throws Exception {
        when(routeController.getRouteStatus(ROUTE_ID_1)).thenReturn(ServiceStatus.Stopped);
        when(camelContext.getRoute(ROUTE_ID_1)).thenReturn(route);
        when(route.getConsumer()).thenReturn(mock(org.apache.camel.Consumer.class)); // Generic
        when(camelContext.removeRoute(ROUTE_ID_1)).thenThrow(new RuntimeException("Simulated remove error"));

        assertDoesNotThrow(() -> routesRegistry.removeStopped(ROUTE_ID_1));
        verify(camelContext).removeRoute(ROUTE_ID_1); // Ensure attempt
        verify(consumerManager, never()).destroyPushConsumer(any(PushConsumerKey.class));
    }
}
