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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.iotdb.session.subscription.model.Topic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import com.github.alessandrofrenna.camel.component.iotdb.support.IoTDBTestSupport;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class IoTDBTopicProducerTest extends IoTDBTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(IoTDBTopicProducerTest.class);

    private final String TIMESERIES_PATH = "root.test.test_device.test_variable";
    private final String PRODUCER_TOPIC_CREATE = "crate_test_topic";
    private final String PRODUCER_TOPIC_CREATE_EXISTING = "create_existing_test_topic";
    private final String PRODUCER_TOPIC_DROP = "drop_test_topic";
    private final String PRODUCER_TOPIC_DROP_NON_EXISTING = "drop_test_missing_topic";

    private boolean topicExists(String topicName) {
        AtomicBoolean exists = new AtomicBoolean(false);
        doInSession(session -> {
            Set<Topic> topics = session.getTopics();
            exists.set(topics.stream().anyMatch(t -> t.getTopicName().equals(topicName)));
        });
        return exists.get();
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:createTopic")
                        .to("iotdb-subscription:" + PRODUCER_TOPIC_CREATE + "?action=create&path=" + TIMESERIES_PATH)
                        .log(LoggingLevel.INFO, "Create topic route processed for " + PRODUCER_TOPIC_CREATE);

                // Route for creating an already existing topic
                from("direct:createExistingTopic")
                        .to("iotdb-subscription:" + PRODUCER_TOPIC_CREATE_EXISTING + "?action=create&path="
                                + TIMESERIES_PATH)
                        .log(
                                LoggingLevel.INFO,
                                "Create existing topic route processed for " + PRODUCER_TOPIC_CREATE_EXISTING);

                // Route for dropping a topic
                from("direct:dropTopic")
                        .to("iotdb-subscription:" + PRODUCER_TOPIC_DROP + "?action=drop")
                        .log(LoggingLevel.INFO, "Drop topic route processed for " + PRODUCER_TOPIC_DROP);

                // Route for dropping a non-existing topic
                from("direct:dropNonExistingTopic")
                        .to("iotdb-subscription:" + PRODUCER_TOPIC_DROP_NON_EXISTING + "?action=drop")
                        .log(
                                LoggingLevel.INFO,
                                "Drop non-existing topic route processed for " + PRODUCER_TOPIC_DROP_NON_EXISTING);

                // Route for testing create without path
                from("direct:createTopicNoPath")
                        .to("iotdb-subscription:someTopicNoPath?action=create") // No path parameter
                        .log(LoggingLevel.INFO, "Create topic no path route processed");

                // Consumer route to observe topic drop effects (optional)
                from("iotdb-subscription:" + PRODUCER_TOPIC_DROP + "?groupId=test_group&consumerId=drop_observer")
                        .routeId("dropObserverRoute")
                        .log(LoggingLevel.INFO, "Drop observer consumer running for " + PRODUCER_TOPIC_DROP)
                        .to("mock:dropObserver")
                        .autoStartup(false);
            }
        };
    }

    @BeforeEach
    void setUpTestSuite() {
        createTimeseriesPathQuietly(TIMESERIES_PATH);
    }

    @Test
    void createTopic_shouldBeSuccessful() {
        assertFalse(topicExists(PRODUCER_TOPIC_CREATE), "Topic should not exist before creation");
        template.sendBody("direct:createTopic", null);
        assertTrue(topicExists(PRODUCER_TOPIC_CREATE), "Topic should exist after creation");
    }

    @Test
    void when_topicAlreadyExists_createTopicShouldNotFail() {
        createTopicQuietly(PRODUCER_TOPIC_CREATE_EXISTING, TIMESERIES_PATH);

        assertTrue(topicExists(PRODUCER_TOPIC_CREATE_EXISTING), "Topic should be pre-created");
        template.sendBody("direct:createExistingTopic", "TestCreateExisting");
        assertTrue(topicExists(PRODUCER_TOPIC_CREATE_EXISTING), "Topic should still exist");
    }

    @Test
    void dropExistingTopic_shouldSucceed() throws Exception {
        createTopicQuietly(PRODUCER_TOPIC_DROP, TIMESERIES_PATH);
        assertTrue(topicExists(PRODUCER_TOPIC_DROP), "Topic should be pre-created for drop test");

        context.getRouteController().startRoute("dropObserverRoute");
        final MockEndpoint mockDropObserver = getMockEndpoint("mock:dropObserver");
        mockDropObserver.expectedMinimumMessageCount(0);

        template.sendBody("direct:dropTopic", null);
        Awaitility.await()
                .atMost(Duration.ofSeconds(3))
                .pollInterval(IoTDBTopicProducerConfiguration.PRE_DROP_DELAY)
                .untilAsserted(() -> {
                    assertFalse(topicExists(PRODUCER_TOPIC_DROP), "Topic should not exist after drop action");
                    assertNull(
                            context.getRoute("dropObserverRoute"), "Consumer route should be removed after topic drop");
                    mockDropObserver.assertIsSatisfied(100);
                });
    }

    @Test
    void dropNonExistingTopic_shouldSucceed() {
        assertFalse(topicExists(PRODUCER_TOPIC_DROP_NON_EXISTING), "Topic should not exist before drop attempt");
        Exchange exchange = template.send("direct:dropNonExistingTopic", (e) -> {});

        Awaitility.await()
                .atMost(Duration.ofSeconds(3))
                .pollInterval(IoTDBTopicProducerConfiguration.PRE_DROP_DELAY)
                .untilAsserted(() -> {
                    assertFalse(topicExists(PRODUCER_TOPIC_DROP_NON_EXISTING), "Topic should still not exist");
                    assertNull(exchange.getException());
                });
    }

    @Test
    void when_pathIsNotDefinedOnTopicCreation_theProducerShouldThrowAnException() {
        // We expect an exception during the processing of this route
        Exchange exchange = template.send("direct:createTopicNoPath", ex -> {});
        Exception caughtException = exchange.getException();

        assertNotNull(caughtException, "An exception should have occurred");
        assertInstanceOf(IllegalArgumentException.class, caughtException, "");
        assertEquals("path is required when action=create", caughtException.getMessage());
    }
}
