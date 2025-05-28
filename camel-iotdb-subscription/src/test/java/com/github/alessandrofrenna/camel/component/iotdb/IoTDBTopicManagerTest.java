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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Properties;
import java.util.function.Supplier;

import org.apache.camel.RuntimeCamelException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IoTDBTopicManagerTest {
    @Mock
    private SubscriptionSession session;

    @Mock
    private Supplier<SubscriptionSession> sessionFactory;

    private IoTDBTopicManager.Default topicManager;

    @Captor
    private ArgumentCaptor<Properties> propertiesCaptor;

    @BeforeEach
    void setUp() {
        when(sessionFactory.get()).thenReturn(session);
        topicManager = new IoTDBTopicManager.Default(sessionFactory);
    }

    @Test
    void topicCreation_shouldSucceed() throws IoTDBConnectionException, StatementExecutionException {
        String topicName = "test_topic";
        String path = "root.test_device.test_variable";
        topicManager.createTopicIfNotExists(topicName, path);

        verify(sessionFactory).get();
        verify(session).open();
        verify(session).createTopicIfNotExists(eq(topicName), propertiesCaptor.capture());
        verify(session).close();

        Properties capturedProps = propertiesCaptor.getValue();
        assertEquals(path, capturedProps.getProperty(TopicConstant.PATH_KEY));
        assertEquals(TopicConstant.NOW_TIME_VALUE, capturedProps.getProperty(TopicConstant.START_TIME_KEY));
        assertEquals(TopicConstant.MODE_DEFAULT_VALUE, capturedProps.getProperty(TopicConstant.MODE_KEY));
        assertEquals(
                TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE,
                capturedProps.getProperty(TopicConstant.FORMAT_KEY));
    }

    @Test
    void topicCreationShouldFails_onSessionConnectionExceptions()
            throws IoTDBConnectionException, StatementExecutionException {
        String topicName = "test_topic";
        String path = "root.test_device.test_variable";

        doThrow(new IoTDBConnectionException("Connection failed")).when(session).open();
        RuntimeCamelException thrown =
                assertThrows(RuntimeCamelException.class, () -> topicManager.createTopicIfNotExists(topicName, path));
        assertInstanceOf(IoTDBConnectionException.class, thrown.getCause());

        verify(sessionFactory).get();
        verify(session).open();
        verify(session, never()).createTopicIfNotExists(anyString(), any(Properties.class));
        verify(session).close();
    }

    @Test
    void topicCreationShouldFail_onStatementExceptions() throws IoTDBConnectionException, StatementExecutionException {
        String topicName = "test_topic";
        String path = "root.test_device.test_variable";

        doThrow(new StatementExecutionException("Execution failed"))
                .when(session)
                .createTopicIfNotExists(eq(topicName), any(Properties.class));
        RuntimeCamelException thrown =
                assertThrows(RuntimeCamelException.class, () -> topicManager.createTopicIfNotExists(topicName, path));
        assertInstanceOf(StatementExecutionException.class, thrown.getCause());

        verify(sessionFactory).get();
        verify(session).open();
        verify(session).createTopicIfNotExists(eq(topicName), any(Properties.class));
        verify(session).close(); // Should be called due to try-with-resources
    }

    @Test
    void topicCreationShouldFail_onLossOfSessionConnection()
            throws IoTDBConnectionException, StatementExecutionException {
        String topicName = "test_topic";
        String path = "root.test_device.test_variable";

        doThrow(new IoTDBConnectionException("Connection lost during create"))
                .when(session)
                .createTopicIfNotExists(eq(topicName), any(Properties.class));
        RuntimeCamelException thrown =
                assertThrows(RuntimeCamelException.class, () -> topicManager.createTopicIfNotExists(topicName, path));
        assertInstanceOf(IoTDBConnectionException.class, thrown.getCause());

        verify(sessionFactory).get();
        verify(session).open();
        verify(session).createTopicIfNotExists(eq(topicName), any(Properties.class));
        verify(session).close();
    }

    @Test
    void topicDrop_shouldSucceed() throws IoTDBConnectionException, StatementExecutionException {
        String topicName = "test_topic";

        topicManager.dropTopicIfExists(topicName);
        verify(sessionFactory).get();
        verify(session).open();
        verify(session).dropTopicIfExists(topicName);
        verify(session).close();
    }

    @Test
    void topicDropShouldFail_onSessionConnectionException()
            throws IoTDBConnectionException, StatementExecutionException {
        String topicName = "test_topic";

        doThrow(new IoTDBConnectionException("Connection failed")).when(session).open();
        RuntimeCamelException thrown =
                assertThrows(RuntimeCamelException.class, () -> topicManager.dropTopicIfExists(topicName));
        assertInstanceOf(IoTDBConnectionException.class, thrown.getCause());

        verify(sessionFactory).get();
        verify(session).open();
        verify(session, never()).dropTopicIfExists(anyString());
        verify(session).close(); // From try-with-resources, even if open failed after get()
    }

    @Test
    void topicDropShouldFail_onStatementException() throws IoTDBConnectionException, StatementExecutionException {
        String topicName = "test_topic";

        doThrow(new StatementExecutionException("Execution failed"))
                .when(session)
                .dropTopicIfExists(topicName);
        RuntimeCamelException thrown =
                assertThrows(RuntimeCamelException.class, () -> topicManager.dropTopicIfExists(topicName));
        assertInstanceOf(StatementExecutionException.class, thrown.getCause());

        verify(sessionFactory).get();
        verify(session).open();
        verify(session).dropTopicIfExists(topicName);
        verify(session).close(); // Should be called due to try-with-resources
    }

    @Test
    void topicDropShouldFail_onLossOfSessionConnection() throws IoTDBConnectionException, StatementExecutionException {
        String topicName = "test_topic";

        doThrow(new IoTDBConnectionException("Connection lost during drop"))
                .when(session)
                .dropTopicIfExists(topicName);
        RuntimeCamelException thrown =
                assertThrows(RuntimeCamelException.class, () -> topicManager.dropTopicIfExists(topicName));
        assertInstanceOf(IoTDBConnectionException.class, thrown.getCause());

        verify(sessionFactory).get();
        verify(session).open();
        verify(session).dropTopicIfExists(topicName);
        verify(session).close(); // Should be called due to try-with-resources
    }
}
