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

import static org.apache.iotdb.rpc.subscription.config.TopicConstant.FORMAT_KEY;
import static org.apache.iotdb.rpc.subscription.config.TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE;
import static org.apache.iotdb.rpc.subscription.config.TopicConstant.MODE_DEFAULT_VALUE;
import static org.apache.iotdb.rpc.subscription.config.TopicConstant.MODE_KEY;
import static org.apache.iotdb.rpc.subscription.config.TopicConstant.NOW_TIME_VALUE;
import static org.apache.iotdb.rpc.subscription.config.TopicConstant.PATH_KEY;
import static org.apache.iotdb.rpc.subscription.config.TopicConstant.START_TIME_KEY;

import java.util.Properties;
import java.util.function.Supplier;

import org.apache.camel.RuntimeCamelException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <b>IoTDBTopicManager</b> interface define create and drop methods to perform topic's operations.
 */
public interface IoTDBTopicManager {
    /**
     * Crate a topic with a name on a specific IoTDB timeseries path.</br>
     * If the topic already exists the creation will be skipped.
     *
     * @param topicName is the name of the topic to create
     * @param path is the path on which the topic will be bound
     */
    void createTopicIfNotExists(String topicName, String path);

    /**
     * Delete a topic from IoTDB using its name.</br>
     * If the topic doesn't exist it will not be dropped.
     *
     * @param topicName is the name of the topic to drop
     */
    void dropTopicIfExists(String topicName);

    /**
     * The <b>Default</b> class implements {@link IoTDBTopicManager} interface.</br>
     * It is used as delegate to handle create or drop operations on IoTDB topics.
     */
    class Default implements IoTDBTopicManager {
        private static final Logger LOG = LoggerFactory.getLogger(IoTDBTopicManager.class);

        private final Supplier<SubscriptionSession> sessionFactory;

        /**
         * Create a default {@link IoTDBTopicManager} instance.
         *
         * @param sessionFactory to supply an instance of a {@link SubscriptionSession}
         */
        public Default(Supplier<SubscriptionSession> sessionFactory) {
            this.sessionFactory = sessionFactory;
        }

        /**
         * {@inheritDoc}
         *
         * @param topicName is the name of the topic to create.
         * @param path is the path on which the topic will be bound.
         **/
        @Override
        public void createTopicIfNotExists(String topicName, String path) {
            Properties topicProperties = new Properties();
            topicProperties.put(PATH_KEY, path);
            topicProperties.put(START_TIME_KEY, NOW_TIME_VALUE);
            topicProperties.put(MODE_KEY, MODE_DEFAULT_VALUE);
            topicProperties.put(FORMAT_KEY, FORMAT_SESSION_DATA_SETS_HANDLER_VALUE);

            try (var session = sessionFactory.get()) {
                session.open();
                LOG.debug("Attempting to create topic '{}' for path '{}'", topicName, path);
                session.createTopicIfNotExists(topicName, topicProperties);
                LOG.info("Topic {} bound to {} timeseries path", topicName, path);
            } catch (IoTDBConnectionException e) {
                throw new RuntimeCamelException("Failed to connect to IoTDB for topic creation: " + e.getMessage(), e);
            } catch (StatementExecutionException e) {
                String errorMessage = String.format(
                        "Execution failed during creation/check of topic '%s' for path '%s'", topicName, path);
                LOG.error(errorMessage, e);
                throw new RuntimeCamelException(errorMessage, e);
            }
        }

        /**
         * {@inheritDoc}
         *
         * @param topicName is the name of the topic to drop
         * */
        @Override
        public void dropTopicIfExists(String topicName) {
            try (var session = sessionFactory.get()) {
                session.open();
                LOG.debug("Attempting to drop IoTDB topic '{}' (if it exists)", topicName);
                session.dropTopicIfExists(topicName);
                LOG.info("IoTDB topic '{}' successfully dropped (if it existed).", topicName);
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                LOG.error("Drop of IoTDB topic '{}' failed. IoTDBTopicDropped event will not be sent", topicName, e);
                throw new RuntimeCamelException(e);
            }
        }
    }
}
