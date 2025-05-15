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

import java.util.Optional;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriParams;

/** The <b>IoTDBTopicConsumerConfiguration</b> class keeps track of the uri parameters used by the consumer. */
@UriParams
public class IoTDBTopicConsumerConfiguration {

    @UriParam
    @Metadata(
            title = "Consumer group id",
            description = "The consumer group id that contains the topic consumer. Randomly assigned if not provided")
    private String groupId;

    @UriParam
    @Metadata(
            title = "Topic consumer id",
            description =
                    "The id of the topic consumer that belongs to a consumer group. Randomly assigned if not provided")
    private String consumerId;

    @UriParam
    @Metadata(
            title = "Heartbeat interval",
            description =
                    "The consumer will ping IoTDB to check health after this interval of time in millisecond passes")
    private Long heartbeatIntervalMs;

    @UriParam
    @Metadata(
            title = "Synchronization interval",
            description =
                    "The consumer will ping IoTDB to synchronize its state after this interval of time in millisecond passes")
    private Long syncIntervalMs;

    public IoTDBTopicConsumerConfiguration() {}

    /**
     * Get the consumer group id passed as route uri parameter.</br> The value of consumer group id is optional. If not
     * defined it will be randomly assigned.
     *
     * @return an optional wrapping the consumer group id
     */
    public Optional<String> getGroupId() {
        return Optional.ofNullable(groupId);
    }

    /**
     * Set the consumer group id from the route uri parameters.
     *
     * @param groupId optional string that identifies a consumer group
     */
    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * Get the consumer id passed as route uri parameter.</br> The value of consumer id is optional. If not defined it
     * will be randomly assigned.
     *
     * @return an optional wrapping the consumer group id
     */
    public Optional<String> getConsumerId() {
        return Optional.ofNullable(consumerId);
    }

    /**
     * Set the consumer group id from the route uri parameters.
     *
     * @param consumerId optional string that identifies a consumer inside a group
     */
    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    /**
     * Get the heartbeat interval used by the consumer to perform healthcheck.</br> This value is optional. If not
     * provided this method returns the default value of 30_000ms (30s).
     *
     * @return the heartbeat interval in milliseconds
     */
    public Long getHeartbeatIntervalMs() {
        if (heartbeatIntervalMs == null) {
            heartbeatIntervalMs = 30_000L;
        }
        return heartbeatIntervalMs;
    }

    /**
     * Set the heartbeat interval from the route uri parameters.
     *
     * @param heartbeatIntervalMs milliseconds interval used by the consumer to perform healthcheck against IoTDB
     */
    public void setHeartbeatIntervalMs(Long heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    /**
     * Get the synchronization interval used by the consumer to sync against IoTDB.</br> This value is optional. If not
     * provided this method returns the default value of 120_000ms (120s).
     *
     * @return the synchronization interval in milliseconds
     */
    public Long getSyncIntervalMs() {
        if (syncIntervalMs == null) {
            syncIntervalMs = 120_000L;
        }
        return syncIntervalMs;
    }

    /**
     * Set the synchronization interval from the route uri parameters.
     *
     * @param syncIntervalMs milliseconds interval used by the consumer to sync against IoTDB
     */
    public void setSyncIntervalMs(Long syncIntervalMs) {
        this.syncIntervalMs = syncIntervalMs;
    }
}
