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

/** The <b>IoTDBTopicProducerConfiguration</b> class keeps track of the uri parameters used by the producer. */
@UriParams
public class IoTDBTopicProducerConfiguration {

    @UriParam
    @Metadata(
            title = "Topic action",
            description = "Action to execute. It can either be create or update",
            required = true,
            enums = "create,drop")
    private String action;

    @UriParam
    @Metadata(
            title = "Timeseries path",
            description = "The path associated to the created topic",
            enums = "create,delete")
    private String path;

    public IoTDBTopicProducerConfiguration() {}

    /**
     * Get the action passed as route uri parameter.</br> The value of action is required. It must be one of "create" or
     * "delete".</br> The "create" action will be used to create a topic at a given by {@link #getPath()}.</br> The
     * "delete" action will delete the topic.
     *
     * @return the action to apply to the topic
     */
    public String getAction() {
        return action;
    }

    /**
     * Set the action passed as route uri parameter.
     *
     * @param action is a string that could either be "create" or "delete"
     */
    public void setAction(String action) {
        this.action = action;
    }

    /**
     * Get the timeseries path passed as route uri parameter.</br> The value of the path is required only for the
     * "create" action. When action is "delete" this parameter, if passed is ignored.
     *
     * @return an optional wrapping the timeseries path
     */
    public Optional<String> getPath() {
        return Optional.ofNullable(path);
    }

    /**
     * Set the timeseries path passed as route uri parameter.
     *
     * @param path is a string that represent a valid IoTDB timeseries path
     */
    public void setPath(String path) {
        this.path = path;
    }
}
