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

import static com.github.alessandrofrenna.camel.component.iotdb.IoTDbSessionConfiguration.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.camel.Endpoint;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.annotations.Component;
import org.apache.camel.support.HealthCheckComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the <b>IoTDBSubscriptionComponent</b> extends the HealthCheckComponent.<br>The component allows the integration
 * between IoTDB data subscription API and apache camel
 */
@Component("iotdb-subscription")
public class IoTDbSubscriptionComponent extends HealthCheckComponent {
    private static final Logger LOG = LoggerFactory.getLogger(IoTDbSubscriptionComponent.class);

    @Metadata(title = "IoTDB host", required = true, defaultValue = DEFAULT_HOST)
    private String host;

    @Metadata(title = "IoTDB port", required = true, defaultValue = DEFAULT_PORT, javaType = "Integer")
    private Integer port;

    @Metadata(title = "IoTDB username", required = true, secret = true, defaultValue = DEFAULT_USERNAME)
    private String user;

    @Metadata(title = "IoTDB password", required = true, secret = true, defaultValue = DEFAULT_PASSWORD)
    private String password;

    /**
     * Create an {@link IoTDbSubscriptionComponent} instance
     */
    public IoTDbSubscriptionComponent() {}

    /**
     * Get the IoTDB host name.
     *
     * @return host
     */
    public String getHost() {
        return host;
    }

    /**
     * Set the IoTDB host name.
     *
     * @param host name
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Get the IoTDB server port.
     *
     * @return the server port
     */
    public int getPort() {
        return port;
    }

    /**
     * Set the IoTDB server port.
     *
     * @param port number
     */
    public void setPort(Integer port) {
        this.port = port;
    }

    /**
     * Get the IoTDB user name.
     *
     * @return the username
     */
    public String getUser() {
        return user;
    }

    /**
     * Set the IoTDB user name.
     *
     * @param user name
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Get the IoTDB user password.
     *
     * @return the password associated with the value returned by {@link #getUser()}
     */
    public String getPassword() {
        return password;
    }

    /**
     * The IoTDB user password.
     *
     * @param password associated with the value returned by {@link #getUser()}
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Get the session configuration.
     *
     * @return the session configuration
     */
    public IoTDbSessionConfiguration getSessionConfig() {
        return new IoTDbSessionConfiguration(getHost(), getPort(), getUser(), getPassword());
    }

    protected Endpoint createEndpoint(String uri, String path, Map<String, Object> parameters) throws Exception {
        List<String> pathParts = Arrays.asList(path.split(":"));
        if (pathParts.size() != 2) {
            throw new IllegalArgumentException("path must contain consumerGroupId:consumerId");
        }

        IoTDbConsumerEndpoint endpoint = new IoTDbConsumerEndpoint(uri, this);
        endpoint.setConsumerGroupId(pathParts.get(0));
        endpoint.setConsumerId(pathParts.get(1));
        setProperties(endpoint, parameters);

        LOG.debug("A new IoTDbTopicEndpoint has been created");
        return endpoint;
    }
}
