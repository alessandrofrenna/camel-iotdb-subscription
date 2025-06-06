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
package com.github.alessandrofrenna.camel.test.infra.iotdb.services;

import static com.github.alessandrofrenna.camel.test.infra.iotdb.common.IoTDbProperties.*;

import java.util.Optional;

import org.apache.camel.test.infra.common.LocalPropertyResolver;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * The <b>IoTDbContainer</b> class extends {@link GenericContainer class}.<br>
 * It defines the controller that will be created and launched.
 */
public class IoTDbContainer extends GenericContainer<IoTDbContainer> {
    /**
     * The default container name.
     */
    public static final String CONTAINER_NAME = "iotdb";

    /**
     * Default <b>IoTDbContainer</b> constructor.
     */
    public IoTDbContainer() {
        super(LocalPropertyResolver.getProperty(IoTDbLocalContainerInfrastructure.class, IOTDB_CONTAINER));
        int port = Optional.of(LocalPropertyResolver.getProperty(IoTDbLocalContainerInfrastructure.class, IOTDB_PORT))
                .map(Integer::parseInt)
                .orElse(DEFAULT_PORT);
        this.withNetworkAliases(CONTAINER_NAME).withExposedPorts(port).waitingFor(Wait.forListeningPort());
    }

    /**
     * Create a <b>IoTDbContainer</b> using its name.
     * @param imageName of iotdb
     */
    public IoTDbContainer(String imageName) {
        super(DockerImageName.parse(imageName));
    }

    /**
     * Initialize a <b>IoTDbContainer</b> using its name and network aliases.
     * @param imageName of iotdb
     * @param networkAlias to use with the container
     * @return an instance of the container object
     */
    public static IoTDbContainer initContainer(String imageName, String networkAlias) {
        int port = Optional.of(LocalPropertyResolver.getProperty(IoTDbLocalContainerInfrastructure.class, IOTDB_PORT))
                .map(Integer::parseInt)
                .orElse(DEFAULT_PORT);
        return new IoTDbContainer(imageName)
                .withNetworkAliases(networkAlias)
                .withExposedPorts(port)
                .waitingFor(Wait.forListeningPort());
    }
}
