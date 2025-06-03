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

import static com.github.alessandrofrenna.camel.test.infra.iotdb.common.IoTDbProperties.DEFAULT_PORT;
import static com.github.alessandrofrenna.camel.test.infra.iotdb.common.IoTDbProperties.IOTDB_HOST;
import static com.github.alessandrofrenna.camel.test.infra.iotdb.common.IoTDbProperties.IOTDB_PORT;
import static com.github.alessandrofrenna.camel.test.infra.iotdb.common.IoTDbProperties.IOTDB_SERVICE_ADDRESS;

import org.apache.camel.spi.annotations.InfraService;
import org.apache.camel.test.infra.common.services.ContainerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <b>IoTDbLocalContainerInfrastructure</b> is the default implementation of {@link IoTDbInfraService}.<br>
 * This class also implements the {@link ContainerService} interface.<br>
 * It is the infrastructure that will be used by camel to create the container for testing
 */
@InfraService(
        service = IoTDbInfraService.class,
        description = "IoTDb is a database used to efficiently store data from iot devices",
        serviceAlias = {"iotdb", "iotdb-service"})
public class IoTDbLocalContainerInfrastructure implements IoTDbInfraService, ContainerService<IoTDbContainer> {

    private static final Logger LOG = LoggerFactory.getLogger(IoTDbLocalContainerInfrastructure.class);

    private final IoTDbContainer container;

    /**
     * Default <b>IoTDbLocalContainerInfrastructure</b> constructor.
     */
    public IoTDbLocalContainerInfrastructure() {
        container = new IoTDbContainer();
    }

    /**
     * Create an <b>IoTDbLocalContainerInfrastructure</b> instance using the image name of the constructor.
     * @param imageName of the container.
     */
    public IoTDbLocalContainerInfrastructure(String imageName) {
        container = IoTDbContainer.initContainer(imageName, IoTDbContainer.CONTAINER_NAME);
    }

    /**
     * Register properties that will be used by the contained in the system.
     */
    @Override
    public void registerProperties() {
        System.setProperty(IOTDB_SERVICE_ADDRESS, getServiceAddress());
        System.setProperty(IOTDB_HOST, host());
        System.setProperty(IOTDB_PORT, String.valueOf(port()));
    }

    /**
     * Initialize the container instance.
     */
    @Override
    public void initialize() {
        LOG.info("Trying to start IoTDb container");
        container.start();
        registerProperties();
        LOG.info("IoTDb instance running at {}", getServiceAddress());
    }

    /**
     * Shutdown the container instance.
     */
    @Override
    public void shutdown() {
        LOG.info("Stopping IoTDb container");
        container.stop();
    }

    /**
     * Get the running container instance.
     * @return the container instance
     */
    @Override
    public IoTDbContainer getContainer() {
        return container;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String host() {
        return container.getHost();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int port() {
        return container.getMappedPort(DEFAULT_PORT);
    }
}
