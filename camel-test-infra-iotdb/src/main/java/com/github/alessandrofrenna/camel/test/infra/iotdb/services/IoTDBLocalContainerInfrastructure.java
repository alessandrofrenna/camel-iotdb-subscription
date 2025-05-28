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

import static com.github.alessandrofrenna.camel.test.infra.iotdb.common.IoTDBProperties.DEFAULT_PORT;
import static com.github.alessandrofrenna.camel.test.infra.iotdb.common.IoTDBProperties.IOTDB_HOST;
import static com.github.alessandrofrenna.camel.test.infra.iotdb.common.IoTDBProperties.IOTDB_PORT;
import static com.github.alessandrofrenna.camel.test.infra.iotdb.common.IoTDBProperties.IOTDB_SERVICE_ADDRESS;

import org.apache.camel.spi.annotations.InfraService;
import org.apache.camel.test.infra.common.services.ContainerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <b>IoTDBLocalContainerInfrastructure</b> is the default implementation of {@link IoTDBInfraService}.<br>
 * This class also implements the {@link ContainerService} interface.<br>
 * It is the infrastructure that will be used by camel to create the container for testing
 */
@InfraService(
        service = IoTDBInfraService.class,
        description = "IoTDB is a database used to efficiently store data from iot devices",
        serviceAlias = {"iotdb", "iotdb-service"})
public class IoTDBLocalContainerInfrastructure implements IoTDBInfraService, ContainerService<IoTDBContainer> {

    private static final Logger LOG = LoggerFactory.getLogger(IoTDBLocalContainerInfrastructure.class);

    private final IoTDBContainer container;

    /**
     * Default <b>IoTDBLocalContainerInfrastructure</b> constructor.
     */
    public IoTDBLocalContainerInfrastructure() {
        container = new IoTDBContainer();
    }

    /**
     * Create an <b>IoTDBLocalContainerInfrastructure</b> instance using the image name of the constructor.
     * @param imageName of the container.
     */
    public IoTDBLocalContainerInfrastructure(String imageName) {
        container = IoTDBContainer.initContainer(imageName, IoTDBContainer.CONTAINER_NAME);
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
        LOG.info("Trying to start IoTDB container");
        container.start();
        registerProperties();
        LOG.info("IoTDB instance running at {}", getServiceAddress());
    }

    /**
     * Shutdown the container instance.
     */
    @Override
    public void shutdown() {
        LOG.info("Stopping IoTDB container");
        container.stop();
    }

    /**
     * Get the running container instance.
     * @return the container instance
     */
    @Override
    public IoTDBContainer getContainer() {
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
