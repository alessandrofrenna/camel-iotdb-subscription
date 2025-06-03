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

import org.apache.camel.test.infra.common.services.InfrastructureService;

/**
 * The <b>IoTDbInfraService</b> interface extends {@link InfrastructureService}.<br>
 * This interface defines the properties exposed by the infrastructural service.<br>
 * Its default implementations are:
 * <ul>
 *     <li>{@link IoTDbLocalContainerInfrastructure}</li>
 *     <li>{@link IoTDbRemoteContainerInfraService}</li>
 * </ul>
 *
 * You can configure the container properties inside an <b>iotdb.properties</b> file.<br>
 * By default, an instance of {@link IoTDbLocalContainerInfrastructure} will be created.
 * Add the proper property in the configuration to obtain an instance of {@link IoTDbRemoteContainerInfraService} instead.
 */
public interface IoTDbInfraService extends InfrastructureService {

    /**
     * Get the hostname used by the running container instance.
     * @return the hostname
     */
    String host();

    /**
     * Get the port used by the running container instance.
     * @return the port
     */
    int port();

    /**
     * Get the service address of the container instance.<br>
     * Its default implementation concatenate host and port into a string.
     * @return the service address
     */
    default String getServiceAddress() {
        return String.format("%s:%d", host(), port());
    }
}
