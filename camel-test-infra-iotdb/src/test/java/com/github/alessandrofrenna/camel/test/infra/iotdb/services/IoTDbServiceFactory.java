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

import org.apache.camel.test.infra.common.services.SimpleTestServiceBuilder;

/**
 * The <b>IoTDbServiceFactory</b> will create, or utilize a running instance, of an iotdb server container.<br>
 * Its purpose is to be utilized inside a camel component integration tests.
 */
public class IoTDbServiceFactory {

    /**
     * Default constructor
     */
    private IoTDbServiceFactory() {}

    /**
     * Create and return {@link SimpleTestServiceBuilder} instance for iotdb.
     * @return a test service builder instance
     */
    public static SimpleTestServiceBuilder<IoTDbService> builder() {
        return new SimpleTestServiceBuilder<>("iotdb");
    }

    /**
     * Crete an instance of {@link IoTDbService}.<br>
     * @return a local or a remote service
     */
    public static IoTDbService createService() {
        return builder()
                .addLocalMapping(IoTDbLocalContainerService::new)
                .addRemoteMapping(IoTDbRemoteContainerService::new)
                .build();
    }

    public static class IoTDbRemoteContainerService extends IoTDbRemoteContainerInfraService implements IoTDbService {}

    public static class IoTDbLocalContainerService extends IoTDbLocalContainerInfrastructure implements IoTDbService {}
}
