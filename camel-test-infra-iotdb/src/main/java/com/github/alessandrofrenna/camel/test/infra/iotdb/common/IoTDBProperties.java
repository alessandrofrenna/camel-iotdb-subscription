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
package com.github.alessandrofrenna.camel.test.infra.iotdb.common;

/**
 * This class define the default configuration properties required for an IoTDB server instance to create a valid session.
 */
public class IoTDBProperties {

    /**
     * The default IoTDB container property name
     */
    public static final String IOTDB_CONTAINER = "iotdb.container";

    /**
     * The default IoTDB service address value property name
     */
    public static final String IOTDB_SERVICE_ADDRESS = "iotdb.service.address";

    /**
     * The default IoTDB port property name
     */
    public static final String IOTDB_PORT = "iotdb.port";

    /**
     * The default IoTDB host property name
     */
    public static final String IOTDB_HOST = "iotdb.host";

    /**
     * The default IoTDB port property name
     */
    public static final int DEFAULT_PORT = 6667;
}
