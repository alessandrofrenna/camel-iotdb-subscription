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

/**
 * The <b>IoTDbSessionConfiguration</b> record contains the properties used to establish an IoTDb session.
 *
 * @param host name
 * @param port value
 * @param user name
 * @param password of the user
 */
public record IoTDbSessionConfiguration(String host, int port, String user, String password) {
    /**
     * The default host name.
     */
    public static final String DEFAULT_HOST = "localhost";

    /**
     * The default port value.
     */
    public static final String DEFAULT_PORT = "6667";

    /**
     * The default username.
     */
    public static final String DEFAULT_USERNAME = "root";

    /**
     * The default password.
     */
    public static final String DEFAULT_PASSWORD = "root";

    /** Create a <b>IoTDbSessionConfiguration</b> an IoTDb session with default properties. */
    public IoTDbSessionConfiguration() {
        this(DEFAULT_HOST, Integer.parseInt(DEFAULT_PORT), DEFAULT_USERNAME, DEFAULT_PASSWORD);
    }
}
