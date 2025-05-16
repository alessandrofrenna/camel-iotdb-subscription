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
package com.github.alessandrofrenna.camel.component.iotdb.support;

import com.github.alessandrofrenna.camel.component.iotdb.IoTDBSessionConfiguration;
import com.github.alessandrofrenna.camel.component.iotdb.IoTDBSubscriptionComponent;
import com.github.alessandrofrenna.camel.test.infra.iotdb.services.IoTDBService;
import com.github.alessandrofrenna.camel.test.infra.iotdb.services.IoTDBServiceFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.camel.CamelContext;
import org.apache.camel.support.PropertyBindingSupport;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.apache.camel.test.junit5.TestSupport;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IoTDBTestSupport extends CamelTestSupport {

    @RegisterExtension
    public static IoTDBService service = IoTDBServiceFactory.createService();

    private static final Properties properties = new Properties();
    private static final String TEST_OPTIONS_PROPERTIES = "/iotdb.properties";

    static {
        loadProperties();
    }

    private static void loadProperties() {
        TestSupport.loadExternalPropertiesQuietly(properties, IoTDBTestSupport.class, TEST_OPTIONS_PROPERTIES);
    }

    @BeforeAll
    public static void createTimeseriesAndTopic() {
        try (Session session = new Session(service.host(), service.port(), "root", "root")) {
            session.open();
            session.setStorageGroup("root.test");
            session.createTimeseries(
                    "root.test.demo_device.rain", TSDataType.DOUBLE, TSEncoding.GORILLA_V1, CompressionType.ZSTD);

        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private IoTDBSessionConfiguration sessionCfg;

    public void doInSession(SessionFnConsumer sessionConsumer) {
        try (SubscriptionSession session = getSession()) {
            session.open();
            sessionConsumer.accept(session);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected CamelContext createCamelContext() throws Exception {
        CamelContext context = super.createCamelContext();

        Map<String, Object> options = getIoTDBSessionCfgMap();

        IoTDBSubscriptionComponent comp = new IoTDBSubscriptionComponent();
        PropertyBindingSupport.bindProperties(context, comp, options);
        context.addComponent("iotdb-subscription", comp);

        return context;
    }

    private Map<String, Object> getIoTDBSessionCfgMap() {
        Map<String, Object> options = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            options.put(entry.getKey().toString(), entry.getValue().toString().isEmpty() ? "value" : entry.getValue());
        }
        options.put("host", service.host());
        options.put("port", service.port());

        sessionCfg = new IoTDBSessionConfiguration(
                options.get("host").toString(),
                (int) options.get("port"),
                options.get("user").toString(),
                options.get("password").toString());

        return options;
    }

    private SubscriptionSession getSession() {
        return new SubscriptionSession(
                sessionCfg.host(),
                sessionCfg.port(),
                sessionCfg.user(),
                sessionCfg.password(),
                SessionConfig.DEFAULT_MAX_FRAME_SIZE);
    }

    @AfterAll
    void shutdownService() {
        service.shutdown();
    }

    //    void testCreateAndConsume() throws Exception {
    //        // 5)  Insert a record directly via IoTDB Session
    //                try (Session session = new Session("localhost", 6667, "root", "root")) {
    //                    session.open();
    //                    session.setStorageGroup("root.test");
    //                    session.createTimeseries(
    //                            "root.test.demo_device.rain", TSDataType.DOUBLE, TSEncoding.GORILLA_V1,
    //         CompressionType.ZSTD);
    //
    //                    session.insertRecord(
    //                            "root.test.demo_device",
    //                            Instant.now().toEpochMilli(),
    //                            List.of("rain"),
    //                            List.of(TSDataType.DOUBLE),
    //                            List.of(10));
    //                }
    //
    //        // 6)  Assert that the consumer route got at least one Exchange
    //         MockEndpoint mock = getMockEndpoint("mock:result");
    //         mock.expectedMinimumMessageCount(1);
    //         mock.assertIsSatisfied(10_000);
    //    }
}
