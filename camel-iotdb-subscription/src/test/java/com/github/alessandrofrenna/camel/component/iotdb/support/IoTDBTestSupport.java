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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.camel.CamelContext;
import org.apache.camel.support.PropertyBindingSupport;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.apache.camel.test.junit5.TestSupport;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.alessandrofrenna.camel.component.iotdb.IoTDbSessionConfiguration;
import com.github.alessandrofrenna.camel.component.iotdb.IoTDbSubscriptionComponent;
import com.github.alessandrofrenna.camel.test.infra.iotdb.services.IoTDbService;
import com.github.alessandrofrenna.camel.test.infra.iotdb.services.IoTDbServiceFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IoTDBTestSupport extends CamelTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(IoTDBTestSupport.class);

    @RegisterExtension
    public static IoTDbService service = IoTDbServiceFactory.createService();

    private static final Properties properties = new Properties();
    private static final String TEST_OPTIONS_PROPERTIES = "/iotdb.properties";

    static {
        loadProperties();
    }

    private static void loadProperties() {
        TestSupport.loadExternalPropertiesQuietly(properties, IoTDBTestSupport.class, TEST_OPTIONS_PROPERTIES);
    }

    private IoTDbSessionConfiguration sessionCfg;

    @Override
    protected CamelContext createCamelContext() throws Exception {
        CamelContext context = super.createCamelContext();

        Map<String, Object> options = getIoTDBSessionCfgMap();

        IoTDbSubscriptionComponent comp = new IoTDbSubscriptionComponent();
        PropertyBindingSupport.bindProperties(context, comp, options);
        context.addComponent("iotdb-subscription", comp);

        return context;
    }

    protected void createTopicQuietly(String topicName, String path) {
        try {
            doInSession(session -> {
                Properties props = new Properties();
                props.setProperty(TopicConstant.PATH_KEY, path);
                props.setProperty(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE);
                session.createTopicIfNotExists(topicName, props);
                LOG.debug("Created topic '{}' for path: '{}'", topicName, path);
            });
        } catch (RuntimeException e) {
            LOG.warn("Error creating topic '{}' for path '{}': {}", topicName, path, e.getMessage());
        }
    }

    protected void createTimeseriesPathQuietly(String timeSeriesPath) {
        try {
            doInSession(session -> {
                if (session.checkTimeseriesExists(timeSeriesPath)) {
                    LOG.debug("Timeseries with path {} already exists", timeSeriesPath);
                    return;
                }
                session.createTimeseries(
                        timeSeriesPath, TSDataType.DOUBLE, TSEncoding.GORILLA_V1, CompressionType.ZSTD);
            });
        } catch (RuntimeException e) {
            LOG.warn("Error creating timeseries {}: {}", timeSeriesPath, e.getMessage());
        }
    }

    protected void generateDataPoints(String timeSeriesPath, int size, double min, double max) {
        final int lastDotIndex = timeSeriesPath.lastIndexOf(".");
        final String devicePath = timeSeriesPath.substring(0, lastDotIndex);
        final String measureVariable = timeSeriesPath.substring(lastDotIndex + 1);

        List<MeasurementSchema> schemaList = new ArrayList<>();
        schemaList.add(new MeasurementSchema(measureVariable, TSDataType.DOUBLE));
        Tablet tablet = new Tablet(devicePath, schemaList, size);

        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            int rowIndex = tablet.rowSize++;
            tablet.addTimestamp(rowIndex, timestamp++);
            double value = min;
            if (min < max) {
                value = ThreadLocalRandom.current().nextDouble(min, max == Double.MAX_VALUE ? max : Math.nextUp(max));
            }
            tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, value);
        }

        try {
            doInSession(session -> {
                if (tablet.rowSize != 0) {
                    session.insertTablet(tablet);
                    LOG.info(
                            "Added {} data points for {} {}: {}",
                            tablet.rowSize,
                            devicePath,
                            tablet.getSchemas(),
                            tablet.values);
                }
                tablet.reset();
            });
        } catch (RuntimeException e) {
            LOG.warn("Error adding data point fot timeseries {}: {}", timeSeriesPath, e.getMessage());
        }
    }

    public void doInSession(SessionFnConsumer sessionConsumer) {
        try (SubscriptionSession session = getSubscriptionSession()) {
            session.open();
            sessionConsumer.accept(session);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> getIoTDBSessionCfgMap() {
        Map<String, Object> options = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            options.put(entry.getKey().toString(), entry.getValue().toString().isEmpty() ? "value" : entry.getValue());
        }
        options.put("host", service.host());
        options.put("port", service.port());

        sessionCfg = new IoTDbSessionConfiguration(
                options.get("host").toString(),
                (int) options.get("port"),
                options.get("user").toString(),
                options.get("password").toString());

        return options;
    }

    private SubscriptionSession getSubscriptionSession() {
        return new SubscriptionSession(
                sessionCfg.host(),
                sessionCfg.port(),
                sessionCfg.user(),
                sessionCfg.password(),
                SessionConfig.DEFAULT_MAX_FRAME_SIZE);
    }
}
