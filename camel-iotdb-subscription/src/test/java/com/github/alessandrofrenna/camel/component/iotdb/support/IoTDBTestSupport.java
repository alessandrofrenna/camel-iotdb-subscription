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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.support.PropertyBindingSupport;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.apache.camel.test.junit5.TestSupport;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.subscription.ISubscriptionTreeSession;
import org.apache.iotdb.session.subscription.SubscriptionTreeSessionBuilder;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
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
            doInSubscriptionSession((ISubscriptionTreeSession session) -> {
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
            doInSession((ISession session) -> {
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

    protected void dropTimeseriesPathQuietly(String timeSeriesPath) {
        try {
            doInSession(session -> {
                session.deleteTimeseries(timeSeriesPath);
                LOG.info("Timeseries with path {} removed", timeSeriesPath);
            });
        } catch (RuntimeException e) {
            LOG.error("Error deleting timeseries {}: {}", timeSeriesPath, e.getMessage());
        }
    }

    protected void generateDataPoints(String timeSeriesPath, int size, double min, double max) {
        final int lastDotIndex = timeSeriesPath.lastIndexOf(".");
        final String devicePath = timeSeriesPath.substring(0, lastDotIndex);
        final String measureVariable = timeSeriesPath.substring(lastDotIndex + 1);

        List<IMeasurementSchema> schemaList = new ArrayList<>();
        schemaList.add(new MeasurementSchema(measureVariable, TSDataType.DOUBLE));
        Tablet tablet = new Tablet(devicePath, schemaList, size);

        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            int rowIndex = tablet.getRowSize();
            tablet.addTimestamp(rowIndex, timestamp++);
            double value = min;
            if (min < max) {
                value = ThreadLocalRandom.current().nextDouble(min, max == Double.MAX_VALUE ? max : Math.nextUp(max));
            }
            tablet.addValue(schemaList.get(0).getMeasurementName(), rowIndex, value);
        }

        try {
            doInSession(session -> {
                if (tablet.getRowSize() != 0) {
                    session.insertTablet(tablet);
                    LOG.info(
                            "Added {} data points for {} {}: {}",
                            tablet.getRowSize(),
                            devicePath,
                            tablet.getSchemas(),
                            tablet.getValues());
                }
                tablet.reset();
            });
        } catch (RuntimeException e) {
            LOG.warn("Error adding data point fot timeseries {}: {}", timeSeriesPath, e.getMessage());
        }
    }

    protected void assertFromExchange(Exchange exchange, String topicName, long timeseriesCount) {
        SubscriptionMessage message = exchange.getIn().getBody(SubscriptionMessage.class);
        assertNotNull(message);
        assertEquals(topicName, message.getCommitContext().getTopicName());
        var rowCount = 0;
        for (var sessionDataSet : message.getSessionDataSetsHandler()) {
            while (sessionDataSet.hasNext()) {
                RowRecord rowRecord = sessionDataSet.next();
                System.out.println(rowRecord);
                rowCount++;
            }
        }
        assertTrue(rowCount >= timeseriesCount);
    }

    public void doInSession(SessionFnConsumer sessionConsumer) {
        try (ISession session = getSession()) {
            session.open();
            sessionConsumer.accept(session);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void doInSubscriptionSession(SubscriptionSessionFnConsumer sessionConsumer) {
        try (ISubscriptionTreeSession session = getSubscriptionSession()) {
            session.open();
            sessionConsumer.accept(session);
        } catch (Exception e) {
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

    private ISubscriptionTreeSession getSubscriptionSession() {
        return new SubscriptionTreeSessionBuilder()
                .host(sessionCfg.host())
                .port(sessionCfg.port())
                .username(sessionCfg.user())
                .password(sessionCfg.password())
                .build();
    }

    private ISession getSession() {
        return new Session.Builder()
                .host(sessionCfg.host())
                .port(sessionCfg.port())
                .username(sessionCfg.user())
                .password(sessionCfg.password())
                .build();
    }
}
