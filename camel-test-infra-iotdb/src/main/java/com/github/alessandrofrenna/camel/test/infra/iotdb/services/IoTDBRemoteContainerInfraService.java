package com.github.alessandrofrenna.camel.test.infra.iotdb.services;

import static com.github.alessandrofrenna.camel.test.infra.iotdb.common.IoTDBProperties.DEFAULT_PORT;
import static com.github.alessandrofrenna.camel.test.infra.iotdb.common.IoTDBProperties.IOTDB_HOST;
import static com.github.alessandrofrenna.camel.test.infra.iotdb.common.IoTDBProperties.IOTDB_PORT;

public class IoTDBRemoteContainerInfraService implements IoTDBInfraService {

    @Override
    public String host() {
        return System.getProperty(IOTDB_HOST);
    }

    @Override
    public int port() {
        String port = System.getProperty(IOTDB_PORT);

        if (port == null) {
            return DEFAULT_PORT;
        }

        return Integer.parseInt(port);
    }

    @Override
    public void registerProperties() {
        // NO-OP
    }

    @Override
    public void initialize() {
        registerProperties();
    }

    @Override
    public void shutdown() {
        // NO-OP
    }
}
