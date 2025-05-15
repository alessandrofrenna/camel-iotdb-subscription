package com.github.alessandrofrenna.camel.test.infra.iotdb.services;

import org.apache.camel.test.infra.common.services.SimpleTestServiceBuilder;

public class IoTDBServiceFactory {

    private IoTDBServiceFactory() {}

    public static SimpleTestServiceBuilder<IoTDBService> builder() {
        return new SimpleTestServiceBuilder<>("iotdb");
    }

    public static IoTDBService createService() {
        return builder()
                .addLocalMapping(IoTDBLocalContainerService::new)
                .addRemoteMapping(IoTDBRemoteContainerService::new)
                .build();
    }

    public static class IoTDBRemoteContainerService extends IoTDBRemoteContainerInfraService implements IoTDBService {}

    public static class IoTDBLocalContainerService extends IoTDBLocalContainerInfrastructure implements IoTDBService {}
}
