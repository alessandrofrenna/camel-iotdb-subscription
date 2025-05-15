package com.github.alessandrofrenna.camel.test.infra.iotdb.services;

import org.apache.camel.test.infra.common.services.ContainerTestService;
import org.apache.camel.test.infra.common.services.TestService;

public interface IoTDBService extends TestService, IoTDBInfraService, ContainerTestService {}
