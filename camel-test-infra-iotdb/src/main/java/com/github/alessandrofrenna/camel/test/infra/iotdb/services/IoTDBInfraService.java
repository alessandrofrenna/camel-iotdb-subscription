package com.github.alessandrofrenna.camel.test.infra.iotdb.services;

import org.apache.camel.test.infra.common.services.InfrastructureService;

public interface IoTDBInfraService extends InfrastructureService {
  String host();

  int port();

  default String getServiceAddress() {
    return String.format("%s:%d", host(), port());
  }
}
