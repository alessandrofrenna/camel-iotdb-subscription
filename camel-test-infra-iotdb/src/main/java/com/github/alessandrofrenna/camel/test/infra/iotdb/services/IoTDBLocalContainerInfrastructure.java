package com.github.alessandrofrenna.camel.test.infra.iotdb.services;

import static com.github.alessandrofrenna.camel.test.infra.iotdb.common.IoTDBProperties.DEFAULT_PORT;
import static com.github.alessandrofrenna.camel.test.infra.iotdb.common.IoTDBProperties.IOTDB_HOST;
import static com.github.alessandrofrenna.camel.test.infra.iotdb.common.IoTDBProperties.IOTDB_PORT;
import static com.github.alessandrofrenna.camel.test.infra.iotdb.common.IoTDBProperties.IOTDB_SERVICE_ADDRESS;

import org.apache.camel.spi.annotations.InfraService;
import org.apache.camel.test.infra.common.services.ContainerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InfraService(
    service = IoTDBInfraService.class,
    description = "IoTDB is a database used to efficiently store data from iot devices",
    serviceAlias = {
      "iotdb",
      "iotdb-service",
    })
public class IoTDBLocalContainerInfrastructure
    implements IoTDBInfraService, ContainerService<IoTDBContainer> {
  private static final Logger LOG =
      LoggerFactory.getLogger(IoTDBLocalContainerInfrastructure.class);

  private final IoTDBContainer container;

  public IoTDBLocalContainerInfrastructure() {
    container = new IoTDBContainer();
  }

  public IoTDBLocalContainerInfrastructure(String imageName) {
    container = IoTDBContainer.initContainer(imageName, IoTDBContainer.CONTAINER_NAME);
  }

  @Override
  public void registerProperties() {
    System.setProperty(IOTDB_SERVICE_ADDRESS, getServiceAddress());
    System.setProperty(IOTDB_HOST, host());
    System.setProperty(IOTDB_PORT, String.valueOf(port()));
  }

  @Override
  public void initialize() {
    LOG.info("Trying to start IoTDB container");
    container.start();
    registerProperties();
    LOG.info("IoTDB instance running at {}", getServiceAddress());
  }

  @Override
  public void shutdown() {
    LOG.info("Stopping IoTDB container");
    container.stop();
  }

  @Override
  public IoTDBContainer getContainer() {
    return container;
  }

  @Override
  public String host() {
    return container.getHost();
  }

  @Override
  public int port() {
    return container.getMappedPort(DEFAULT_PORT);
  }
}
