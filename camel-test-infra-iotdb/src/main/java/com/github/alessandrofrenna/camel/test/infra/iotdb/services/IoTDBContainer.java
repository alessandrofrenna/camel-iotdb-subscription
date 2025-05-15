package com.github.alessandrofrenna.camel.test.infra.iotdb.services;

import static com.github.alessandrofrenna.camel.test.infra.iotdb.common.IoTDBProperties.*;

import java.util.Optional;
import org.apache.camel.test.infra.common.LocalPropertyResolver;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class IoTDBContainer extends GenericContainer<IoTDBContainer> {
  public static final String CONTAINER_NAME = "iotdb";

  public IoTDBContainer() {
    super(
        LocalPropertyResolver.getProperty(
            IoTDBLocalContainerInfrastructure.class, IOTDB_CONTAINER));
    int port =
        Optional.of(
                LocalPropertyResolver.getProperty(
                    IoTDBLocalContainerInfrastructure.class, IOTDB_PORT))
            .map(Integer::parseInt)
            .orElse(DEFAULT_PORT);
    this.withNetworkAliases(CONTAINER_NAME)
        .withExposedPorts(port)
        .waitingFor(Wait.forListeningPort());
  }

  public IoTDBContainer(String imageName) {
    super(DockerImageName.parse(imageName));
  }

  public static IoTDBContainer initContainer(String imageName, String networkAlias) {
    int port =
        Optional.of(
                LocalPropertyResolver.getProperty(
                    IoTDBLocalContainerInfrastructure.class, IOTDB_PORT))
            .map(Integer::parseInt)
            .orElse(DEFAULT_PORT);
    return new IoTDBContainer(imageName)
        .withNetworkAliases(networkAlias)
        .withExposedPorts(port)
        .waitingFor(Wait.forListeningPort());
  }
}
