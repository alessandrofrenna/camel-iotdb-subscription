package com.github.alessandrofrenna.camel.component.iotdb;

import java.util.Map;
import org.apache.camel.Endpoint;
import org.apache.camel.spi.annotations.Component;
import org.apache.camel.support.HealthCheckComponent;

/**
 * This is the <b>IoTDBSubscriptionComponent</b> extends the HealthCheckComponent. The component
 * allows the integration between IoTDB data subscription API and apache camel
 */
@Component("iotdb-subscription")
public class IoTDBSubscriptionComponent extends HealthCheckComponent {

  protected Endpoint createEndpoint(String uri, String topic, Map<String, Object> parameters)
      throws Exception {
    final IoTDBTopicConsumerConfiguration consumerCfg = new IoTDBTopicConsumerConfiguration();
    final IoTDBTopicProducerConfiguration producerCfg = new IoTDBTopicProducerConfiguration();

    IoTDBTopicEndpoint endpoint = new IoTDBTopicEndpoint(uri, this, consumerCfg, producerCfg);
    setProperties(endpoint, parameters);
    endpoint.setTopic(topic);

    return endpoint;
  }
}
