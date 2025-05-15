package com.github.alessandrofrenna.camel.component.iotdb;

import org.apache.camel.Category;
import org.apache.camel.Component;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriPath;
import org.apache.camel.support.DefaultEndpoint;

/** The <b>IoTDBTopicEndpoint</b> is used to define methods to */
@UriEndpoint(
    firstVersion = "1.0.0-SNAPSHOT",
    scheme = "iotdb-subscription",
    title = "IoTDBSubscription",
    syntax = "iotdb-subscription:topic",
    category = {Category.IOT})
public class IoTDBTopicEndpoint extends DefaultEndpoint {

  @UriPath(description = "The IoTDB topic name")
  @Metadata(
      title = "IoTDB topic name",
      description =
          "A topic is a channel that emit message when a data point is added to an IoTDB timeseries",
      required = true)
  private String topic;

  @UriParam private final IoTDBTopicConsumerConfiguration consumerCfg;

  @UriParam private final IoTDBTopicProducerConfiguration producerCfg;

  public IoTDBTopicEndpoint(
      String uri,
      Component component,
      IoTDBTopicConsumerConfiguration consumerCfg,
      IoTDBTopicProducerConfiguration producerCfg) {
    super(uri, component);
    this.consumerCfg = consumerCfg;
    this.producerCfg = producerCfg;
  }

  /**
   * Create a {@link IoTDBTopicProducer}. It will be used to create IoTDB topics with apache camel.
   *
   * @return an instance of {@link IoTDBTopicProducer}
   */
  @Override
  public Producer createProducer() {
    return new IoTDBTopicProducer(this);
  }

  /**
   * Create a {@link IoTDBTopicConsumer}. It will be used to create IoTDB topic subscribers that
   * listens for new messages.
   *
   * @param processor the given processor
   * @return an instance of {@link IoTDBTopicConsumer}
   * @throws Exception if the creation of the consumer fails
   */
  @Override
  public Consumer createConsumer(Processor processor) throws Exception {
    final IoTDBTopicConsumer consumer = new IoTDBTopicConsumer(this, processor);
    configureConsumer(consumer);
    return consumer;
  }

  /**
   * Get the topic name provided in the route path.
   *
   * @return a string containing the IoTDB topic name
   */
  public String getTopic() {
    return topic;
  }

  /**
   * Set the topic name provided in the route path.
   *
   * @param topic subject of the subscription
   */
  public void setTopic(String topic) {
    this.topic = topic;
  }

  protected IoTDBTopicConsumerConfiguration getConsumerCfg() {
    return consumerCfg;
  }

  protected IoTDBTopicProducerConfiguration getProducerCfg() {
    return producerCfg;
  }
}
