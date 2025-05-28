# Apache camel IoTDB Data subscription component

#### Since Camel 4.11.0
### Both producer and consumer are supported

The IoTDB Subscription component is used to subscribe to topic inside IoTDB.</br>
At the moment only IoTDB v1.3.4 is supported. Newer version will be added soon.

# Licensing
This project is licensed under the [Apache License v2.0](https://www.apache.org/licenses/LICENSE-2.0).

This product includes software developed at
[The Apache Software Foundation](https://www.apache.org/) such as:
1. [Apache Camel](https://camel.apache.org/).
2. [Apache IoTDB](https://iotdb.apache.org/).

Testate
IoTDBTopicProducer
IoTDBTopicConsumer
IoTDBTopicManager
IoTDBTopicConsumerManager
IoTDBRouteRegistry
IoTDBSubscriptionEventListener

Da non testare sono:
IoTDBSessionConfiguration
IoTDBTopicConsumerConfiguration
IoTDBTopicProducerConfiguration
TopicAwareConsumerListener
IoTDBSubscriptionComponent
IoTDTopicEndpoint

Rimangono da testare
RoutedConsumerListener