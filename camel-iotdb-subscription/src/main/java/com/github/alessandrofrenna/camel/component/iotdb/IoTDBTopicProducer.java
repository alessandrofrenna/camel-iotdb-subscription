package com.github.alessandrofrenna.camel.component.iotdb;

import org.apache.camel.Exchange;
import org.apache.camel.support.DefaultProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The <b>IoTDBTopicProducer</b> */
public class IoTDBTopicProducer extends DefaultProducer {

    private static final Logger LOG = LoggerFactory.getLogger(IoTDBTopicProducer.class);
    private IoTDBTopicEndpoint endpoint;

    public IoTDBTopicProducer(IoTDBTopicEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
    }

    public void process(Exchange exchange) throws Exception {
        System.out.println(exchange.getIn().getBody());
    }
}
