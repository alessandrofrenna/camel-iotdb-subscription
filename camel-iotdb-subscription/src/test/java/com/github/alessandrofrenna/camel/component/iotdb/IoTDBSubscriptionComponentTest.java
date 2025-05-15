package com.github.alessandrofrenna.camel.component.iotdb;

import com.github.alessandrofrenna.camel.test.infra.iotdb.services.IoTDBService;
import com.github.alessandrofrenna.camel.test.infra.iotdb.services.IoTDBServiceFactory;
import java.time.Instant;
import java.util.List;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.apache.iotdb.session.Session;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;

public class IoTDBSubscriptionComponentTest extends CamelTestSupport {

  @RegisterExtension public static IoTDBService service = IoTDBServiceFactory.createService();

  @AfterEach
  void shutdownService() {
    service.shutdown();
  }

  @Override
  protected CamelContext createCamelContext() throws Exception {
    CamelContext context = super.createCamelContext();
    IoTDBSubscriptionComponent comp = new IoTDBSubscriptionComponent();
    context.addComponent("iotdb-subscription", comp);
    return context;
  }

  @Override
  protected RouteBuilder createRouteBuilder() throws Exception {
    return new RouteBuilder() {
      public void configure() {
        from("iotdb-subscription:rain_topic?groupId=test_group&consumerId=test_consumer")
            .to("mock:result");
      }
    };
  }

  // @Test
  void testCreateAndConsume() throws Exception {
    // template.sendBody("iotdb-subscription:rain_topic?action=create", null);

    // 5)  Insert a record directly via IoTDB Session
    try (Session session = new Session("localhost", 6667, "root", "root")) {
      session.open();
      session.setStorageGroup("root.test");
      session.createTimeseries(
          "root.test.demo_device.rain",
          TSDataType.DOUBLE,
          TSEncoding.GORILLA_V1,
          CompressionType.ZSTD);
      // insert one data point at timestamp=1, value=42
      session.insertRecord(
          "root.test.demo_device",
          Instant.now().toEpochMilli(),
          List.of("rain"),
          List.of(TSDataType.DOUBLE),
          List.of(10));
    }

    // 6)  Assert that the consumer route got at least one Exchange
    MockEndpoint mock = getMockEndpoint("mock:result");
    mock.expectedMinimumMessageCount(1);
    mock.assertIsSatisfied(10_000);
  }
}
