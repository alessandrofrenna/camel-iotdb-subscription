# Apache camel IoTDB Data subscription component


[![Run CI tests with maven](https://github.com/alessandrofrenna/camel-iotdb-subscription/actions/workflows/ci.yml/badge.svg)](https://github.com/alessandrofrenna/camel-iotdb-subscription/actions/workflows/ci.yml) [![Snyk Security](https://github.com/alessandrofrenna/camel-iotdb-subscription/actions/workflows/snyk.yml/badge.svg)](https://github.com/alessandrofrenna/camel-iotdb-subscription/actions/workflows/snyk.yml) [![](https://jitpack.io/v/alessandrofrenna/camel-iotdb-subscription.svg)](https://jitpack.io/#alessandrofrenna/camel-iotdb-subscription) 

#### Since Camel 4.11.0
### Both producer and consumer are supported

The IoTDB Subscription component is used to subscribe to topic inside IoTDB.</br>
At the moment only IoTDB v1.3.4 is supported. Newer version will be added soon.

### Maven

Enable the repository in your pom.xml:
```xml
<repositories>
    <!-- other repositories -->
    
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>
```

Add this to your dependencies:
```xml
<dependencies>
    <!-- other dependencies -->

    <dependency>
        <groupId>com.github.alessandrofrenna</groupId>
        <artifactId>camel-iotdb-subscription</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </dependency>
</dependencies>

```

If you want to use the `camel-test-infra-iotb` add this to your dependencies:

```xml
<dependencies>
    <!-- other dependencies -->
    
    <dependency>
        <groupId>com.github.alessandrofrenna</groupId>
        <artifactId>camel-test-infra-iotdb</artifactId>
        <version>1.0.0-SNAPSHOT</version>
        <scope>test</scope>
    </dependency>
    
    <dependency>
        <groupId>com.github.alessandrofrenna</groupId>
        <artifactId>camel-test-infra-iotdb</artifactId>
        <version>1.0.0-SNAPSHOT</version>
        <type>test-jar</type>
        <scope>test</scope>
    </dependency>
</dependencies>
```

# Licensing
This project is licensed under the [Apache License v2.0](https://www.apache.org/licenses/LICENSE-2.0).

This product includes software developed at
[The Apache Software Foundation](https://www.apache.org/) such as:
1. [Apache Camel](https://camel.apache.org/).
2. [Apache IoTDB](https://iotdb.apache.org/).