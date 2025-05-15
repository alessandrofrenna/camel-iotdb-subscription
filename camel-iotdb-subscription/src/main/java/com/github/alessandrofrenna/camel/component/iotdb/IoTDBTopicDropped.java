/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.alessandrofrenna.camel.component.iotdb;

import java.util.EventObject;
import org.apache.camel.spi.CamelEvent;

/**
 * The <b>IoTDBTopicDropped</b> is a {@link CamelEvent} that extends an {@link EventObject}.</br> The event if fired by
 * an {@link IoTDBTopicProducer} when {@link IoTDBTopicProducerConfiguration#getAction()} returns "drop" as value.</br>
 * The events will be handled by camel using the {@link IoTDBSubscriptionEventListener}.
 */
public class IoTDBTopicDropped extends EventObject implements CamelEvent {
    private final String topicName;
    private long timestamp;

    public IoTDBTopicDropped(Object source, String topicName) {
        super(source);
        this.topicName = topicName;
    }

    @Override
    public Type getType() {
        return Type.Custom;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return String.format(
                "IoTDBTopicDropped[topic=%s, timestamp=%d, source=%s]", topicName, getTimestamp(), getSource());
    }
}
