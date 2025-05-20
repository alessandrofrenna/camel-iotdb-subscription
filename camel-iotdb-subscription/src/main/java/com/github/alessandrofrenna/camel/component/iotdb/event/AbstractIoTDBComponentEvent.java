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

package com.github.alessandrofrenna.camel.component.iotdb.event;

import java.util.EventObject;
import java.util.Objects;

import org.apache.camel.spi.CamelEvent;

/**
 * The <b>AbstractIoTDBComponentEvent</b> is a {@link CamelEvent} that extends an {@link EventObject}.</br> This
 * abstract class is shared among all concrete events
 */
public class AbstractIoTDBComponentEvent extends EventObject implements CamelEvent {
    private final String topicName;
    private long timestamp;

    public AbstractIoTDBComponentEvent(Object source, String topicName) {
        super(source);
        Objects.requireNonNull(topicName, "topicName is null");
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
                "%s[topic=%s, timestamp=%d, source=%s]",
                getClass().getSimpleName(), topicName, getTimestamp(), getSource());
    }

    public String getTopicName() {
        return topicName;
    }
}
