package com.github.alessandrofrenna.camel.component.iotdb;

import java.util.Optional;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriParams;

/** The <b>IoTDBTopicProducerConfiguration</b> class keeps track of the uri parameters used by the producer. */
@UriParams
public class IoTDBTopicProducerConfiguration {

    @UriParam(description = "Action to execute. It can either be create or update")
    @Metadata(required = true, enums = "create,delete")
    private String action;

    @UriParam(description = "The path associated to the created topic")
    @Metadata(enums = "create,delete")
    private String path;

    public IoTDBTopicProducerConfiguration() {}

    /**
     * Get the action passed as route uri parameter.</br> The value of action is required. It must be one of "create" or
     * "delete".</br> The "create" action will be used to create a topic at a given by {@link #getPath()}.</br> The
     * "delete" action will delete the topic.
     *
     * @return the action to apply to the topic
     */
    public String getAction() {
        return action;
    }

    /**
     * Set the action passed as route uri parameter.
     *
     * @param action is a string that could either be "create" or "delete"
     */
    public void setAction(String action) {
        this.action = action;
    }

    /**
     * Get the timeseries path passed as route uri parameter.</br> The value of the path is required only for the
     * "create" action. When action is "delete" this parameter, if passed is ignored.
     *
     * @return an optional wrapping the timeseries path
     */
    public Optional<String> getPath() {
        return Optional.ofNullable(path);
    }

    /**
     * Set the timeseries path passed as route uri parameter.
     *
     * @param path is a string that represent a valid IoTDB timeseries path
     */
    public void setPath(String path) {
        this.path = path;
    }
}
