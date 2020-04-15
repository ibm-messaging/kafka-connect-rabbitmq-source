package com.ibm.eventstreams.connect.rabbitmqsource;

import com.ibm.eventstreams.connect.rabbitmqsource.config.RabbitMQSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RabbitMQSourceConnector extends SourceConnector {
    RabbitMQSourceConnectorConfig config;

    public static String VERSION = "1.0.0";

    private Map<String, String> props;

    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a String
     */
    @Override public String version() {
        return VERSION;
    }

    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param props configuration settings
     */
    @Override public void start(Map<String, String> props) {
        this.config = new RabbitMQSourceConnectorConfig(props);
        this.props = props;
    }

    /**
     * Returns the Task implementation for this Connector.
     */
    @Override public Class<? extends Task> taskClass() {
        return RabbitMQSourceTask.class;
    }

    /**
     * Returns a set of configurations for Tasks based on the current configuration,
     * producing at most count configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for Tasks
     */
    @Override public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Collections.nCopies(maxTasks, this.props);
    }

    /**
     * Stop this connector.
     */
    @Override public void stop() {

    }

    /**
     * Define the configuration for the connector.
     * @return The ConfigDef for this connector.
     */
    @Override public ConfigDef config() {
        return RabbitMQSourceConnectorConfig.config();
    }
}
