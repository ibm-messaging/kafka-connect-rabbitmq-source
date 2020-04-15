package com.ibm.eventstreams.connect.rabbitmqsource.config;

import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class RabbitMQSourceConnectorConfig extends RabbitMQConnectorConfig {

    public final String kafkaTopic;
    public static final String CONFIG_NAME_DESTINATION_KAFKA_TOPIC = "kafka.topic";
    static final String CONFIG_DOCUMENTATION_DESTINATION_KAFKA_TOPIC = "Destination Kafka topic where messages are written.";

    public final List<String> queues;
    public static final String CONFIG_NAME_SOURCE_RABBITMQ_QUEUES = "rabbitmq.queue";
    static final String CONFIG_DOCUMENTATION_SOURCE_RABBITMQ_QUEUE = "Source RabbitMQ queue where messages are pulled.";

    public final int prefetchCount;
    public static final String CONFIG_NAME_RABBITMQ_PREFETCH_COUNT = "rabbitmq.prefetch.count";
    static final String CONFIG_DOCUMENTATION_RABBITMQ_PREFETCH_COUNT = "Maximum number of messages that the server will deliver (0 for unlimited).";

    public final boolean prefetchGlobal;
    public static final String CONFIG_NAME_RABBITMQ_PREFETCH_GLOBAL = "rabbitmq.prefetch.global";
    static final String CONFIG_DOCUMENT_RABBITMQ_PREFETCH_GLOBAL = "True if the settings should be applied to the entire channel rather than to each consumer.";

    public RabbitMQSourceConnectorConfig(Map<String, String> settings) {
        super(config(), settings);

        this.kafkaTopic = this.getString(CONFIG_NAME_DESTINATION_KAFKA_TOPIC);
        this.queues = this.getList(CONFIG_NAME_SOURCE_RABBITMQ_QUEUES);
        this.prefetchCount = this.getInt(CONFIG_NAME_RABBITMQ_PREFETCH_COUNT);
        this.prefetchGlobal = this.getBoolean(CONFIG_NAME_RABBITMQ_PREFETCH_GLOBAL);
    }

    public static ConfigDef config() {
        ConfigDef config = RabbitMQConnectorConfig.config();

        config.define(CONFIG_NAME_DESTINATION_KAFKA_TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, CONFIG_DOCUMENTATION_DESTINATION_KAFKA_TOPIC);
        config.define(CONFIG_NAME_SOURCE_RABBITMQ_QUEUES, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, CONFIG_DOCUMENTATION_SOURCE_RABBITMQ_QUEUE);
        config.define(CONFIG_NAME_RABBITMQ_PREFETCH_COUNT, ConfigDef.Type.INT, 100, ConfigDef.Importance.MEDIUM, CONFIG_DOCUMENTATION_RABBITMQ_PREFETCH_COUNT);
        config.define(CONFIG_NAME_RABBITMQ_PREFETCH_GLOBAL, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, CONFIG_DOCUMENT_RABBITMQ_PREFETCH_GLOBAL);

        return config;
    }
}
