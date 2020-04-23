package com.ibm.eventstreams.connect.avroconverter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.eventstreams.connect.rabbitmqsource.schema.HeaderSchema;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class AvroConverter implements Converter, HeaderConverter {
    private static final Logger logger = LoggerFactory.getLogger(AvroConverter.class);

    JsonConverter jsonConverter = new JsonConverter();

    @Override
    public void close() throws IOException {

    }

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public void configure(Map<String, ?> map) {

    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        return new byte[0];
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] bytes) {
        logger.warn(topic);
        logger.warn(bytes.toString());
        SchemaAndValue jsonSchemaAndValue = jsonConverter.toConnectData(topic, bytes);
        logger.warn(jsonSchemaAndValue.schema().toString());
        logger.warn(jsonSchemaAndValue.value().toString());
        return jsonSchemaAndValue;
    }

    @Override
    public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
        return null;
    }

    @Override
    public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
        return new byte[0];
    }
}
