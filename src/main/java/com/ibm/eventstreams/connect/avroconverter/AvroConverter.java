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
    private JsonConverter jsonConverter;
    private static final Logger logger = LoggerFactory.getLogger(AvroConverter.class);
    
    public AvroConverter() {
        jsonConverter = new JsonConverter();
    }
    
    @Override
    public void close() {
        jsonConverter.close();
    }

    @Override
    public ConfigDef config() {
        return jsonConverter.config();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        jsonConverter.configure(configs);

    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        jsonConverter.configure(configs, isKey);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        logger.warn(topic);
        logger.warn(schema.toString());
        logger.warn(value.toString());
        byte[] bytes = jsonConverter.fromConnectData(topic, schema, value);
        logger.warn(bytes.toString());
        return bytes;
    }
//
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
        return jsonConverter.toConnectHeader(topic, headerKey, value);
    }

    @Override
    public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
        return jsonConverter.fromConnectHeader(topic, headerKey, schema, value);
    }
}
