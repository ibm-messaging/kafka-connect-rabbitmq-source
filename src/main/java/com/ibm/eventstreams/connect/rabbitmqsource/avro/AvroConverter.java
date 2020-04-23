package com.ibm.eventstreams.connect.rabbitmqsource.avro;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;



public class AvroConverter implements Converter {
    private static Logger logger = LoggerFactory.getLogger(AvroConverter.class);

    private Integer schemaCacheSize = 80;
    private org.apache.avro.Schema avroSchema = null;
    private AvroData avroDataHelper = null;
    private Schema connectSchema = null;

    public boolean validateSchema(Map<String, ?> configs, boolean isKey) {
        if (configs.get("schema.cache.size") instanceof Integer) {
            schemaCacheSize = (Integer) configs.get("schema.cache.size");
        }

        avroDataHelper = new AvroData(schemaCacheSize);

        if (configs.get("schema.path") instanceof String) {
            String avroSchemaPath = (String) configs.get("schema.path");
            org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();

            File avroSchemaFile = null;
            try {
                avroSchemaFile = new File(avroSchemaPath);
                avroSchema = parser.parse(avroSchemaFile);
                connectSchema = avroDataHelper.toConnectSchema(avroSchema);
            } catch (Exception e) {
                logger.warn(" *********** ERRROR ****************: " + e);
                return false;
            }
            return true;
        }
        return false;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    // SERIALIZE
    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        DatumWriter<GenericRecord> datumWriter;
        if (avroSchema != null) {
            datumWriter = new GenericDatumWriter<GenericRecord>(avroSchema);
        } else {
            datumWriter = new GenericDatumWriter<GenericRecord>();
        }
        GenericRecord avroInstance = (GenericRecord)avroDataHelper.fromConnectData(schema, value);

        try (
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        ) {
            dataFileWriter.setCodec(CodecFactory.nullCodec());

            if (avroSchema != null) {
                dataFileWriter.create(avroSchema, baos);
            } else {
                dataFileWriter.create(avroInstance.getSchema(), baos);
            }

            dataFileWriter.append(avroInstance);
            dataFileWriter.flush();

            return baos.toByteArray();
        } catch (IOException ioe) {
            throw new DataException("Error serializing Avro", ioe);
        }
    }

    // DESERIALIZE
    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        DatumReader<GenericRecord> datumReader;
        if (avroSchema != null) {
            datumReader = new GenericDatumReader<>(avroSchema);
        } else {
            datumReader = new GenericDatumReader<>();
        }
        GenericRecord instance = null;

        try (
                SeekableByteArrayInput sbai = new SeekableByteArrayInput(value);
                DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(sbai, datumReader);
        ) {
            instance = dataFileReader.next(instance);
            if (instance == null) {
                logger.warn("Instance was null");
            }

            if (avroSchema != null) {
                return avroDataHelper.toConnectData(avroSchema, instance);
            } else {
                return avroDataHelper.toConnectData(instance.getSchema(), instance);
            }
        } catch (IOException ioe) {
            throw new DataException("Failed to deserialize Avro data from topic %s :".format(topic), ioe);
        }
    }
}