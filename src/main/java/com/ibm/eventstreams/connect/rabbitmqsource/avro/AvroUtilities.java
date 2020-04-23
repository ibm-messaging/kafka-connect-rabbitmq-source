package com.ibm.eventstreams.connect.rabbitmqsource.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class AvroUtilities extends AvroConverter {
    protected static Logger logger = LoggerFactory.getLogger(AvroUtilities.class);

    public boolean validateAvroSchema(String schemaFilePath) {
        String validSchemaPath = new File(schemaFilePath).getAbsolutePath();

        AvroConverter stv = new AvroConverter();
        Map<String, Object> settings = new HashMap<String, Object>();
        settings.put("schema.path", validSchemaPath);

        return stv.validateSchema(settings, false);
    }

    public byte[] deserialize (String schemaFilePath) {
        String validSchemaPath = new File(schemaFilePath).getAbsolutePath();

        AvroConverter stv = new AvroConverter();
        Map<String, Object> settings = new HashMap<String, Object>();
        settings.put("schema.path", validSchemaPath);

        if(stv.validateSchema(settings, false)){

            // TODO: For testing only add these as params to this method.
            Schema userSchema = SchemaBuilder.struct()
                .name("Users")
                .field("name", Schema.STRING_SCHEMA)
                .field("id", Schema.INT16_SCHEMA)
                .field("phone", Schema.STRING_SCHEMA)
                .build();

            Struct userStruct = new Struct(userSchema)
                    .put("name", "Scott")
                    .put("id", "8")
                    .put("phone", "(888) 555-9191");

            byte[] result = stv.fromConnectData("kafka_test", userSchema, userStruct);

            // The way avro works - the resulting byte array isn't deterministic - so we need
            // to read it back using the avro tools.
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            GenericRecord instance = null;
            try (
                    SeekableByteArrayInput sbai = new SeekableByteArrayInput(result);
                    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(sbai, datumReader);
            ) {
                instance = dataFileReader.next();
                logger.warn("DESERIALIZE = " + String.valueOf(instance));

            } catch (IOException ioe) {
                logger.warn("DFailed to deserialize Avro data", ioe);
            }

            return result;
        }

        return new byte[0];
    }

    public SchemaAndValue serialize(String schemaFilePath, byte[] byteData) {

        String validSchemaPath = new File(schemaFilePath).getAbsolutePath();

        AvroConverter stv = new AvroConverter();
        Map<String, Object> settings = new HashMap<String, Object>();
        settings.put("schema.path", validSchemaPath);
        stv.configure(settings, false);

        SchemaAndValue results = stv.toConnectData("kafka_test", byteData);
        logger.warn("SERIALIZE = " + results);
        return results;
    }

}