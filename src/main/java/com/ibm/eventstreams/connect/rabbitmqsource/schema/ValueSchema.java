package com.ibm.eventstreams.connect.rabbitmqsource.schema;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class ValueSchema {
    static final String FIELD_MESSAGE_CONSUMERTAG = "consumerTag";
    static final String FIELD_MESSAGE_ENVELOPE = "envelope";
    static final String FIELD_MESSAGE_BASICPROPERTIES = "basicProperties";
    static final String FIELD_MESSAGE_BODY = "body";

    static final Schema SCHEMA = SchemaBuilder.struct()
            .field(FIELD_MESSAGE_BASICPROPERTIES, BasicPropertiesSchema.SCHEMA)
            .field(FIELD_MESSAGE_BODY, SchemaBuilder.string().build())
            .build();

    public static Struct toStruct(String consumerTag, Envelope envelope, AMQP.BasicProperties basicProperties, byte[] body) {
        String bodyString = new String(body);
        return new Struct(SCHEMA)
                .put(FIELD_MESSAGE_BASICPROPERTIES, BasicPropertiesSchema.toStruct(basicProperties))
                .put(FIELD_MESSAGE_BODY, bodyString);
    }
}
