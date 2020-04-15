package com.ibm.eventstreams.connect.rabbitmqsource.schema;

import com.rabbitmq.client.AMQP;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class KeySchema {

    static final Schema SCHEMA = SchemaBuilder.struct()
            .name("MESSAGE KEY: ")
            .doc("Key used for partition assignment in Kafka.")
            .field(
                    BasicPropertiesSchema.FIELD_MESSAGEID,
                    SchemaBuilder.string().optional().doc("The value in the messageId field. BasicProperties.getMessageId(): ").build()
            )
            .build();

    public static Struct toStruct(AMQP.BasicProperties basicProperties) {
        return new Struct(SCHEMA)
                .put(BasicPropertiesSchema.FIELD_MESSAGEID, basicProperties.getMessageId());
    }
}