package com.ibm.eventstreams.connect.rabbitmqsource.schema;

import com.rabbitmq.client.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class EnvelopeSchema {
    public static final String FIELD_ROUTINGKEY = "routingKey";
    public static final String FIELD_DELIVERYTAG = "deliveryTag";

    static final String FIELD_ISREDELIVER = "isRedeliver";
    static final String FIELD_EXCHANGE = "exchange";

    static final Schema SCHEMA = SchemaBuilder.struct()
            .name("KAFKA CONNECT ENVELOPE: ")
            .doc("Encapsulates a group of parameters used for AMQP's Basic methods.")
            .field(FIELD_DELIVERYTAG, SchemaBuilder.int64().doc("Envelope Delivery Tag: ").build())
            .field(FIELD_ISREDELIVER, SchemaBuilder.bool().doc("Envelope Redeliver: ").build())
            .field(FIELD_EXCHANGE, SchemaBuilder.string().optional().doc("Envelope Exchange: "))
            .field(FIELD_ROUTINGKEY, SchemaBuilder.string().optional().doc("Envelope Routing Key: ").build())
            .build();

    static Struct toStruct(Envelope envelope) {
        return new Struct(SCHEMA)
                .put(FIELD_DELIVERYTAG, envelope.getDeliveryTag())
                .put(FIELD_ISREDELIVER, envelope.isRedeliver())
                .put(FIELD_EXCHANGE, envelope.getExchange())
                .put(FIELD_ROUTINGKEY, envelope.getRoutingKey());
    }

}
