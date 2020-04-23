package com.ibm.eventstreams.connect.rabbitmqsource.sourcerecord;

import com.google.common.collect.ImmutableMap;
import com.ibm.eventstreams.connect.rabbitmqsource.avro.AvroUtilities;
import com.ibm.eventstreams.connect.rabbitmqsource.config.RabbitMQSourceConnectorConfig;
import com.ibm.eventstreams.connect.rabbitmqsource.schema.EnvelopeSchema;
import com.ibm.eventstreams.connect.rabbitmqsource.schema.KeySchema;
import com.ibm.eventstreams.connect.rabbitmqsource.schema.ValueSchema;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

public class RabbitMQSourceRecordFactory {
    private static final Logger log = LoggerFactory.getLogger(RabbitMQSourceRecordFactory.class);

    private final RabbitMQSourceConnectorConfig config;
    private final Time time = new SystemTime();

    public RabbitMQSourceRecordFactory(RabbitMQSourceConnectorConfig config) {
        this.config = config;
    }

    protected String bytesArrayToString(byte[] bytes) {
        String payload;
        payload = new String(bytes, StandardCharsets.UTF_8);
        log.warn("*** PAYLOAD === " + payload + " === PAYLOAD ***");

        return payload;
    }

    public SourceRecord makeSourceRecord(String consumerTag, Envelope envelope, AMQP.BasicProperties basicProperties, byte[] bytes) {
        final Map<String, ?> sourcePartition = ImmutableMap.of(EnvelopeSchema.FIELD_ROUTINGKEY, envelope.getRoutingKey());
        final Map<String, ?> sourceOffset = ImmutableMap.of(EnvelopeSchema.FIELD_DELIVERYTAG, envelope.getDeliveryTag());

        final Struct key = KeySchema.toStruct(basicProperties);
        // Hereo2: Body Compose
        final Struct value = ValueSchema.toStruct(consumerTag, envelope, basicProperties, bytes);

        bytesArrayToString(bytes);

        AvroUtilities vs = new AvroUtilities();
        Boolean isValidSchema = vs.validateAvroSchema("avrodata/users-schema.avsc");

        AvroUtilities is = new AvroUtilities();
        Boolean isInvalidSchema = is.validateAvroSchema("avrodata/invalid-schema.avsc");

        log.warn("*** isValidSchema === " + isValidSchema + " === isValidSchema ***");
        log.warn("*** isInvalidSchema === " + isInvalidSchema + " === isInvalidSchema ***");

        //AvroUtilities DS = new AvroUtilities();
        //byte[] deserialize = DS.deserialize("avrodata/users-schema.avsc");

        //AvroUtilities SL = new AvroUtilities();
        //SchemaAndValue serialize = SL.serialize("avrodata/users-schema.avsc", bytes);

        //log.warn("*** deserialize === " + deserialize + " === deserialize ***");
        //log.warn("*** serialize === " + serialize + " === serialize ***");

        final String topic = this.config.kafkaTopic;

        long timestamp = Optional.ofNullable(basicProperties.getTimestamp()).map(Date::getTime).orElse(this.time.milliseconds());

        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                topic,
                null,
                key.schema(),
                key,
                value.schema(),
                value,
                timestamp
        );
    }
}