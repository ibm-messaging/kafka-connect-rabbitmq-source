package com.ibm.eventstreams.connect.rabbitmqsource.sourcerecord;

import com.google.common.collect.ImmutableMap;
import com.ibm.eventstreams.connect.rabbitmqsource.config.RabbitMQSourceConnectorConfig;
import com.ibm.eventstreams.connect.rabbitmqsource.schema.EnvelopeSchema;
import com.ibm.eventstreams.connect.rabbitmqsource.schema.KeySchema;
import com.ibm.eventstreams.connect.rabbitmqsource.schema.ValueSchema;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.LongString;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.data.Schema.BYTES_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

public class RabbitMQSourceRecordFactory {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQSourceRecordFactory.class);

    private final RabbitMQSourceConnectorConfig config;
    private final Time time = new SystemTime();

    public RabbitMQSourceRecordFactory(RabbitMQSourceConnectorConfig config) {
        this.config = config;
    }


    private Header toConnectHeader(String key, Object value) {
        return new Header() {
            @Override
            public String key() {
                System.out.println("KEY CALLED!");
                return key;
            }

            @Override
            public Schema schema() { // TODO Infer from type
                System.out.println("SCHEMA CALLED!");
                return BYTES_SCHEMA;
            }

            @Override
            public Object value() {
                System.out.println("VALUE CALLED! " + Arrays.toString(Values.convertToString(BYTES_SCHEMA, value).getBytes(StandardCharsets.UTF_8)));
                return value;
            }

            @Override
            public Header with(Schema schema, Object o) {
                System.out.println("WITH CALLED!");
                return null;
            }

            @Override
            public Header rename(String s) {
                System.out.println("RENAME CALLED!");
                return null;
            }
        };
    }

    private List<Header> toConnectHeaders(Map<String, Object> ampqHeaders) {
        List<Header> headers = new ArrayList<>();

        for (Map.Entry<String, Object> kvp : ampqHeaders.entrySet()) {
            Object headerValue = kvp.getValue();

            if (headerValue instanceof LongString) {
                headerValue = kvp.getValue().toString();
            } else if (kvp.getValue() instanceof List) {
                final List<LongString> list = (List<LongString>) headerValue;
                final List<String> values = new ArrayList<>(list.size());
                for (LongString l : list) {
                    values.add(l.toString());
                }
                headerValue = values;
            }

            Header header = toConnectHeader(kvp.getKey(), headerValue);
            headers.add(header);
        }

        return headers;
    }

    public SourceRecord makeSourceRecord(String consumerTag, Envelope envelope, AMQP.BasicProperties basicProperties, byte[] bytes) {
        final Map<String, ?> sourcePartition = ImmutableMap.of(EnvelopeSchema.FIELD_ROUTINGKEY, envelope.getRoutingKey());
        final Map<String, ?> sourceOffset = ImmutableMap.of(EnvelopeSchema.FIELD_DELIVERYTAG, envelope.getDeliveryTag());

        final Struct key = KeySchema.toStruct(basicProperties);
        final Struct value = ValueSchema.toStruct(consumerTag, envelope, basicProperties, bytes);

        List<Header> headers = toConnectHeaders(basicProperties.getHeaders());
        final String messageBody = value.getString("body");

        final String topic = this.config.kafkaTopic;

        long timestamp = Optional.ofNullable(basicProperties.getTimestamp()).map(Date::getTime).orElse(this.time.milliseconds());

        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                topic,
                null,
                key.schema(),
                key,
                STRING_SCHEMA,
                messageBody,
                timestamp,
                headers
        );
    }
}