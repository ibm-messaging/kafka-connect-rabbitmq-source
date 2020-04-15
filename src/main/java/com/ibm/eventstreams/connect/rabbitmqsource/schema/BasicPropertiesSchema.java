package com.ibm.eventstreams.connect.rabbitmqsource.schema;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class BasicPropertiesSchema {
    private static final Logger log = LoggerFactory.getLogger(BasicPropertiesSchema.class);

    static final String FIELD_MESSAGEID = "messageId";
    static final String FIELD_CONTENTTYPE = "contentType";
    static final String FIELD_CONTENTENCODING = "contentEncoding";
    static final String FIELD_HEADERS = "headers";
    static final String FIELD_DELIVERYMODE = "deliveryMode";
    static final String FIELD_PRIORITY = "priority";
    static final String FIELD_CORRELATIONID = "correlationId";
    static final String FIELD_REPLYTO = "replyTo";
    static final String FIELD_EXPIRATION = "expiration";
    static final String FIELD_TIMESTAMP = "timestamp";
    static final String FIELD_TYPE = "type";
    static final String FIELD_USERID = "userId";
    static final String FIELD_APPID = "appId";

    static final Schema SCHEMA = SchemaBuilder.struct()
            .name("BASIC PROPERTIES: ")
            .optional()
            .doc("Corresponds to the Basic Properties.")
            .field(
                    FIELD_CONTENTTYPE,
                    SchemaBuilder.string().optional().doc("The value in the contentType field.").build()
            )
            .field(
                    FIELD_CONTENTENCODING,
                    SchemaBuilder.string().optional().doc("The value in the contentEncoding field.").build()
            )
            .field(
                    FIELD_HEADERS,
                    SchemaBuilder.map(Schema.STRING_SCHEMA, HeaderSchema.SCHEMA).build()
            )
            .field(
                    FIELD_DELIVERYMODE,
                    SchemaBuilder.int32().optional().doc("The value in the deliveryMode field.").build()
            )
            .field(
                    FIELD_PRIORITY,
                    SchemaBuilder.int32().optional().doc("The value in the priority field.").build()
            )
            .field(
                    FIELD_CORRELATIONID,
                    SchemaBuilder.string().optional().doc("The value in the correlationId field.").build()
            )
            .field(
                    FIELD_REPLYTO,
                    SchemaBuilder.string().optional().doc("The value in the replyTo field.")
            )
            .field(
                    FIELD_EXPIRATION,
                    SchemaBuilder.string().optional().doc("The value in the expiration field.").build()
            )
            .field(
                    FIELD_MESSAGEID,
                    SchemaBuilder.string().optional().doc("The value in the messageId field.").build()
            )
            .field(
                    FIELD_TIMESTAMP, Timestamp.builder().optional().doc("The value in the timestamp field. ").build()
            )
            .field(
                    FIELD_TYPE, SchemaBuilder.string().optional().doc("The value in the type field.").build()
            )
            .field(
                    FIELD_USERID,
                    SchemaBuilder.string().optional().doc("The value in the userId field.").build()
            )
            .field(
                    FIELD_APPID,
                    SchemaBuilder.string().optional().doc("The value in the appId field.").build()
            )
            .build();

    static Struct toStruct(com.rabbitmq.client.BasicProperties basicProperties) {
        if (null == basicProperties) {
            log.trace("basicProperties() - basicProperties is null.");
            return null;
        }

        Map<String, Struct> headers = HeaderSchema.toStructMap(basicProperties);
        return new Struct(SCHEMA)
                .put(FIELD_CONTENTTYPE, basicProperties.getContentType())
                .put(FIELD_CONTENTENCODING, basicProperties.getContentEncoding())
                .put(FIELD_HEADERS, headers)
                .put(FIELD_DELIVERYMODE, basicProperties.getDeliveryMode())
                .put(FIELD_PRIORITY, basicProperties.getPriority())
                .put(FIELD_CORRELATIONID, basicProperties.getCorrelationId())
                .put(FIELD_REPLYTO, basicProperties.getReplyTo())
                .put(FIELD_EXPIRATION, basicProperties.getExpiration())
                .put(FIELD_MESSAGEID, basicProperties.getMessageId())
                .put(FIELD_TIMESTAMP, basicProperties.getTimestamp())
                .put(FIELD_TYPE, basicProperties.getType())
                .put(FIELD_USERID, basicProperties.getUserId())
                .put(FIELD_APPID, basicProperties.getAppId());
    }
}
