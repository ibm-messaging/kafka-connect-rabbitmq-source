package com.ibm.eventstreams.connect.rabbitmqsource.schema;

import com.google.common.collect.ImmutableMap;
import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.LongString;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HeaderSchema {

    private static final Logger log = LoggerFactory.getLogger(HeaderSchema.class);

    static final Schema SCHEMA;
    static {
        SchemaBuilder builder = SchemaBuilder.struct()
                .name("HEADER VALUE: ")
                .doc("Used to store the value of a header value.")
                .field("type", SchemaBuilder.string().doc("Used to define the type for the HeaderValue.").build()
                )
                .field("timestamp", Timestamp.builder().optional().doc("Storage for when the `type` field is set to `timestamp`. Null otherwise.").build())
                .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().doc("Storage for when the `type` field is set to `array`. Null otherwise.").build());

        for (Schema.Type v : Schema.Type.values()) {
            if (Schema.Type.ARRAY == v || Schema.Type.MAP == v || Schema.Type.STRUCT == v) {
                continue;
            }
            final String doc = String.format("Storage for when the `type` field is set to `%s`. Null otherwise.", v.name().toLowerCase());

            Schema fieldSchema = SchemaBuilder.type(v)
                    .doc(doc)
                    .optional()
                    .build();
            builder.field(v.name().toLowerCase(), fieldSchema);
        }

        SCHEMA = builder.build();
    }

    static final Map<Class<?>, String> FIELD_LOOKUP;
    static {
        Map<Class<?>, String> fieldLookup = new HashMap<>();
        fieldLookup.put(String.class, Schema.Type.STRING.name().toLowerCase());
        fieldLookup.put(Byte.class, Schema.Type.INT8.name().toLowerCase());
        fieldLookup.put(Short.class, Schema.Type.INT16.name().toLowerCase());
        fieldLookup.put(Integer.class, Schema.Type.INT32.name().toLowerCase());
        fieldLookup.put(Long.class, Schema.Type.INT64.name().toLowerCase());
        fieldLookup.put(Float.class, Schema.Type.FLOAT32.name().toLowerCase());
        fieldLookup.put(Double.class, Schema.Type.FLOAT64.name().toLowerCase());
        fieldLookup.put(Boolean.class, Schema.Type.BOOLEAN.name().toLowerCase());
        fieldLookup.put(ArrayList.class, Schema.Type.ARRAY.name().toLowerCase());
        fieldLookup.put(Date.class, "timestamp");
        FIELD_LOOKUP = ImmutableMap.copyOf(fieldLookup);
    }

    static Map<String, Struct> toStructMap(BasicProperties basicProperties) {
        Map<String, Object> input = basicProperties.getHeaders();
        Map<String, Struct> results = new LinkedHashMap<>();
        if (null != input) {
            for (Map.Entry<String, Object> kvp : input.entrySet()) {
                log.trace("headers() - key = '{}' value= '{}'", kvp.getKey(), kvp.getValue());
                final String field;
                final Object headerValue;

                if (kvp.getValue() instanceof LongString) {
                    headerValue = kvp.getValue().toString();
                } else if (kvp.getValue() instanceof List) {
                    final List<LongString> list = (List<LongString>) kvp.getValue();
                    final List<String> values = new ArrayList<>(list.size());
                    for (LongString l : list) {
                        values.add(l.toString());
                    }
                    headerValue = values;
                } else {
                    headerValue = kvp.getValue();
                }

                if (!FIELD_LOOKUP.containsKey(headerValue.getClass())) {
                    throw new DataException(
                            String.format("Could not determine the type for field '%s' type '%s'", kvp.getKey(), headerValue.getClass().getName())
                    );
                } else {
                    field = FIELD_LOOKUP.get(headerValue.getClass());
                }

                log.trace("headers() - Storing value for header in field = '{}' as {}", field, field);

                Struct value = new Struct(SCHEMA)
                        .put("type", field)
                        .put(field, headerValue);
                results.put(kvp.getKey(), value);
            }
        }
        return results;
    }

}
