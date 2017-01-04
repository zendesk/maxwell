package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class SchemaRegistryProducer extends AbstractKafkaProducer {
    static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegistryProducer.class);

    private final KafkaProducer<String, GenericRecord> kafka;
    private String schemaMappingURI;

    private static final String JSON_SUFFIX = ".json";

    private static final String TYPE_NULL = "null";
    private static final String TYPE_DOUBLE = "double";
    private static final String TYPE_FLOAT = "float";
    private static final String TYPE_LONG = "long";
    private static final String TYPE_STRING = "string";
    private static final String TYPE_INT = "int";
    private static final String TYPE_BOOLEAN = "boolean";
    private static final String TYPE_FIXED = "fixed";
    private static final String TYPE_BYTES = "bytes";

    // NOTE: The KafkaAvroProducer maintains a cache of known childSchemas via an IdentityHashMap.
    // Since the equals method on IdentityHashMap is based on identity, not value - like other hashmaps,
    // passing in a newly created Schema that is logically equivalent to previously passed in childSchemas
    // results in the Producer believing it has a new Schema and caching it (again).
    // As a result the Producer's cache quickly fills up. Once full the Producer raises and exception and falls down.
    // Given this behavior we must maintain our own cache of childSchemas and always pass in the same Schema object to the
    // Producer. This represents a little more overhead, but it should be tolerable - even a few hundred Schemas would
    // represent a small about of in-memory data.
    private HashMap<String, Schema> schemaCache = new HashMap<>();

    // a cache of URIs to their data
    private HashMap<String, JSONObject> resourceCache = new HashMap<>();

    private static final String ddlSchemaString = "{" +
                                                "      \"type\": \"record\"," +
                                                "      \"name\": \"maxwell_ddl\"," +
                                                "      \"namespace\": \"maxwell_dll.avro\"," +
                                                "      \"fields\": [" +
                                                "        {" +
                                                "          \"name\": \"value\"," +
                                                "          \"type\": \"string\"" +
                                                "        }" +
                                                "    ]" +
                                                "}";

    private static final Schema ddlSchema = new Schema.Parser().parse(ddlSchemaString);
    private static final GenericRecord ddlSchemaRecord = new GenericData.Record(ddlSchema);

    public SchemaRegistryProducer(MaxwellContext context, Properties kafkaProperties, String kafkaTopic) {
        super(context, kafkaTopic);

        // hardcoded because the schema registry is opinionated about what it accepts
        kafkaProperties.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaProperties.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        this.kafka = new KafkaProducer<>(kafkaProperties);

        this.schemaMappingURI = context.getConfig().schemaMappingURI;
    }

    /** TODO: fix or rm docs (please fix)
     * Load the keyLookup, dataColumn and childSchemas from the specified config file.
     */
    private JSONObject loadResource(String resourceURI) {
        if (resourceURI == null) {
            throw new RuntimeException("No resource supplied.");
        }

        try {
            URL resource = new URL(resourceURI);

            String content = IOUtils.toString(resource.openStream()); // TODO: deprecated call
            return new JSONObject(content);
        } catch (IOException | NullPointerException e) {
            throw new RuntimeException("Failed to load resource: " + resourceURI + ". Exception: " + e.toString());
        }
    }

    private JSONObject getResourceWithCache(String uri) {
        JSONObject schemaInfo;
        if (this.resourceCache.containsKey(uri)){
            schemaInfo = this.resourceCache.get(uri);
        } else {
            schemaInfo = loadResource(uri);
            this.resourceCache.put(uri, schemaInfo);
        }

        return schemaInfo;
    }

    /**
     * Why would we cache the schema? See the lengthy description where schemaCache is declared.
     * In short, the Avro Schema Registry client chooses an IdentityHasHMap for performance reasons.
     *
     * @param schemaString The schema corresponding to the current row of data.
     * @return The cached schema object.
     */
    private Schema getSchemaFromCache(String schemaString) {
        Schema schema;
        if (schemaCache.containsKey(schemaString)) {
            schema = this.schemaCache.get(schemaString);
        } else {
            schema = new Schema.Parser().parse(schemaString);
            schemaCache.put(schemaString, schema);
        }
        return schema;
    }

    private String getResourceKey(String tableName) {
        return this.schemaMappingURI + tableName + this.JSON_SUFFIX;
    }

    private ArrayList<String> getTypesForSchema(Schema schema, String dataKey) {
        ArrayList<String> validTypes = new ArrayList<String>();
        Schema typeSchema = ((Schema.Field) schema.getField(dataKey)).schema();

        if (!typeSchema.getType().getName().equals("union")) {
            // we have 1 type for this field. add it and return
            validTypes.add(typeSchema.getType().getName());
        } else {
            // multiple types. populate them all and return
            List<Schema> types = ((Schema.Field) schema.getField(dataKey)).schema().getTypes();
            Schema[] schemas = types.toArray(new Schema[types.size()]);
            if (schemas.length > 2) {
                throw new RuntimeException(">2 types for key " + dataKey);
            }

            for (Schema s : schemas) {
                validTypes.add(s.getName());
            }
        }
        return validTypes;
    }

    private byte[] valueSerializer(Object object) {
        if (object instanceof String) {
            return ((String) object).getBytes();//ByteBuffer.wrap((byte[]) ((String) object).getBytes());
        } else if (object instanceof BigDecimal) {
            return ((BigDecimal) object).unscaledValue().toByteArray();
        } else if (object instanceof byte[]) {
            return (byte[]) object;
        } else {
            throw new RuntimeException("Unknown byte conversion from " + object.getClass().toString());
        }
    }

    /**
     * Create and populate a GenericRecord based on a schema and a row of data.
     *
     * @param schema The Schema object.
     * @param rowMap The row of data.
     * @return A GenericRecord based on the Schema and row data.
     */
    private GenericRecord populateRecordFromRowMap(Schema schema, RowMap rowMap) {
        GenericRecord record = new GenericData.Record(schema);

        for (String dataKey : rowMap.getDataKeys()) {
            Object data = rowMap.getData(dataKey);
            ArrayList<String> validTypes = getTypesForSchema(schema, dataKey);

            if (data == null) {
                if (!validTypes.contains(TYPE_NULL)) {
                    throw new RuntimeException("Invalid null value found in field " + schema.getField(dataKey).name());
                }
                record.put(dataKey, null);
            } else if (validTypes.contains(TYPE_DOUBLE)) {
                record.put(dataKey, Double.parseDouble(data.toString()));
            } else if (validTypes.contains((TYPE_FLOAT))) {
                record.put(dataKey, Float.parseFloat(data.toString()));
            } else if (validTypes.contains((TYPE_LONG))) {
                record.put(dataKey, Long.parseLong(data.toString()));
            } else if (validTypes.contains((TYPE_STRING))) {
                record.put(dataKey, data.toString());
            } else if (validTypes.contains((TYPE_INT))) {
                record.put(dataKey, Integer.parseInt(data.toString()));
            } else if (validTypes.contains((TYPE_BOOLEAN))) {
                record.put(dataKey, Boolean.parseBoolean(data.toString()));
            } else if (validTypes.contains((TYPE_FIXED))) {
                record.put(dataKey, data.toString());
            } else if (validTypes.contains((TYPE_BYTES))) {
                byte[] bytes = valueSerializer(data);
                record.put(dataKey, java.nio.ByteBuffer.wrap(bytes));
            } else {
                throw new RuntimeException("Unknown mapping for avro type "
                        + ((Schema.Field) schema.getField(dataKey)).schema().getType().getName().toString());
            }
        }
        return record;
    }

    protected Integer getNumPartitions(String topic) {
        try {
            return this.kafka.partitionsFor(topic).size(); //returns 1 for new topics
        } catch (KafkaException e) {
            LOGGER.error("Topic '" + topic + "' name does not exist. Exception: " + e.getLocalizedMessage());
            throw e;
        }
    }

    @Override
    public void push(RowMap r) throws Exception {
        String key = r.pkToJson(keyFormat);
        String value = r.toJSON(outputConfig);

        if ( value == null ) { // heartbeat row or other row with suppressed output
            skipMessage(r);
            return;
        }

        ProducerRecord<String, GenericRecord> record;
        GenericRecord genericRecord;
        if (r instanceof DDLMap) {
            genericRecord = ddlSchemaRecord;
            genericRecord.put("value", value);
            record = new ProducerRecord<>(this.ddlTopic, this.ddlPartitioner.kafkaPartition(r, getNumPartitions(this.ddlTopic)), key, genericRecord);
        } else {
            String schemaString = getResourceWithCache(getResourceKey(r.getTable())).get("schema").toString();
            Schema schema = getSchemaFromCache(schemaString);
            genericRecord = populateRecordFromRowMap(schema, r);
            String topic = generateTopic(this.topic, r);
            record = new ProducerRecord<>(topic, this.partitioner.kafkaPartition(r, getNumPartitions(topic)), key, genericRecord);
        }

        KafkaCallback callback = new KafkaCallback(inflightMessages, r.getPosition(), r.isTXCommit(), this.context, key, genericRecord);

        if ( r.isTXCommit() )
            inflightMessages.addMessage(r.getPosition());


		/* if debug logging isn't enabled, release the reference to `value`, which can ease memory pressure somewhat */
        if ( !KafkaCallback.LOGGER.isDebugEnabled() )
            value = null;

        kafka.send(record, callback);
    }
}
