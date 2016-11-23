package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.partitioners.MaxwellKafkaPartitioner;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.row.RowMap.KeyFormat;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;

public class SchemaRegistryProducer extends AbstractProducer {
    static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegistryProducer.class);

    private final InflightMessageList inflightMessages;
    private final KafkaProducer<String, GenericRecord> kafka;
    private String topic;
    private final String ddlTopic;
    private final MaxwellKafkaPartitioner partitioner;
    private final MaxwellKafkaPartitioner ddlPartitioner;
    private final KeyFormat keyFormat;
    private JSONObject resources;

    private static final String keyLookup = "key_column";
    private static final String dataLookup = "data_column";
    private static final String childSchemaLookup = "child_schema_uri";

    // NOTE: The KafkaAvroProducer maintains a cache of known childSchemas via an IdentityHashMap.
    // Since the equals method on IdentityHashMap is based on identity, not value - like other hashmaps,
    // passing in a newly created Schema that is logically equivalent to previously passed in childSchemas
    // results in the Producer believing it has a new Schema and caching it (again).
    // As a result the Producer's cache quickly fills up. Once full the Producer raises and exception and falls down.
    // Given this behavior we must maintain our own cache of childSchemas and always pass in the same Schema object to the
    // Producer. This represents a little more overhead, but it should be tolerable - even a few hundred Schemas would
    // represent a small about of in-memory data.
    private HashMap<String, Schema> schemaCache = new HashMap<String, Schema>();

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
        super(context);
        // TODO: maybe refactor to extend maxwellkafkaproducer

        this.topic = kafkaTopic;
        if ( this.topic == null ) {
            this.topic = "maxwell";
        }

        // hardcoded because the schema registry is opinionated about what it accepts
        kafkaProperties.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaProperties.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        this.kafka = new KafkaProducer<>(kafkaProperties);

        String hash = context.getConfig().kafkaPartitionHash;
        String partitionKey = context.getConfig().kafkaPartitionKey;
        String partitionColumns = context.getConfig().kafkaPartitionColumns;
        String partitionFallback = context.getConfig().kafkaPartitionFallback;
        this.partitioner = new MaxwellKafkaPartitioner(hash, partitionKey, partitionColumns, partitionFallback);
        this.ddlPartitioner = new MaxwellKafkaPartitioner(hash, "database", null,"database");
        this.ddlTopic =  context.getConfig().ddlKafkaTopic;

        if ( context.getConfig().kafkaKeyFormat.equals("hash") )
            keyFormat = KeyFormat.HASH;
        else
            keyFormat = KeyFormat.ARRAY;

        this.inflightMessages = new InflightMessageList();
        this.resources = loadResource(context.getConfig().jsonMappingConfig);
        LOGGER.info("Loaded resources " + this.resources.keys().toString());
    }

    /**
     * Load the keyLookup, dataColumn and childSchemas from the specified config file.
     *
     * @param resourceURI The resource uri to load data from.
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
            LOGGER.error("Failed to load resources: " + e.toString());
        }

        return null;
    }

    /**
     * Return the schema for a given key.
     *
     * @param rowMap
     * @return The schema corresponding to the key.
     */
    private String getSchema(RowMap rowMap) {
        try {
            JSONObject schemaInfo = this.resources.getJSONObject(rowMap.getTable());
            JSONObject schema = schemaInfo.getJSONObject("schema");
            if (schemaInfo.keySet().contains(this.keyLookup)) {
                JSONObject childSchema = getChildSchema(rowMap, schemaInfo);
                String dataColumn = schemaInfo.getString(this.dataLookup);

                for (Object field : schema.getJSONArray("fields")) {
                    JSONObject f = (JSONObject) field;
                    if (f.getString("name").equals(dataColumn)) {
                        f.put("type", childSchema);
                        break;
                    }
                }
            }
            return schema.toString();
        } catch (JSONException e) {
            throw new RuntimeException("No schema information for table " + rowMap.getTable() + " - " + e.toString());
        }
    }

    private JSONObject getChildSchema(RowMap rowMap, JSONObject schemaInfo) {
        String key = schemaInfo.getString(this.keyLookup);
        return loadResource(schemaInfo.getString(this.childSchemaLookup) + rowMap.getData(key) + ".json");
    }

    /**
     * Why would we cache the schema? See the lengthy description where schemaCache is declared.
     * In short, the Avro Schema Registry client chooses an IdentityHasHMap which has an eccentric .equals() method.
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

    /**
     * Create and populate a GenericRecord based on a schema and a row of data.
     *
     * @param schema The Schema object.
     * @param rowMap The row of data.
     * @return A GenericRecord based on the Schema and row data.
     */
    private GenericRecord populateRecord(Schema schema, RowMap rowMap) {
        GenericRecord record = new GenericData.Record(schema);
        JSONObject schemaInfo = this.resources.getJSONObject(rowMap.getTable());
        String dataColumn = null;
        if (schemaInfo.keySet().contains(this.keyLookup)) {
            dataColumn = schemaInfo.getString(this.dataLookup);
        }
        // the issue here is that we cannot blindly populate the record since we may have a subrecort that
        // is just a string right now, but is actually a json blob
        for (String dataKey : rowMap.getDataKeys()) {
            Object data = rowMap.getData(dataKey);
            // TODO: what other types need dealing with?
            if (data != null && rowMap.getColumnType(dataKey).equals("decimal")) {
                record.put(dataKey, ((BigDecimal) data).floatValue());
            } else
            if (dataColumn != null && dataKey.equals(dataColumn)) {
                Schema childSchema = ((Schema) record.getSchema()).getField("event_data").schema();
                GenericRecord childRecord = populateRecordFromJson(childSchema, new JSONObject(data.toString()));
                record.put(dataKey, childRecord);
            } else {
                record.put(dataKey, data);
            }
        }
        return record;
    }

    /**
     * Create and populate a GenericRecord from a Schema and a Json object.
     *
     * @param schema The Schema object.
     * @param data the Json object.
     * @return A GenericRecord based on the Schema and the Json object.
     */
    private GenericRecord populateRecordFromJson(Schema schema, JSONObject data) {
        GenericRecord record = new GenericData.Record(schema);
        for (String key : data.keySet()) {
            if (schema.getField(key).schema().getType().getName().equals("record")) {
                record.put(key, populateRecordFromJson(schema.getField(key).schema(), (JSONObject) data.get(key)));
            } else {
                Object obj = data.get(key);
                if (obj instanceof Double) {
                    record.put(key, ((Double) obj).doubleValue());
                } else if (obj instanceof Float) {
                    record.put(key, ((Float) obj).floatValue());
                } else if (obj instanceof Long) {
                    record.put(key, ((Long) obj).longValue());
                } else if (obj instanceof Integer) {
                    record.put(key, ((Integer) obj).intValue());
                } else if (obj instanceof String) {
                    record.put(key, obj.toString());
                } else {
                    throw new RuntimeException("Unknown type mapping from " + obj.getClass());
                }
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

    private String generateTopic(String topic, RowMap r){
        return topic.replaceAll("%\\{database\\}", r.getDatabase()).replaceAll("%\\{table\\}", r.getTable());
    }

    @Override
    public void push(RowMap r) throws Exception {
        String key = r.pkToJson(keyFormat);
        String value = r.toJSON(outputConfig);

        if ( value == null ) { // heartbeat row or other row with suppressed output
            inflightMessages.addMessage(r.getPosition());
            BinlogPosition newPosition = inflightMessages.completeMessage(r.getPosition());

            if ( newPosition != null ) {
                context.setPosition(newPosition);
            }

            return;
        }

        ProducerRecord<String, GenericRecord> record;
        GenericRecord genericRecord;
        String topic;
        if (r instanceof DDLMap) {
            genericRecord = ddlSchemaRecord;
            genericRecord.put("value", value);
            topic = this.ddlTopic;
        } else {
            // TODO: test against RI
            Schema schema = getSchemaFromCache(getSchema(r));
            genericRecord = populateRecord(schema, r);
            topic = generateTopic(this.topic, r);
        }

        record = new ProducerRecord<>(topic, this.partitioner.kafkaPartition(r, getNumPartitions(topic)), key, genericRecord);

        KafkaCallback callback = new KafkaCallback(inflightMessages, r.getPosition(), r.isTXCommit(), this.context, key, genericRecord);

        if ( r.isTXCommit() )
            inflightMessages.addMessage(r.getPosition());


		/* if debug logging isn't enabled, release the reference to `value`, which can ease memory pressure somewhat */
        if ( !KafkaCallback.LOGGER.isDebugEnabled() )
            value = null;

        kafka.send(record, callback);
    }

    @Override
    public void writePosition(BinlogPosition p) throws SQLException {
        // ensure that we don't prematurely advance the binlog pointer.
        inflightMessages.addMessage(p);
        inflightMessages.completeMessage(p);
    }
}
