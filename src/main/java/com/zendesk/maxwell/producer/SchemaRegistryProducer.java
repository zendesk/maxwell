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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

public class SchemaRegistryProducer extends AbstractProducer {
    static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegistryProducer.class);

    private final InflightMessageList inflightMessages;
    private final KafkaProducer<String, Object> kafka;
    private String topic;
    private final String ddlTopic;
    private final MaxwellKafkaPartitioner partitioner;
    private final MaxwellKafkaPartitioner ddlPartitioner;
    private final KeyFormat keyFormat;
    private HashMap<String, String> jsonMappings;
    private JSONObject configuredSchemas;
    private String dataColumn;
    private String keyColumn;

    // NOTE: The KafkaAvroProducer maintains a cache of known configuredSchemas via an IdentityHashMap.
    // Since the equals method on IdentityHashMap is based on identity, not value - like other hashmaps,
    // passing in a newly created Schema that is logically equivalent to previously passed in configuredSchemas
    // results in the Producer believing it has a new Schema and caching it (again).
    // As a result the Producer's cache quickly fills up. Once full the Producer raises and exception and falls down.
    // Given this behavior we must maintain our own cache of configuredSchemas and always pass in the same Schema object to the
    // Producer. This represents a little more overhead, but it should be tolerable - even a few hundred Schemas would
    // represent a small about of in-memory data.
    private HashMap<String, Schema> schemaCache = new HashMap<String, Schema>();

    public SchemaRegistryProducer(MaxwellContext context, Properties kafkaProperties, String kafkaTopic) {
        super(context);

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
        loadJsonConfig(context.getConfig().jsonMappingConfig);
    }

    private void loadJsonConfig(String filename) {
        if (filename == null) {
            return;
        }

        try {
            String content = new String(Files.readAllBytes(Paths.get(filename)));
            JSONObject obj = new JSONObject(content);
            this.configuredSchemas = (JSONObject) obj.get("schemas");
            this.dataColumn = obj.get("data_column").toString();
            this.keyColumn = obj.get("key_column").toString();
            LOGGER.info("Loaded configuredSchemas " + this.configuredSchemas.keys().toString());
        } catch (IOException | NullPointerException e) {
            LOGGER.error("Failed to load configuredSchemas: " + e.toString());
        }
    }

    private Integer getNumPartitions(String topic) {
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

    private String getSchema(String key) {
        try {
            return this.configuredSchemas.get(key).toString();
        } catch (JSONException e) {
            throw new RuntimeException("No schema for key " + key + " - " + e.toString());
        }
    }

    private String buildSchemaTypeFragment(String columnName, String columnType, String key) {
        // TODO: some types need dealing with, e.g. blob, datetime, timestamp, bit
        // NOTE: we're avoiding complex types at the moment because they seem to take the following form on the
        // consumer side: {"field_name": {"bytes": "<byte value>"}}
        // instead of the form: {"field_name": "field_value"}

        if (this.dataColumn != null && columnName.equals(this.dataColumn)) {
            String fragment = "{  \n" +
                    "            \"name\":\"" + this.dataColumn + "\",\n" +
                    "            \"type\":";

            fragment += getSchema(key);

            fragment += "},";
            return fragment;
        } else if (Arrays.asList("varchar", "char").contains(columnType) || columnType.contains("text")) {
            return "{" +
                    "\"name\":\"" + columnName + "\"," +
                    "  \"type\": [\"null\",\"string\"]" +
                    "},";
        } else if (columnType.contains("blob")) {
            return "{" +
                    "\"name\":\"" + columnName + "\"," +
                    "  \"type\": [\"null\",\"string\"]" +
                    "},";
        } else if (Arrays.asList("int", "smallint", "bigint", "id", "tinyint").contains(columnType)) {
            return "{" +
                    "\"name\":\"" + columnName + "\"," +
                    "  \"type\": [\"null\",\"long\"]" +
                    "},";
        } else if (columnType.equalsIgnoreCase("float") || columnType.equalsIgnoreCase("decimal")) {
            return "{" +
                    "\"name\":\"" + columnName + "\"," +
                    "  \"type\": [\"null\",\"float\"]" +
                    "},";
        } else if (columnType.equalsIgnoreCase("double")) {
            return "{" +
                    "\"name\":\"" + columnName + "\"," +
                    "  \"type\": [\"null\",\"double\"]" +
                    "},";
        } else if (columnType.equalsIgnoreCase("bit")) {
            return "{" +
                    "\"name\":\"" + columnName + "\"," +
                    "  \"type\": [\"null\",\"long\"]" +
                    "},";
        } else if (columnType.equalsIgnoreCase("datetime")) {
            return "{" +
                    "\"name\":\"" + columnName + "\"," +
                    "  \"type\": [\"null\",\"string\"]" +
                    "},";
        } else if (columnType.equalsIgnoreCase("timestamp")) {
            return "{" +
                    "\"name\":\"" + columnName + "\"," +
                    "  \"type\": [\"null\",\"string\"]" +
                    "},";
        } else {
            throw new RuntimeException("Unknown conversion for column: " + columnName + ", database type: " + columnType);
        }
    }

    private String buildSchema(RowMap rowMap) {
        String schema = "{" +
                "    \"type\":\"record\"," +
                "    \"name\":\"" + rowMap.getTable() + "\"," +
                "    \"namespace\":\"" + rowMap.getTable() + ".avro\"," +
                "    \"fields\":[";

        for (String columnName : rowMap.getDataKeys()) {
            String key = null;
            if (this.keyColumn != null) {
                key = rowMap.getData(this.keyColumn).toString();
            }
            schema += buildSchemaTypeFragment(columnName, rowMap.getColumnType(columnName), key);
        }

        schema += "]}";
        schema = schema.replace("},]}", "}]}");
        return schema;
    }

    // Why would we cache the schema? See the lengthy description where schemaCache is declared.
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

    private GenericRecord populateRecord(Schema schema, RowMap rowMap) {
        GenericRecord maxwellRecord = new GenericData.Record(schema);
        for (String dataKey : rowMap.getDataKeys()) {
            Object data = rowMap.getData(dataKey);
            // TODO: what other types need dealing with?
            if (data != null && rowMap.getColumnType(dataKey).equals("decimal")) {
                maxwellRecord.put(dataKey, ((BigDecimal) data).floatValue());
            } else if (dataKey.equals(this.dataColumn)) {
                String schemaStr = getSchema(rowMap.getData(this.keyColumn).toString());
                Schema childSchema = getSchemaFromCache(schemaStr);
                GenericRecord childRecord = populateRecordFromJson(childSchema, new JSONObject(data.toString()));
                maxwellRecord.put(dataKey, childRecord);
            } else {
                maxwellRecord.put(dataKey, data);
            }
        }
        return maxwellRecord;
    }

    private GenericRecord populateRecordFromJson(Schema schema, JSONObject data) {
        GenericRecord maxwellRecord = new GenericData.Record(schema);
        for (String key : data.keySet()) {
            if (schema.getField(key).schema().getType().getName().equals("record")) {
                maxwellRecord.put(key, populateRecordFromJson(schema.getField(key).schema(), (JSONObject) data.get(key)));
            } else if (schema.getField(key).schema().getTypes().toString().contains("float")) {
                maxwellRecord.put(key, Float.parseFloat(data.get(key).toString()));
            } else if (schema.getField(key).schema().getTypes().toString().contains("int")) {
                maxwellRecord.put(key, Integer.parseInt((data.get(key).toString())));
            } else {
                maxwellRecord.put(key, data.get(key));
            }
        }
        return maxwellRecord;
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

        KafkaCallback callback;
        ProducerRecord<String, Object> record;
        if (r instanceof DDLMap) {
            record = new ProducerRecord<>(this.ddlTopic, this.ddlPartitioner.kafkaPartition(r, getNumPartitions(this.ddlTopic)), key, (Object) value);

            callback = new KafkaCallback(inflightMessages, r.getPosition(), r.isTXCommit(), this.context, key, value);
        } else {
            String schemaString = buildSchema(r);
            Schema schema = getSchemaFromCache(schemaString);

            GenericRecord maxwellRecord = populateRecord(schema, r);

            String topic = generateTopic(this.topic, r);
            record = new ProducerRecord<>(topic, this.partitioner.kafkaPartition(r, getNumPartitions(topic)), key, (Object) maxwellRecord);

            callback = new KafkaCallback(inflightMessages, r.getPosition(), r.isTXCommit(), this.context, key, maxwellRecord);
        }

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
