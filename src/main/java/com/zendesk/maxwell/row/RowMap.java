package com.zendesk.maxwell.row;

import com.zendesk.maxwell.errors.ProtectedAttributeNameException;
import com.zendesk.maxwell.producer.EncryptionMode;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.replication.Position;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonGenerator;


public class RowMap implements Serializable {


	public enum KeyFormat { HASH, ARRAY }

	static final Logger LOGGER = LoggerFactory.getLogger(RowMap.class);

	private String rowQuery;
	private final String rowType;
	private final String database;
	private final String table;
	private final Long timestampMillis;
	private final Long timestampSeconds;
	private final Position position;
	private Position nextPosition;
	private String kafkaTopic;
	private String partitionString;
	protected boolean suppressed;

	private Long xid;
	private Long xoffset;
	private boolean txCommit;
	private Long serverId;
	private Long threadId;
	private Long schemaId;

	private final LinkedHashMap<String, Object> data;
	private final LinkedHashMap<String, Object> dataSchema;
	private final LinkedHashMap<String, Object> oldData;
	private final LinkedHashMap<String, Object> oldDataSchema;

	private final LinkedHashMap<String, Object> extraAttributes;

	private final List<String> pkColumns;
	private RowIdentity rowIdentity;

	private long approximateSize;

	public RowMap(String type, String database, String table, Long timestampMillis, List<String> pkColumns,
			Position position, Position nextPosition, String rowQuery) {
		this.rowQuery = rowQuery;
		this.rowType = type;
		this.database = database;
		this.table = table;
		this.timestampMillis = timestampMillis;
		this.timestampSeconds = timestampMillis / 1000;
		this.data = new LinkedHashMap<>();
		this.dataSchema = new LinkedHashMap<>();
		this.oldData = new LinkedHashMap<>();
		this.oldDataSchema = new LinkedHashMap<>();
		this.extraAttributes = new LinkedHashMap<>();
		this.position = position;
		this.nextPosition = nextPosition;
		this.pkColumns = pkColumns;
		this.suppressed = false;
		this.approximateSize = 100L; // more or less 100 bytes of overhead
	}

	public RowMap(String type, String database, String table, Long timestampMillis, List<String> pkColumns,
				  Position nextPosition, String rowQuery) {
		this(type, database, table, timestampMillis, pkColumns, nextPosition, nextPosition, rowQuery);
	}

	public RowMap(String type, String database, String table, Long timestampMillis, List<String> pkColumns,
				  Position nextPosition) {
		this(type, database, table, timestampMillis, pkColumns, nextPosition, null);
	}

	public RowIdentity getRowIdentity() {
		if (rowIdentity == null) {
			List<Pair<String, Object>> entries = new ArrayList<>(pkColumns.size());
			for (String pk: pkColumns) {
				entries.add(Pair.of(pk, data.get(pk)));
			}
			rowIdentity = new RowIdentity(database, table, rowType, entries);
		}

		return rowIdentity;
	}

	public String pkToJson(KeyFormat format) throws IOException {
		return getRowIdentity().toKeyJson(format);
	}

	public String buildPartitionKey(List<String> partitionColumns) {
		StringBuilder partitionKey= new StringBuilder();
		for (String pc : partitionColumns) {
			Object pcValue = null;
			if (data.containsKey(pc))
				pcValue = data.get(pc);
			if (pcValue != null)
				partitionKey.append(pcValue.toString());
		}

		return partitionKey.toString();
	}

	private void writeMapToJSON(
			String jsonMapName,
			LinkedHashMap<String, Object> data,
			JsonGenerator g,
			boolean includeNullField
	) throws IOException, NoSuchAlgorithmException {
		g.writeObjectFieldStart(jsonMapName);

		for (String key : data.keySet()) {
			Object value = data.get(key);

			MaxwellJson.writeValueToJSON(g, includeNullField, key, value);
		}

		g.writeEndObject(); // end of 'jsonMapName: { }'
	}

	public String toJSON() throws Exception {
		return toJSON(new MaxwellOutputConfig());
	}

	public String toJSON(MaxwellOutputConfig outputConfig) throws Exception {
		MaxwellJson json = MaxwellJson.getInstance();
		JsonGenerator g = json.reset();

		g.writeStartObject(); // start of row {

		g.writeStringField(FieldNames.DATABASE, this.database);
		g.writeStringField(FieldNames.TABLE, this.table);

		if ( outputConfig.includesRowQuery && this.rowQuery != null) {
			g.writeStringField(FieldNames.QUERY, this.rowQuery);
		}

		g.writeStringField(FieldNames.TYPE, this.rowType);
		g.writeNumberField(FieldNames.TIMESTAMP, this.timestampSeconds);

		if ( outputConfig.includesCommitInfo ) {
			if ( this.xid != null )
				g.writeNumberField(FieldNames.TRANSACTION_ID, this.xid);

			if ( outputConfig.includesXOffset && this.xoffset != null && !this.txCommit )
				g.writeNumberField(FieldNames.TRANSACTION_OFFSET, this.xoffset);

			if ( this.txCommit )
				g.writeBooleanField(FieldNames.COMMIT, true);
		}

		if ( this.position != null ) {
			BinlogPosition binlogPosition = this.position.getBinlogPosition();
			if ( outputConfig.includesBinlogPosition )
				g.writeStringField(FieldNames.POSITION, binlogPosition.getFile() + ":" + binlogPosition.getOffset());

			if ( outputConfig.includesGtidPosition)
				g.writeStringField(FieldNames.GTID, binlogPosition.getGtid());
		}

		if ( outputConfig.includesServerId && this.serverId != null ) {
			g.writeNumberField(FieldNames.SERVER_ID, this.serverId);
		}

		if ( outputConfig.includesThreadId && this.threadId != null ) {
			g.writeNumberField(FieldNames.THREAD_ID, this.threadId);
		}

		if ( outputConfig.includesSchemaId && this.schemaId != null ) {
			g.writeNumberField(FieldNames.SCHEMA_ID, this.schemaId);
		}

		for ( Map.Entry<String, Object> entry : this.extraAttributes.entrySet() ) {
			g.writeObjectField(entry.getKey(), entry.getValue());
		}

		EncryptionContext encryptionContext = null;
		if (outputConfig.encryptionEnabled()) {
			encryptionContext = EncryptionContext.create(outputConfig.secretKey);
		}

		DataJsonGenerator dataWriter = outputConfig.encryptionMode == EncryptionMode.ENCRYPT_DATA
			? json.getEncryptingGenerator()
			: json.getPlaintextGenerator();

		JsonGenerator dataGenerator = dataWriter.begin();
		if ( outputConfig.includesPrimaryKeys ) {
			List<Object> pkValues = new ArrayList<>();
			pkColumns.forEach(pkColumn -> pkValues.add(this.data.get(pkColumn)));
			MaxwellJson.writeValueToJSON(g, outputConfig.includesNulls, FieldNames.PRIMARY_KEY, pkValues);
		}

		if ( outputConfig.includesPrimaryKeyColumns ) {
			MaxwellJson.writeValueToJSON(g, outputConfig.includesNulls, FieldNames.PRIMARY_KEY_COLUMNS, pkColumns);
		}

		if ( outputConfig.excludeColumns.size() > 0 ) {
			// NOTE: to avoid concurrent modification.
			Set<String> keys = new HashSet<>();
			keys.addAll(this.data.keySet());
			keys.addAll(this.oldData.keySet());

			for ( Pattern p : outputConfig.excludeColumns ) {
				for ( String key : keys ) {
					if ( p.matcher(key).matches() ) {
						this.data.remove(key);
						this.oldData.remove(key);
						this.dataSchema.remove(key);
						this.oldDataSchema.remove(key);
					}
				}
			}
		}

		writeMapToJSON(FieldNames.DATA, this.data, dataGenerator, outputConfig.includesNulls);
		if( !this.oldData.isEmpty() ){
			writeMapToJSON(FieldNames.OLD, this.oldData, dataGenerator, outputConfig.includesNulls);

			if(outputConfig.includesSchema) {
				writeMapToJSON(FieldNames.SCHEMA_OLD, this.oldDataSchema, dataGenerator, outputConfig.includesNulls);
			}
		}
		
		if(outputConfig.includesSchema){
			writeMapToJSON(FieldNames.SCHEMA, this.dataSchema, dataGenerator, outputConfig.includesNulls);
		}

		dataWriter.end(encryptionContext);

		g.writeEndObject(); // end of row

		if(outputConfig.encryptionMode == EncryptionMode.ENCRYPT_ALL){
			String plaintext = json.consume();
			json.getEncryptingGenerator().writeEncryptedObject(plaintext, encryptionContext);
		}
		
		return json.consume();
	}

	public Object getData(String key) {
		return this.data.get(key);
	}

	public Object getExtraAttribute(String key) {
		return this.extraAttributes.get(key);
	}

	public long getApproximateSize() {
		return approximateSize;
	}

	private long approximateKVSize(String key, Object value) {
		long length = 0;
		length += 40; // overhead.  Whynot.
		length += key.length() * 2;

		if ( value instanceof String ) {
			length += ((String) value).length() * 2;
		} else {
			length += 64;
		}

		return length;
	}

	public void putData(String key, Object value) {
		this.data.put(key, value);

		this.approximateSize += approximateKVSize(key, value);
	}

	public void putDataSchema(String key, String type) {
		this.dataSchema.put(key, type);
	}

	public void putExtraAttribute(String key, Object value) {
		if (FieldNames.isProtected(key)) {
			throw new ProtectedAttributeNameException("Extra attribute key name '" + key + "' is " +
					"a protected name. Must not be any of: " +
					String.join(", ", FieldNames.getFieldnames()));
		}
		this.extraAttributes.put(key, value);

		this.approximateSize += approximateKVSize(key, value);
	}

	public Object getOldData(String key) {
		return this.oldData.get(key);
	}

	public void putOldData(String key, Object value) {
		this.oldData.put(key, value);

		this.approximateSize += approximateKVSize(key, value);
	}

	public void putOldDataSchema(String key, String type) {
		this.oldDataSchema.put(key, type);
	}

	public Position getNextPosition() { return nextPosition; }
	public Position getPosition() { return position; }

	public Long getXid() {
		return xid;
	}

	public void setXid(Long xid) {
		this.xid = xid;
	}

	public Long getXoffset() {
		return xoffset;
	}

	public void setXoffset(Long xoffset) {
		this.xoffset = xoffset;
	}

	public void setTXCommit() {
		this.txCommit = true;
	}

	public boolean isTXCommit() {
		return this.txCommit;
	}

	public Long getServerId() {
		return serverId;
	}

	public void setServerId(Long serverId) {
		this.serverId = serverId;
	}

	public Long getThreadId() {
		return threadId;
	}

	public void setThreadId(Long threadId) {
		this.threadId = threadId;
	}

	public Long getSchemaId() {
		return schemaId;
	}

	public void setSchemaId(Long schemaId) {
		this.schemaId = schemaId;
	}

	public String getDatabase() {
		return database;
	}

	public String getTable() {
		return table;
	}

	public Long getTimestamp() {
		return timestampSeconds;
	}

	public Long getTimestampMillis() {
		return timestampMillis;
	}

	public boolean hasData(String name) {
		return this.data.containsKey(name);
	}

	public String getRowQuery() {
		return rowQuery;
	}

	public void setRowQuery(String query) {
		this.rowQuery = query;
	}

	public String getRowType() {
		return this.rowType;
	}

	// determines whether there is anything for the producer to output
	// override this for extended classes that don't output a value
	// return false when there is a heartbeat row or other row with suppressed output
	public boolean shouldOutput(MaxwellOutputConfig outputConfig) {
		return !suppressed;
	}

	public LinkedHashMap<String, Object> getData()
	{
		return data;
	}

	public LinkedHashMap<String, Object> getExtraAttributes()
	{
		return extraAttributes;
	}

	public LinkedHashMap<String, Object> getOldData()
	{
		return oldData;
	}

	public void suppress() {
		this.suppressed = true;
	}

	public String getKafkaTopic() {
		return this.kafkaTopic;
	}
	public void setKafkaTopic(String topic) {
		this.kafkaTopic = topic;
	}

	public String getPartitionString() {
		return this.partitionString;
	}

	public void setPartitionString(String partitionString) {
		this.partitionString = partitionString;
	}

}
