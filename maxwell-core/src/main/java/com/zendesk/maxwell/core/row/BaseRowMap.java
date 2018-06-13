package com.zendesk.maxwell.core.row;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.zendesk.maxwell.api.config.MaxwellOutputConfig;
import com.zendesk.maxwell.api.config.EncryptionMode;
import com.zendesk.maxwell.api.replication.BinlogPosition;
import com.zendesk.maxwell.api.replication.Position;
import com.zendesk.maxwell.core.config.BaseMaxwellOutputConfig;
import com.zendesk.maxwell.core.errors.ProtectedAttributeNameException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.regex.Pattern;


public class BaseRowMap implements Serializable, RowMap {

	static final Logger LOGGER = LoggerFactory.getLogger(BaseRowMap.class);

	private final String rowQuery;
	private final String rowType;
	private final String database;
	private final String table;
	private final Long timestampMillis;
	private final Long timestampSeconds;
	private Position nextPosition;

	private Long xid;
	private Long xoffset;
	private boolean txCommit;
	private Long serverId;
	private Long threadId;

	private final LinkedHashMap<String, Object> data;
	private final LinkedHashMap<String, Object> oldData;

	private final LinkedHashMap<String, Object> extraAttributes;

	private final List<String> pkColumns;

	private static final JsonFactory jsonFactory = new JsonFactory();

	private long approximateSize;

	private static final ThreadLocal<ByteArrayOutputStream> byteArrayThreadLocal =
			new ThreadLocal<ByteArrayOutputStream>(){
				@Override
				protected ByteArrayOutputStream initialValue() {
					return new ByteArrayOutputStream();
				}
			};

	private static JsonGenerator resetJsonGenerator() {
		byteArrayThreadLocal.get().reset();
		return jsonGeneratorThreadLocal.get();
	}

	private static final ThreadLocal<JsonGenerator> jsonGeneratorThreadLocal =
			new ThreadLocal<JsonGenerator>() {
				@Override
				protected JsonGenerator initialValue() {
					JsonGenerator g = null;
					try {
						g = jsonFactory.createGenerator(byteArrayThreadLocal.get());
					} catch (IOException e) {
						LOGGER.error("error initializing jsonGenerator", e);
						return null;
					}
					g.setRootValueSeparator(null);
					return g;
				}
			};

	private static final ThreadLocal<DataJsonGenerator> plaintextDataGeneratorThreadLocal =
			new ThreadLocal<DataJsonGenerator>() {
				@Override
				protected DataJsonGenerator initialValue() {
					return new PlaintextJsonGenerator(jsonGeneratorThreadLocal.get());
				}
			};

	private static final ThreadLocal<EncryptingJsonGenerator> encryptingJsonGeneratorThreadLocal =
			new ThreadLocal<EncryptingJsonGenerator>() {
				@Override
				protected EncryptingJsonGenerator initialValue(){
					try {
						return new EncryptingJsonGenerator(jsonGeneratorThreadLocal.get(),jsonFactory);
					} catch (IOException e) {
						LOGGER.error("error initializing EncryptingJsonGenerator", e);
						return null;
					}
				}
			};

	public BaseRowMap(String type, String database, String table, Long timestampMillis, List<String> pkColumns,
					  Position nextPosition, String rowQuery) {
		this.rowQuery = rowQuery;
		this.rowType = type;
		this.database = database;
		this.table = table;
		this.timestampMillis = timestampMillis;
		this.timestampSeconds = timestampMillis / 1000;
		this.data = new LinkedHashMap<>();
		this.oldData = new LinkedHashMap<>();
		this.extraAttributes = new LinkedHashMap<>();
		this.nextPosition = nextPosition;
		this.pkColumns = pkColumns;
		this.approximateSize = 100L; // more or less 100 bytes of overhead
	}

	public BaseRowMap(String type, String database, String table, Long timestampMillis, List<String> pkColumns,
					  Position nextPosition) {
		this(type, database, table, timestampMillis, pkColumns, nextPosition, null);
	}

	//Do we want to encrypt this part?
	@Override
	public String pkToJson(KeyFormat keyFormat) throws IOException {
		if ( keyFormat == KeyFormat.HASH )
			return pkToJsonHash();
		else
			return pkToJsonArray();
	}

	private String pkToJsonHash() throws IOException {
		JsonGenerator g = resetJsonGenerator();

		g.writeStartObject(); // start of row {

		g.writeStringField(FieldNames.DATABASE, database);
		g.writeStringField(FieldNames.TABLE, table);

		if (pkColumns.isEmpty()) {
			g.writeStringField(FieldNames.UUID, UUID.randomUUID().toString());
		} else {
			for (String pk : pkColumns) {
				Object pkValue = null;
				if ( data.containsKey(pk) )
					pkValue = data.get(pk);

				writeValueToJSON(g, true, "pk." + pk.toLowerCase(), pkValue);
			}
		}

		g.writeEndObject(); // end of 'data: { }'
		g.flush();
		return jsonFromStream();
	}

	private String pkToJsonArray() throws IOException {
		JsonGenerator g = resetJsonGenerator();

		g.writeStartArray();
		g.writeString(database);
		g.writeString(table);

		g.writeStartArray();
		for (String pk : pkColumns) {
			Object pkValue = null;
			if ( data.containsKey(pk) )
				pkValue = data.get(pk);

			g.writeStartObject();
			writeValueToJSON(g, true, pk.toLowerCase(), pkValue);
			g.writeEndObject();
		}
		g.writeEndArray();
		g.writeEndArray();
		g.flush();
		return jsonFromStream();
	}

	@Override
	public String pkAsConcatString() {
		if (pkColumns.isEmpty()) {
			return database + table;
		}
		StringBuilder keys = new StringBuilder();
		for (String pk : pkColumns) {
			Object pkValue = null;
			if (data.containsKey(pk))
				pkValue = data.get(pk);
			if (pkValue != null)
				keys.append(pkValue.toString());
		}
		if (keys.length() == 0)
			return "None";
		return keys.toString();
	}

	@Override
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

			writeValueToJSON(g, includeNullField, key, value);
		}

		g.writeEndObject(); // end of 'jsonMapName: { }'
	}

	private void writeValueToJSON(JsonGenerator g, boolean includeNullField, String key, Object value) throws IOException {
		if (value == null && !includeNullField)
			return;

		if (value instanceof List) { // sets come back from .asJSON as lists, and jackson can't deal with lists natively.
			List stringList = (List) value;

			g.writeArrayFieldStart(key);
			for (Object s : stringList) {
				g.writeObject(s);
			}
			g.writeEndArray();
		} else if (value instanceof RawJSONString) {
			// JSON column type, using binlog-connector's serializers.
			g.writeFieldName(key);
			g.writeRawValue(((RawJSONString) value).json);
		} else {
			g.writeObjectField(key, value);
		}
	}

	@Override
	public String toJSON() throws Exception {
		return toJSON(new BaseMaxwellOutputConfig());
	}

	@Override
	public String toJSON(MaxwellOutputConfig outputConfig) throws Exception {
		JsonGenerator g = resetJsonGenerator();

		g.writeStartObject(); // start of row {

		g.writeStringField(FieldNames.DATABASE, this.database);
		g.writeStringField(FieldNames.TABLE, this.table);

		if ( outputConfig.isIncludesRowQuery() && this.rowQuery != null) {
			g.writeStringField(FieldNames.QUERY, this.rowQuery);
		}

		g.writeStringField(FieldNames.TYPE, this.rowType);
		g.writeNumberField(FieldNames.TIMESTAMP, this.timestampSeconds);

		if (outputConfig.isIncludesCommitInfo()) {
			if ( this.xid != null )
				g.writeNumberField(FieldNames.TRANSACTION_ID, this.xid);

			if ( outputConfig.isIncludesXOffset() && this.xoffset != null && !this.txCommit )
				g.writeNumberField(FieldNames.TRANSACTION_OFFSET, this.xoffset);

			if ( this.txCommit )
				g.writeBooleanField(FieldNames.COMMIT, true);
		}

		BinlogPosition binlogPosition = this.nextPosition.getBinlogPosition();
		if (outputConfig.isIncludesBinlogPosition())
			g.writeStringField(FieldNames.POSITION, binlogPosition.getFile() + ":" + binlogPosition.getOffset());


		if (outputConfig.isIncludesGtidPosition())
			g.writeStringField(FieldNames.GTID, binlogPosition.getGtid());

		if ( outputConfig.isIncludesServerId() && this.serverId != null ) {
			g.writeNumberField(FieldNames.SERVER_ID, this.serverId);
		}

		if ( outputConfig.isIncludesThreadId() && this.threadId != null ) {
			g.writeNumberField(FieldNames.THREAD_ID, this.threadId);
		}

		for ( Map.Entry<String, Object> entry : this.extraAttributes.entrySet() ) {
			g.writeObjectField(entry.getKey(), entry.getValue());
		}

		if ( outputConfig.getExcludeColumns().size() > 0 ) {
			// NOTE: to avoid concurrent modification.
			Set<String> keys = new HashSet<>();
			keys.addAll(this.data.keySet());
			keys.addAll(this.oldData.keySet());

			for ( Pattern p : outputConfig.getExcludeColumns()) {
				for ( String key : keys ) {
					if ( p.matcher(key).matches() ) {
						this.data.remove(key);
						this.oldData.remove(key);
					}
				}
			}
		}


		EncryptionContext encryptionContext = null;
		if (outputConfig.isEncryptionEnabled()) {
			encryptionContext = EncryptionContext.create(outputConfig.getSecretKey());
		}

		DataJsonGenerator dataWriter = outputConfig.getEncryptionMode() == EncryptionMode.ENCRYPT_DATA
			? encryptingJsonGeneratorThreadLocal.get()
			: plaintextDataGeneratorThreadLocal.get();

		JsonGenerator dataGenerator = dataWriter.begin();
		writeMapToJSON(FieldNames.DATA, this.data, dataGenerator, outputConfig.isIncludesNulls());
		if( !this.oldData.isEmpty() ){
			writeMapToJSON(FieldNames.OLD, this.oldData, dataGenerator, outputConfig.isIncludesNulls());
		}
		dataWriter.end(encryptionContext);

		g.writeEndObject(); // end of row
		g.flush();

		if(outputConfig.getEncryptionMode() == EncryptionMode.ENCRYPT_ALL){
			String plaintext = jsonFromStream();
			encryptingJsonGeneratorThreadLocal.get().writeEncryptedObject(plaintext, encryptionContext);
			g.flush();
		}
		return jsonFromStream();
	}

	private String jsonFromStream() {
		ByteArrayOutputStream b = byteArrayThreadLocal.get();
		String s = b.toString();
		b.reset();
		return s;
	}

	@Override
	public Object getData(String key) {
		return this.data.get(key);
	}

	@Override
	public Object getExtraAttribute(String key) {
		return this.extraAttributes.get(key);
	}

	@Override
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

	@Override
	public void putData(String key, Object value) {
		this.data.put(key, value);

		this.approximateSize += approximateKVSize(key, value);
	}

	@Override
	public void putExtraAttribute(String key, Object value) {
		if (FieldNames.isProtected(key)) {
			throw new ProtectedAttributeNameException("Extra attribute key name '" + key + "' is " +
					"a protected name. Must not be any of: " +
					String.join(", ", FieldNames.getFieldnames()));
		}
		this.extraAttributes.put(key, value);

		this.approximateSize += approximateKVSize(key, value);
	}

	@Override
	public Object getOldData(String key) {
		return this.oldData.get(key);
	}

	@Override
	public void putOldData(String key, Object value) {
		this.oldData.put(key, value);

		this.approximateSize += approximateKVSize(key, value);
	}

	@Override
	public Position getPosition() {
		return nextPosition;
	}

	@Override
	public Long getXid() {
		return xid;
	}

	@Override
	public void setXid(Long xid) {
		this.xid = xid;
	}

	@Override
	public Long getXoffset() {
		return xoffset;
	}

	@Override
	public void setXoffset(Long xoffset) {
		this.xoffset = xoffset;
	}

	@Override
	public void setTXCommit() {
		this.txCommit = true;
	}

	@Override
	public boolean isTXCommit() {
		return this.txCommit;
	}

	@Override
	public Long getServerId() {
		return serverId;
	}

	@Override
	public void setServerId(Long serverId) {
		this.serverId = serverId;
	}

	@Override
	public Long getThreadId() {
		return threadId;
	}

	@Override
	public void setThreadId(Long threadId) {
		this.threadId = threadId;
	}

	@Override
	public String getDatabase() {
		return database;
	}

	@Override
	public String getTable() {
		return table;
	}

	@Override
	public Long getTimestamp() {
		return timestampSeconds;
	}

	@Override
	public Long getTimestampMillis() {
		return timestampMillis;
	}

	@Override
	public boolean hasData(String name) {
		return this.data.containsKey(name);
	}

	@Override
	public String getRowQuery() {
		return rowQuery;
	}

	@Override
	public String getRowType() {
		return this.rowType;
	}

	// determines whether there is anything for the producer to output
	// override this for extended classes that don't output a value
	// return false when there is a heartbeat row or other row with suppressed output
	@Override
	public boolean shouldOutput(MaxwellOutputConfig outputConfig) {
		return true;
	}

	@Override
	public LinkedHashMap<String, Object> getData()
	{
		return new LinkedHashMap<>(data);
	}

	@Override
	public LinkedHashMap<String, Object> getExtraAttributes()
	{
		return new LinkedHashMap<>(extraAttributes);
	}

	@Override
	public LinkedHashMap<String, Object> getOldData()
	{
		return new LinkedHashMap<>(oldData);
	}
}
