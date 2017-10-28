package com.zendesk.maxwell.row;

import com.fasterxml.jackson.core.*;
import com.zendesk.maxwell.producer.EncryptionMode;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.replication.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;


public class RowMap implements Serializable {

	public enum KeyFormat { HASH, ARRAY }

	static final Logger LOGGER = LoggerFactory.getLogger(RowMap.class);

	private final String rowType;
	private final String database;
	private final String table;
	private final Long timestampMillis;
	private final Long timestampSeconds;
	private Position nextPosition;

	private Long xid;
	private boolean txCommit;
	private Long serverId;
	private Long threadId;

	private final LinkedHashMap<String, Object> data;
	private final LinkedHashMap<String, Object> oldData;
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

	public RowMap(String type, String database, String table, Long timestampMillis, List<String> pkColumns,
			Position nextPosition) {
		this.rowType = type;
		this.database = database;
		this.table = table;
		this.timestampMillis = timestampMillis;
		this.timestampSeconds = timestampMillis / 1000;
		this.data = new LinkedHashMap<>();
		this.oldData = new LinkedHashMap<>();
		this.nextPosition = nextPosition;
		this.pkColumns = pkColumns;
		this.approximateSize = 100L; // more or less 100 bytes of overhead
	}

	//Do we want to encrypt this part?
	public String pkToJson(KeyFormat keyFormat) throws IOException {
		if ( keyFormat == KeyFormat.HASH )
			return pkToJsonHash();
		else
			return pkToJsonArray();
	}

	private String pkToJsonHash() throws IOException {
		JsonGenerator g = resetJsonGenerator();

		g.writeStartObject(); // start of row {

		g.writeStringField("database", database);
		g.writeStringField("table", table);

		if (pkColumns.isEmpty()) {
			g.writeStringField("_uuid", UUID.randomUUID().toString());
		} else {
			for (String pk : pkColumns) {
				Object pkValue = null;
				if ( data.containsKey(pk) )
					pkValue = data.get(pk);

				g.writeObjectField("pk." + pk.toLowerCase(), pkValue);
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
			g.writeObjectField(pk.toLowerCase(), pkValue);
			g.writeEndObject();
		}
		g.writeEndArray();
		g.writeEndArray();
		g.flush();
		return jsonFromStream();
	}

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

	public String buildPartitionKey(List<String> partitionColumns, String partitionKeyFallback) {
		StringBuilder partitionKey= new StringBuilder();
		for (String pc : partitionColumns) {
			Object pcValue = null;
			if (data.containsKey(pc))
				pcValue = data.get(pc);
			if (pcValue != null)
				partitionKey.append(pcValue.toString());
		}
		if (partitionKey.length() == 0)
			return getPartitionKeyFallback(partitionKeyFallback);
		return partitionKey.toString();
	}

	private String getPartitionKeyFallback(String partitionKeyFallback) {
		switch (partitionKeyFallback) {
			case "table":
				return this.table;
			case "primary_key":
				return pkAsConcatString();
			case "database":
			default:
				return this.database;
		}
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

			if (value == null && !includeNullField)
				continue;

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

		g.writeEndObject(); // end of 'jsonMapName: { }'
	}
	
	private void writeMapToStringAsJSON(
			String jsonMapName,
			LinkedHashMap<String, Object> data,
			JsonGenerator g,
			boolean includeNullField
	) throws IOException, NoSuchAlgorithmException {
		StringWriter writer=new StringWriter();
		JsonGenerator json=jsonFactory.createGenerator(new StringWriter());

		for (String key : data.keySet()) {
			Object value = data.get(key);

			if (value == null && !includeNullField)
				continue;

			if (value instanceof List) { // sets come back from .asJSON as lists, and jackson can't deal with lists natively.
				List stringList = (List) value;

				json.writeArrayFieldStart(key);
				for (Object s : stringList) {
					json.writeObject(s);
				}
				json.writeEndArray();
			} else if (value instanceof RawJSONString) {
				// JSON column type, using binlog-connector's serializers.
				json.writeFieldName(key);
				json.writeRawValue(((RawJSONString) value).json);
			} else {
				json.writeObjectField(key, value);
			}
		}
		g.writeStringField(jsonMapName, writer.toString());
	}


	public String toJSON() throws Exception {
		return toJSON(new MaxwellOutputConfig());
	}

	public String toJSON(MaxwellOutputConfig outputConfig) throws Exception {
		JsonGenerator g = resetJsonGenerator();
		

		g.writeStartObject(); // start of row {

		g.writeStringField(outputConfig.prefixString + "database", this.database);
		g.writeStringField(outputConfig.prefixString + "table", this.table);
		g.writeStringField(outputConfig.prefixString + "type", this.rowType);
		g.writeNumberField(outputConfig.prefixString + "ts", this.timestampSeconds);
		if (outputConfig.includesTimeStampMs) {
			g.writeNumberField(outputConfig.prefixString + "tsm", this.timestampMillis);
		}

		if ( outputConfig.includesCommitInfo ) {
			if ( this.xid != null )
				g.writeNumberField(outputConfig.prefixString + "xid", this.xid);

			if ( this.txCommit )
				g.writeBooleanField(outputConfig.prefixString + "commit", true);
		}

		BinlogPosition binlogPosition = this.nextPosition.getBinlogPosition();
		if ( outputConfig.includesBinlogPosition )
			g.writeStringField(outputConfig.prefixString + "position", binlogPosition.getFile() + ":" + binlogPosition.getOffset());


		if ( outputConfig.includesGtidPosition)
			g.writeStringField(outputConfig.prefixString + "gtid", binlogPosition.getGtid());

		if ( outputConfig.includesServerId && this.serverId != null ) {
			g.writeNumberField(outputConfig.prefixString + "server_id", this.serverId);
		}

		if ( outputConfig.includesThreadId && this.threadId != null ) {
			g.writeNumberField(outputConfig.prefixString + "thread_id", this.threadId);
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
					}
				}
			}
		}


		EncryptionContext encryptionContext = null;
		if (outputConfig.encryptionEnabled()) {
			encryptionContext = EncryptionContext.create(outputConfig.secretKey);
		}

		DataJsonGenerator dataWriter = outputConfig.encryptionMode == EncryptionMode.ENCRYPT_DATA
			? encryptingJsonGeneratorThreadLocal.get()
			: plaintextDataGeneratorThreadLocal.get();

			
			
		JsonGenerator dataGenerator = dataWriter.begin();
		if (outputConfig.flattenData) {
			writeMapToJSON(outputConfig.prefixString + "data", this.data, dataGenerator, outputConfig.includesNulls);
			if( !this.oldData.isEmpty() ){
				writeMapToJSON(outputConfig.prefixString + "old", this.oldData, dataGenerator, outputConfig.includesNulls);
			}			
		} else {
			writeMapToStringAsJSON(outputConfig.prefixString + "data", this.data, dataGenerator, outputConfig.includesNulls);
				if( !this.oldData.isEmpty() ){
					writeMapToStringAsJSON(outputConfig.prefixString + "old", this.oldData, dataGenerator, outputConfig.includesNulls);
				}
		}
		dataWriter.end(encryptionContext);

		g.writeEndObject(); // end of row
		g.flush();

		if(outputConfig.encryptionMode == EncryptionMode.ENCRYPT_ALL){
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

	public Object getData(String key) {
		return this.data.get(key);
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

	public Object getOldData(String key) {
		return this.oldData.get(key);
	}

	public void putOldData(String key, Object value) {
		this.oldData.put(key, value);

		this.approximateSize += approximateKVSize(key, value);
	}

	public Position getPosition() {
		return nextPosition;
	}

	public Long getXid() {
		return xid;
	}

	public void setXid(Long xid) {
		this.xid = xid;
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

	public String getRowType() {
		return this.rowType;
	}

	// determines whether there is anything for the producer to output
	// override this for extended classes that don't output a value
	// return false when there is a heartbeat row or other row with suppressed output
	public boolean shouldOutput(MaxwellOutputConfig outputConfig) {
		return true;
	}

	public LinkedHashMap<String, Object> getData()
	{
		return new LinkedHashMap<>(data);
	}

	public LinkedHashMap<String, Object> getOldData()
	{
		return new LinkedHashMap<>(oldData);
	}
}
