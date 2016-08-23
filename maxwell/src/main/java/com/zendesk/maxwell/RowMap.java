package com.zendesk.maxwell;

import com.fasterxml.jackson.core.*;
import com.google.code.or.common.glossary.Column;
import com.zendesk.maxwell.producer.AbstractProducer;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;


//Dave Added
import org.json.JSONObject;

public class RowMap implements Serializable {
	public enum KeyFormat { HASH, ARRAY }

	static final Logger LOGGER = LoggerFactory.getLogger(RowMap.class);

	private final String rowType;
	private final String database;
	private final String table;
	private final Long timestamp;
	private final BinlogPosition nextPosition;


	private Long xid;
	private boolean txCommit;

	private final LinkedHashMap<String, Object> data;
	private final LinkedHashMap<String, Object> oldData;
	private final List<String> pkColumns;
	private List<Pattern> excludeColumns;
	private static Boolean includeNulls;
	private static MaxwellContext context;


	private static final JsonFactory jsonFactory = new JsonFactory();

	private static final ThreadLocal<ByteArrayOutputStream> byteArrayThreadLocal =
			new ThreadLocal<ByteArrayOutputStream>(){
				@Override
				protected ByteArrayOutputStream initialValue() {
					return new ByteArrayOutputStream();
				}
			};

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

	public RowMap(String type, String database, String table, Long timestamp, List<String> pkColumns,
				  BinlogPosition nextPosition) {
		this.rowType = type;
		this.database = database;
		this.table = table;
		this.timestamp = timestamp;
		this.data = new LinkedHashMap<>();
		this.oldData = new LinkedHashMap<>();
		this.nextPosition = nextPosition;
		this.pkColumns = pkColumns;
	}



	public RowMap(String type, String database, String table, Long timestamp, List<String> pkColumns,
				  BinlogPosition nextPosition, List<Pattern> excludeColumns) {
		this(type, database, table, timestamp, pkColumns, nextPosition);
		this.excludeColumns = excludeColumns;
	}

	public String pkToJson(KeyFormat keyFormat) throws IOException {
		if ( keyFormat == KeyFormat.HASH )
			return pkToJsonHash();
		else
			return pkToJsonArray();
	}

	private String pkToJsonHash() throws IOException {
		JsonGenerator g = jsonGeneratorThreadLocal.get();

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

				g.writeObjectField("pk." + pk, pkValue);
			}
		}

		g.writeEndObject(); // end of 'data: { }'
		g.flush();
		return jsonFromStream();
	}

	private String pkToJsonArray() throws IOException {
		JsonGenerator g = jsonGeneratorThreadLocal.get();

		g.writeStartArray();
		g.writeString(database);
		g.writeString(table);

		g.writeStartArray();
		for (String pk : pkColumns) {
			Object pkValue = null;
			if ( data.containsKey(pk) )
				pkValue = data.get(pk);

			g.writeStartObject();
			g.writeObjectField(pk, pkValue);
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
		String keys="";
		for (String pk : pkColumns) {
			Object pkValue = null;
			if (data.containsKey(pk))
				pkValue = data.get(pk);
			if (pkValue != null)
				keys += pkValue.toString();
		}
		if (keys.isEmpty())
			return "None";
		return keys;
	}

	private void writeListToJSON(List<String> inputList, String key, JsonGenerator generator) throws IOException {

		generator.writeArrayFieldStart(key);
		for ( String s : inputList )  {
			if(data.get(s) == null){
				generator.writeObject(data.get(s.toLowerCase()));
			}
			else{
				generator.writeString(data.get(s.toLowerCase()).toString());
			}
		}
		generator.writeEndArray();
	}

	private void writeMapToJSON(String jsonMapName, LinkedHashMap<String, Object> data, boolean includeNullField) throws IOException {
		JsonGenerator generator = jsonGeneratorThreadLocal.get();
		generator.writeObjectFieldStart(jsonMapName); // start of jsonMapName: {

		/* TODO: maintain ordering of fields in column order */
		for ( String key: data.keySet() ) {
			Object value = data.get(key);

			if ( value == null && !includeNullField)
				continue;

			if ( value instanceof List) { // sets come back from .asJSON as lists, and jackson can't deal with lists natively.
				generator.writeArrayFieldStart(key);
				for(Object s: (List) value){
					generator.writeObject(s);
				}
				generator.writeEndArray();
			} else {
				if ( value != null ) {



					//Dave Added to substring the data for Vertica
					if (value.getClass().equals(String.class)) {

						String newData;

						Integer trimString =  Maxwell.trimString;
						Boolean removeNonAscii = Maxwell.removeNonAscii;

						if(trimString == null){
							trimString = 0;
						}

						if(removeNonAscii == null){
							removeNonAscii = false;
						}

						if (Boolean.TRUE.equals(removeNonAscii)){
							newData = value.toString().replaceAll("[^\\p{ASCII}]", "");
						}
						else {
							newData = value.toString();
						}

						if ( trimString > 0 && newData.length() > trimString ) {

							value = newData.substring(1, trimString);

						}
						else {
							value = newData;
						}
					}
				}



				generator.writeObjectField(key, value);
			}
		}

		generator.writeEndObject(); // end of 'jsonMapName: { }'
		return;
	}

	public String toJSON() throws IOException {
		JsonGenerator g = jsonGeneratorThreadLocal.get();

		g.writeStartObject(); // start of row {

		g.writeStringField("database", this.database);
		g.writeStringField("table", this.table);
		g.writeStringField("type", this.rowType);
		g.writeNumberField("ts", this.timestamp);
		writeListToJSON(pkColumns, "primary_key", g);

		/* TODO: allow xid and commit to be configurable in the output */
		if ( this.xid != null )
			g.writeNumberField("xid", this.xid);

		if ( this.txCommit )
			g.writeBooleanField("commit", true);

		if ( this.excludeColumns != null ) {
			// NOTE: to avoid concurrent modification.
			Set<String> keys = new HashSet<String>();
			keys.addAll(this.data.keySet());
			keys.addAll(this.oldData.keySet());

			for ( Pattern p : this.excludeColumns ) {
				for ( String key : keys ) {
					if ( p.matcher(key).matches() ) {
						this.data.remove(key);
						this.oldData.remove(key);
					}
				}
			}
		}

		includeNulls = Maxwell.includeNulls;

		if(includeNulls == null)
			includeNulls = false;

		if (includeNulls) {
			writeMapToJSON("data", this.data, true);
		}
		else {
			writeMapToJSON("data", this.data, false);
		}


		if ( !this.oldData.isEmpty() ) {
			writeMapToJSON("old", this.oldData, true);
		}

		g.writeEndObject(); // end of row
		g.flush();

		return jsonFromStream();
	}

	private String jsonFromStream() {
		ByteArrayOutputStream b = byteArrayThreadLocal.get();
		String s = b.toString();
		b.reset();
		//Dave To encrypt
		String JSON = encryptData(s);
		return JSON;
	}

	public Object getData(String key) {
		return this.data.get(key);
	}

	public void putData(String key, Object value) {
		this.data.put(key,  value);
	}

	public Object getOldData(String key) {
		return this.oldData.get(key);
	}

	public void putOldData(String key, Object value) {
		this.oldData.put(key,  value);
	}

	public BinlogPosition getPosition() {
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

	public String getDatabase() {
		return database;
	}

	public String getTable() {
		return table;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public boolean hasData(String name) {
		return this.data.containsKey(name);
	}

	// Dave Created to encrypt data
	private String encryptData(String json) {
		Boolean useEncryption = Maxwell.useEncryption;
		String jsonOutput;
		try {
			if (useEncryption) {
				JSONObject obj = new JSONObject(json);
				String dataObject = obj.getJSONObject("data").toString();
				String encryptData = MaxwellEncrypt.encrypt(dataObject);
				obj.put("data", encryptData);
				if (!this.oldData.isEmpty() ) {
					String oldObject = obj.getJSONObject("old").toString();
					String encryptOld = MaxwellEncrypt.encrypt(oldObject);
					obj.put("old", encryptOld);
				}
				jsonOutput = obj.toString();
				return jsonOutput;
			} else
				return json;
		} catch (Exception e) {
			return json;
		}
	}
}