package com.zendesk.maxwell;

import com.fasterxml.jackson.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class RowMap {
	static final Logger LOGGER = LoggerFactory.getLogger(RowMap.class);

	private String rowType;
	private String database;
	private String table;
	private Long timestamp;
	private Long xid;
	private boolean txCommit;

	private final HashMap<String, Object> data;

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


	public RowMap() {
		this.data = new HashMap<>();
	}

	public String toJSON() throws IOException {
		JsonGenerator g = jsonGeneratorThreadLocal.get();

		g.writeStartObject(); // start of row {

		g.writeStringField("database", this.database);
		g.writeStringField("table", this.table);
		g.writeStringField("type", this.rowType);
		g.writeNumberField("ts", this.timestamp);

		/* TODO: allow xid and commit to be configurable in the output */
		if ( this.xid != null )
			g.writeNumberField("xid", this.xid);

		if ( this.txCommit )
			g.writeBooleanField("commit", true);

		g.writeObjectFieldStart("data"); // start of data: {

		/* TODO: maintain ordering of fields in column order */
		for ( String key: this.data.keySet() ) {
			Object data = this.data.get(key);

			if ( data == null )
				continue;

			if ( data instanceof List) { // sets come back from .asJSON as lists, and jackson can't deal with lists natively.
				List<String> stringList = (List<String>) data;

				g.writeArrayFieldStart(key);
				for ( String s : stringList )  {
					g.writeString(s);
				}
				g.writeEndArray();
			} else {
				g.writeObjectField(key, data);
			}
		}
		g.writeEndObject(); // end of 'data: { }'
		g.writeEndObject(); // end of row
		g.flush();

		ByteArrayOutputStream b = byteArrayThreadLocal.get();
		String s = b.toString();
		b.reset();
		return s;
	}


	public void setRowType(String type) {
		this.rowType = type;
	}

	public Object getData(String key) {
		return this.data.get(key);
	}

	public void putData(String key, Object value) {
		this.data.put(key,  value);
	}

	public void setTable(String name) {
		this.table = name;
	}

	public void setDatabase(String name) {
		this.database = name;
	}

	public void setTimestamp(Long l) {
		this.timestamp = l;
	}

	public void setXid(Long xid) {
		this.xid = xid;
	}

	public void setTXCommit() {
		this.txCommit = true;
	}

	public Set<String> dataKeySet() {
		return this.data.keySet();
	}
}
