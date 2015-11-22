package com.zendesk.maxwell;

import com.fasterxml.jackson.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class RowMap extends HashMap<String, Object> {
	static final Logger LOGGER = LoggerFactory.getLogger(RowMap.class);
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
		this.put("data", this.data);
	}


	public String toJSON() throws IOException {
		JsonGenerator g = jsonGeneratorThreadLocal.get();

		g.writeStartObject(); // start of row {

		g.writeStringField("database", (String) this.get("database"));
		g.writeStringField("table", (String) this.get("table"));
		g.writeStringField("type", (String) this.get("type"));
		g.writeNumberField("ts", (Long) this.get("ts"));

		/* TODO: allow xid and commit to be configurable */
		if ( this.containsKey("xid") )
			g.writeObjectField("xid", this.get("xid"));

		if ( this.containsKey("commit") && (boolean) this.get("commit") == true)
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
		this.put("type", type);
	}

	public Object getData(String key) {
		return this.data.get(key);
	}

	public void putData(String key, Object value) {
		this.data.put(key,  value);
	}

	public void setTable(String name) {
		this.put("table", name);
	}

	public void setDatabase(String name) {
		this.put("database", name);
	}

	public void setTimestamp(Long l) {
		this.put("ts", l);
	}

	public void setXid(Long xid) {
		this.put("xid", xid);
	}

	public void setTXCommit() {
		this.put("commit", true);
	}

	public Set<String> dataKeySet() {
		return this.data.keySet();
	}
}
