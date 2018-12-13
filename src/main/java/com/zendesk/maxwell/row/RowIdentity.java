package com.zendesk.maxwell.row;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class RowIdentity implements Serializable {
	private final List<Map.Entry<String, Object>> primaryKeyColumns;

	private String database;
	private String table;
	private JsonSerializer serializer = new JsonSerializer();

	// slightly less verbose pair construction
	public static Map.Entry<String, Object> pair(String key, Object value) {
		return new AbstractMap.SimpleImmutableEntry<>(key, value);
	}

	public static List<Map.Entry<String, Object>> pairs(Map.Entry<String, Object> ... entries) {
		return Arrays.asList(entries);
	}

	public RowIdentity(String database, String table, List<Map.Entry<String, Object>> primaryKeyColumns) {
		this.database = database;
		this.table = table;
		this.primaryKeyColumns = primaryKeyColumns;
	}

	public String getDatabase() {
		return database;
	}

	public String getTable() {
		return table;
	}

	public String toKeyJson(RowMap.KeyFormat keyFormat) throws IOException {
		if ( keyFormat == RowMap.KeyFormat.HASH )
			return toKeyJsonHash();
		else
			return toKeyJsonArray();
	}

	private String toKeyJsonHash() throws IOException {
		JsonGenerator g = writeStartCommon();
		if (primaryKeyColumns.isEmpty()) {
			g.writeStringField(FieldNames.UUID, UUID.randomUUID().toString());
		} else {
			for (Map.Entry<String,Object> pk : primaryKeyColumns) {
				writePrimaryKey(g, "pk." + pk.getKey().toLowerCase(), pk.getValue());
			}
		}

		return writeEndCommon(g);
	}

	private String toKeyJsonArray() throws IOException {
		JsonGenerator g = serializer.resetJsonGenerator();

		g.writeStartArray();
		g.writeString(database);
		g.writeString(table);

		g.writeStartArray();
		for (Map.Entry<String,Object> pk : primaryKeyColumns) {
			g.writeStartObject();
			serializer.writeValueToJSON(g, true, pk.getKey().toLowerCase(), pk.getValue());
			g.writeEndObject();
		}
		g.writeEndArray();
		g.writeEndArray();
		g.flush();
		return serializer.jsonFromStream();
	}

	public String toFallbackValueWithReason(String reason) throws IOException {
		JsonGenerator g = writeStartCommon();

		g.writeStringField(FieldNames.REASON, reason);

		g.writeObjectFieldStart(FieldNames.DATA);
		for (Map.Entry<String,Object> pk : primaryKeyColumns) {
			writePrimaryKey(g, pk);
		}
		g.writeEndObject(); // end of 'data: { }'

		return writeEndCommon(g);
	}

	public String toConcatString() {
		if (primaryKeyColumns.isEmpty()) {
			return database + table;
		}
		StringBuilder keys = new StringBuilder();
		for (Map.Entry<String, Object> pk : primaryKeyColumns) {
			Object pkValue = pk.getValue();
			if (pkValue != null) {
				keys.append(pkValue.toString());
			}
		}
		if (keys.length() == 0) {
			return "None";
		}
		return keys.toString();
	}

	private void writePrimaryKey(JsonGenerator g, String jsonKey, Object value) throws IOException {
		serializer.writeValueToJSON(g, true, jsonKey, value);
	}

	private void writePrimaryKey(JsonGenerator g, Map.Entry<String,Object> pk) throws IOException {
		writePrimaryKey(g, pk.getKey(), pk.getValue());
	}

	private JsonGenerator writeStartCommon() throws IOException {
		JsonGenerator g = serializer.resetJsonGenerator();

		g.writeStartObject(); // start of row {

		g.writeStringField(FieldNames.DATABASE, database);
		g.writeStringField(FieldNames.TABLE, table);
		return g;
	}

	private String writeEndCommon(JsonGenerator g) throws IOException {
		g.writeEndObject();
		g.flush();
		return serializer.jsonFromStream();
	}


	@Override
	public String toString() {
		return database + ":" + table + ":" + primaryKeyColumns;
	}
}
