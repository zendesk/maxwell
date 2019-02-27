package com.zendesk.maxwell.row;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class RowIdentity implements Serializable {
	private String database;
	private String table;
	private final List<Pair<String, Object>> primaryKeyColumns;

	public RowIdentity(String database, String table, List<Pair<String, Object>> primaryKeyColumns) {
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
		MaxwellJson json = MaxwellJson.getInstance();
		JsonGenerator g = json.reset();
		if ( keyFormat == RowMap.KeyFormat.HASH ) {
			// kafka_key_format = "hash":
			// Primary key column names are prefixed with "pk."
			// e.g. { "database": "db1", "table": "users", "pk.id": 123 }

			startCommonHashFields(g);
			if (primaryKeyColumns.isEmpty()) {
				g.writeStringField(FieldNames.UUID, UUID.randomUUID().toString());
			} else {
				for (Map.Entry<String,Object> pk : primaryKeyColumns) {
					MaxwellJson.writeValueToJSON(g, true, "pk." + pk.getKey().toLowerCase(), pk.getValue());
				}
			}
			g.writeEndObject();
		}
		else {
			// kafka_key_format = "array":
			// The order of the array values is [ database, table, primary_keys_hash ]
			// e.g. [ "db1", "users", { "id": 123 } ]
			g.writeStartArray();
			g.writeString(database);
			g.writeString(table);

			g.writeStartArray();
			for (Map.Entry<String,Object> pk : primaryKeyColumns) {
				g.writeStartObject();
				MaxwellJson.writeValueToJSON(g, true, pk.getKey().toLowerCase(), pk.getValue());
				g.writeEndObject();
			}
			g.writeEndArray();
			g.writeEndArray();
		}
		return json.consume();
	}

	public String toFallbackValueWithReason(String reason) throws IOException {
		// The "skeleton row" format used when writing to `dead_letter_topic`.
		// Similar to the regular "hash" key format, except:
		//  - an additional `reason` field, which describes why the original message failed
		//  - instead of merging `pk.id` with the other fields, we populate a `data` object.
		//    This is consistent with the RowMap value format, allowing for easy parsing.
		MaxwellJson json = MaxwellJson.getInstance();
		JsonGenerator g = json.reset();
		startCommonHashFields(g);

		g.writeStringField(FieldNames.REASON, reason);

		g.writeObjectFieldStart(FieldNames.DATA);
		for (Map.Entry<String,Object> pk : primaryKeyColumns) {
			writePrimaryKey(g, pk);
		}
		g.writeEndObject(); // end of 'data: { }'

		g.writeEndObject();
		return json.consume();
	}

	public String toConcatString() {
		// Generates a concise, lossy representation of this identity (i.e you can't easily parse it).
		// Simpler than generating real JSON, used as a hash input for partition calculation.
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

	@Override
	public String toString() {
		return database + ":" + table + ":" + primaryKeyColumns;
	}

	private void startCommonHashFields(JsonGenerator g) throws IOException {
		g.writeStartObject(); // start of row {

		g.writeStringField(FieldNames.DATABASE, database);
		g.writeStringField(FieldNames.TABLE, table);
	}

	private void writePrimaryKey(JsonGenerator g, String jsonKey, Object value) throws IOException {
		MaxwellJson.writeValueToJSON(g, true, jsonKey, value);
	}

	private void writePrimaryKey(JsonGenerator g, Map.Entry<String,Object> pk) throws IOException {
		writePrimaryKey(g, pk.getKey(), pk.getValue());
	}

}
