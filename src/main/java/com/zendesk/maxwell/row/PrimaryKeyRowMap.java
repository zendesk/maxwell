package com.zendesk.maxwell.row;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

public class PrimaryKeyRowMap implements Serializable {

	private final Map<String, Object> primaryKeyColumns;
	private String database;
	private String table;
	private JsonSerializer serializer = new JsonSerializer();

	public PrimaryKeyRowMap(String database, String table, Map<String, Object> primaryKeyColumns) {
		this.database = database;
		this.table = table;
		this.primaryKeyColumns = primaryKeyColumns;
	}

	public String toJsonHash() throws IOException {
		JsonGenerator g = serializer.resetJsonGenerator();

		g.writeStartObject(); // start of row {

		g.writeStringField(FieldNames.DATABASE, database);
		g.writeStringField(FieldNames.TABLE, table);


		if (primaryKeyColumns != null) {
			if (primaryKeyColumns.isEmpty()) {
				g.writeStringField(FieldNames.UUID, UUID.randomUUID().toString());
			} else {
				for (String pk : primaryKeyColumns.keySet()) {
					serializer.writeValueToJSON(g, true, "pk." + pk.toLowerCase(), primaryKeyColumns.get(pk));
				}
			}
		}

		g.writeEndObject();
		g.flush();
		return serializer.jsonFromStream();
	}

	public String toJsonHashWithReason(String reason) throws IOException {
		JsonGenerator g = serializer.resetJsonGenerator();

		g.writeStartObject(); // start of row {

		g.writeStringField(FieldNames.DATABASE, database);
		g.writeStringField(FieldNames.TABLE, table);
		if (reason != null) {
			g.writeStringField("reason", reason);
		}
		g.writeObjectFieldStart(FieldNames.DATA);

		if (primaryKeyColumns != null) {
			for (String pk : primaryKeyColumns.keySet()) {
				serializer.writeValueToJSON(g, true, pk.toLowerCase(), primaryKeyColumns.get(pk));
			}
		}
		g.writeEndObject(); // end of 'data: { }'
		g.writeEndObject(); // end of json
		g.flush();
		return serializer.jsonFromStream();
	}

	@Override
	public String toString() {
		return database + ":" + table + ":" + primaryKeyColumns;
	}
}
