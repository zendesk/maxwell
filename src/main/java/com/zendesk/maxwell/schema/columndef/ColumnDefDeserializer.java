package com.zendesk.maxwell.schema.columndef;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.*;

import java.io.IOException;

public class ColumnDefDeserializer extends JsonDeserializer<ColumnDef> {
	@Override
	public ColumnDef deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
		JsonNode node = jp.getCodec().readTree(jp);

		String type    = node.get("type").asText();
		String name    = node.get("name").asText();
		String charset = node.path("charset").asText();

		boolean signed = node.path("signed").asBoolean(false);

		String[] enumValues = null;
		JsonNode enumValueNode = node.get("enum-values");
		if ( enumValueNode != null ) {
			ArrayList<String> values = new ArrayList<>();
			for ( JsonNode n : enumValueNode )
				values.add(n.asText());

			enumValues = (String[]) values.toArray(new String[0]);
		}

		Long columnLength = null;
		JsonNode columnLengthNode = node.get("column-length");
		if ( columnLengthNode != null ) {
			columnLength = columnLengthNode.asLong();
		}
		return ColumnDef.build(name, charset, type, (short) 0, signed, enumValues, columnLength);
	}
}
