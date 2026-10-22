package com.zendesk.maxwell.schema.columndef;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class ColumnDefSerializer extends JsonSerializer<ColumnDef> {
	@Override
	public void serialize(ColumnDef def, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
		jgen.writeStartObject();
		jgen.writeStringField("type", def.getType());
		jgen.writeStringField("name", def.getName());

		switch (def) {
			case StringColumnDef columnDef3 -> jgen.writeStringField("charset", columnDef3.getCharset());
			case IntColumnDef columnDef2 -> jgen.writeBooleanField("signed", columnDef2.isSigned());
			case BigIntColumnDef columnDef1 -> jgen.writeBooleanField("signed", columnDef1.isSigned());
			case EnumeratedColumnDef columnDef -> {
				jgen.writeArrayFieldStart("enum-values");
				for (String s : columnDef.getEnumValues())
					jgen.writeString(s);
				jgen.writeEndArray();
			}
			case ColumnDefWithLength length -> {
				// columnLength is a long but technically, it' not that long. It it were, we could
				// need to use a string to represent it, instead of an integer, to avoid issues
				// with Javascript when parsing long integers.
				Long columnLength = length.getColumnLength();
				if (columnLength != null)
					jgen.writeNumberField("column-length", columnLength);
			}
			case null, default -> {}
		}
		jgen.writeEndObject();
	}
}
