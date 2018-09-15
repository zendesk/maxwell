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
		jgen.writeStringField("name", def.name);

		if ( def instanceof StringColumnDef ) {
			jgen.writeStringField("charset", ((StringColumnDef) def).getCharset());
		} else if ( def instanceof IntColumnDef ) {
			jgen.writeBooleanField("signed", ((IntColumnDef) def).isSigned());
		} else if ( def instanceof BigIntColumnDef ) {
			jgen.writeBooleanField("signed", ((BigIntColumnDef) def).isSigned());
		} else if ( def instanceof EnumeratedColumnDef ) {
			jgen.writeArrayFieldStart("enum-values");
			for (String s : ((EnumeratedColumnDef) def).getEnumValues())
				jgen.writeString(s);
			jgen.writeEndArray();
		} else if ( def instanceof ColumnDefWithLength ) {
			// columnLength is a long but technically, it' not that long. It it were, we could
			// need to use a string to represent it, instead of an integer, to avoid issues
			// with Javascript when parsing long integers.
			Long columnLength = ( (ColumnDefWithLength) def ).getColumnLength();
			if ( columnLength != null )
				jgen.writeNumberField("column-length", columnLength);
		}
		jgen.writeEndObject();
	}
}
