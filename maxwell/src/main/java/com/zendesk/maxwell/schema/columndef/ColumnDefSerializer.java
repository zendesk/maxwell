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
		jgen.writeStringField("type", def.type);
		jgen.writeStringField("name", def.name);

		if ( def instanceof StringColumnDef )
			jgen.writeStringField("charset", ((StringColumnDef) def).charset);
		else if ( def instanceof IntColumnDef )
			jgen.writeBooleanField("signed", ((IntColumnDef) def).isSigned());
		else if ( def instanceof BigIntColumnDef )
			jgen.writeBooleanField("signed", ((BigIntColumnDef) def).isSigned());
		else if ( def instanceof EnumeratedColumnDef ) {
			jgen.writeArrayFieldStart("enum-values");
			for ( String s : ((EnumeratedColumnDef) def).getEnumValues() )
				jgen.writeString(s);
			jgen.writeEndArray();
		}
		jgen.writeEndObject();
	}
}
