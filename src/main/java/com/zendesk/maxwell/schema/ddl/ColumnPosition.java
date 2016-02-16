package com.zendesk.maxwell.schema.ddl;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.zendesk.maxwell.schema.Table;

import java.io.IOException;


class ColumnPositionSerializer extends JsonSerializer<ColumnPosition> {
	@Override
	public void serialize(ColumnPosition columnPosition, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
		String s = columnPosition.position.name();
		if ( s.equals("AFTER") )
			s = s + " " + columnPosition.afterColumn;

		jsonGenerator.writeString(s);
	}
}

class ColumnPositionDeserializer extends JsonDeserializer<ColumnPosition> {
	@Override
	public ColumnPosition deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
		String s = jsonParser.getValueAsString();
		ColumnPosition p = new ColumnPosition();

		try {
			String[] fields = s.split(" ", 2);
			p.position = ColumnPosition.Position.valueOf(fields[0].toUpperCase());

			if ( p.position == ColumnPosition.Position.AFTER ) {
				p.afterColumn = fields[1];
			}
		} catch ( IllegalArgumentException e ) {
			return null;
		}

		return p;
	}
}

@JsonSerialize(using = ColumnPositionSerializer.class)
@JsonDeserialize(using = ColumnPositionDeserializer.class)
public class ColumnPosition {
	enum Position { FIRST, AFTER, DEFAULT };

	public Position position;
	public String afterColumn;

	public ColumnPosition() {
		this.position = Position.DEFAULT;
	}

	public int index(Table t, Integer defaultIndex) throws SchemaSyncError {
		switch(position) {
		case FIRST:
			return 0;
		case DEFAULT:
			if ( defaultIndex != null )
				return defaultIndex;
			else
				return t.getColumnList().size();

		case AFTER:
			int afterIdx = t.findColumnIndex(afterColumn);
			if ( afterIdx == -1 )
				throw new SchemaSyncError("Could not find column " + afterColumn + " (needed in AFTER statement)");
			return afterIdx + 1;
		}
		return -1;
	}
}
