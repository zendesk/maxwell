package com.zendesk.maxwell.schema.columndef;

import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;
import com.zendesk.maxwell.row.RawJSONString;

import java.io.IOException;

import static com.github.shyiko.mysql.binlog.event.deserialization.ColumnType.*;

public class JsonColumnDef extends ColumnDef {
	public JsonColumnDef(String name, String type, int pos) {
		super(name, type, pos);
	}

	@Override
	public Object asJSON(Object value) {
		String jsonString;

		if ( value instanceof String ) {
			jsonString = (String) value;
		} else {
			try {
				byte[] bytes = (byte[]) value;
				jsonString = bytes.length > 0 ? JsonBinary.parseAsString(bytes) : "null";
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return new RawJSONString(jsonString);
	}

	@Override
	public String toSQL(Object value) {
		return null;
	}
}
