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
	public boolean matchesMysqlType(int type) {
		return JSON.getCode() == type;
	}

	@Override
	public Object asJSON(Object value) {
		try {
			String jsonString = JsonBinary.parseAsString((byte[]) value);
			return new RawJSONString(jsonString);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String toSQL(Object value) {
		return null;
	}
}
