package com.zendesk.maxwell.schema.columndef;

import java.sql.Time;

import com.google.code.or.common.util.MySQLConstants;

public class TimeColumnDef extends ColumnDef {
	public TimeColumnDef(String tableName, String name, String type, int pos) {
		super(tableName, name, type, pos);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_TIME;
	}

	@Override
	public String toSQL(Object value) {
		Time t = (Time) value;
		return "'" + String.valueOf(t) + "'";
	}

	@Override
	public Object asJSON(Object value) {
		return String.valueOf((Time) value);
	}
}
