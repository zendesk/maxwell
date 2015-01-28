package com.zendesk.exodus.schema.column;

import com.google.code.or.common.util.MySQLConstants;

public class StringColumn extends Column {
	private final String encoding;

	public StringColumn(String tableName, String name, String type, int pos, String encoding) {
		super(tableName, name, type, pos);
		this.encoding = encoding;
	}
	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_BLOB ||
			   type == MySQLConstants.TYPE_VARCHAR ||
			   type == MySQLConstants.TYPE_STRING;
	}
}
