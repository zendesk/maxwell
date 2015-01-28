package com.zendesk.exodus.schema.column;

import com.google.code.or.common.util.MySQLConstants;

public class FloatColumn extends Column {

	public FloatColumn(String tableName, String name, String type, int pos) {
		super(tableName, name, type, pos);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		if ( getType().equals("float"))
			return type == MySQLConstants.TYPE_FLOAT;
		else
			return type == MySQLConstants.TYPE_DOUBLE;
	}
}
