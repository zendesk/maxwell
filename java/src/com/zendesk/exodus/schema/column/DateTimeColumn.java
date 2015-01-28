package com.zendesk.exodus.schema.column;

import com.google.code.or.common.util.MySQLConstants;

public class DateTimeColumn extends Column {
	public DateTimeColumn(String tableName, String name, String type, int pos) {
		super(tableName, name, type, pos);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		if ( getType().equals("datetime") ) {
			return type == MySQLConstants.TYPE_DATETIME ||
				   type == MySQLConstants.TYPE_DATETIME2;
		} else {
			return type == MySQLConstants.TYPE_TIMESTAMP ||
				   type == MySQLConstants.TYPE_TIMESTAMP2;
		}
	}
}
