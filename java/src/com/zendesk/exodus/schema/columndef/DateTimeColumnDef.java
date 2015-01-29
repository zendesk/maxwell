package com.zendesk.exodus.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;

public class DateTimeColumnDef extends ColumnDef {
	public DateTimeColumnDef(String tableName, String name, String type, int pos) {
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
