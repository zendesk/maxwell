package com.zendesk.exodus.schema.column;

import com.google.code.or.common.util.MySQLConstants;

public class DecimalColumn extends Column {

	public DecimalColumn(String tableName, String name, String type, int pos) {
		super(tableName, name, type, pos);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_DECIMAL;
	}

}
