package com.zendesk.exodus.schema.column;

import com.google.code.or.common.util.MySQLConstants;

public class BigIntColumn extends Column {
	private final boolean signed;

	public BigIntColumn(String tableName, String name, String type, int pos, boolean signed) {
		super(tableName, name, type, pos);
		this.signed = signed;
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_LONGLONG;
	}
}
