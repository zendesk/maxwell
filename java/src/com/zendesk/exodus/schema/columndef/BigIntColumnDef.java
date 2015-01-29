package com.zendesk.exodus.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;

public class BigIntColumnDef extends ColumnDef {
	private final boolean signed;

	public BigIntColumnDef(String tableName, String name, String type, int pos, boolean signed) {
		super(tableName, name, type, pos);
		this.signed = signed;
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_LONGLONG;
	}
}
