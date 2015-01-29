package com.zendesk.exodus.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;

public class DecimalColumnDef extends ColumnDef {

	public DecimalColumnDef(String tableName, String name, String type, int pos) {
		super(tableName, name, type, pos);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_DECIMAL;
	}

}
