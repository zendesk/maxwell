package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;

public class YearColumnDef extends ColumnDef {
	public YearColumnDef(String tableName, String name, String type, int pos) {
		super(tableName, name, type, pos);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_YEAR;
	}

	@Override
	public String toSQL(Object value) {
		return ((Integer)value).toString();
	}
}
