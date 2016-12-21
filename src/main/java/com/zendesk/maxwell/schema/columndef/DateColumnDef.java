package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;
import com.zendesk.maxwell.MaxwellConfig;

public class DateColumnDef extends ColumnDef {
	public DateColumnDef(String name, String type, int pos) {
		super(name, type, pos);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_DATE;
	}

	@Override
	public String toSQL(Object value) {
		return "'" + DateFormatter.formatDate(value) + "'";
	}

	@Override
	public Object asJSON(Object value) {
		return DateFormatter.formatDate(value);
	}
}
