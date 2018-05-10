package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;

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
		String formatted = DateFormatter.formatDate(value);
		if ( formatted == null )
			return null;
		else
			return "'" +  formatted + "'";
	}

	@Override
	public Object asJSON(Object value) {
		if ( value instanceof Long && (Long) value == Long.MIN_VALUE )
			return "0000-00-00";

		return DateFormatter.formatDate(value);
	}
}
