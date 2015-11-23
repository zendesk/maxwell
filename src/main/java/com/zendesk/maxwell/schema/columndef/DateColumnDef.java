package com.zendesk.maxwell.schema.columndef;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.google.code.or.common.util.MySQLConstants;

public class DateColumnDef extends ColumnDef {
	public DateColumnDef(String tableName, String name, String type, int pos) {
		super(tableName, name, type, pos);
	}

	private static SimpleDateFormat dateFormatter;
	private static final TimeZone tz = TimeZone.getTimeZone("UTC");

	protected static SimpleDateFormat getDateFormatter() {
		if ( dateFormatter == null ) {
			dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
			dateFormatter.setTimeZone(tz);
		}
		return dateFormatter;
	}


	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_DATE;
	}

	private String formatDate(Object value) {
		return getDateFormatter().format((Date) value);
	}

	@Override
	public String toSQL(Object value) {
		return "'" + formatDate(value) + "'";
	}

	@Override
	public Object asJSON(Object value) {
		return formatDate(value);
	}
}
