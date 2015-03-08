package com.zendesk.maxwell.schema.columndef;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.google.code.or.common.util.MySQLConstants;

public class DateTimeColumnDef extends ColumnDef {
	public DateTimeColumnDef(String tableName, String name, String type, int pos) {
		super(tableName, name, type, pos);
	}

	private static SimpleDateFormat dateTimeFormatter;
	private static final TimeZone tz = TimeZone.getTimeZone("UTC");

	protected static SimpleDateFormat getDateTimeFormatter() {
		if ( dateTimeFormatter == null ) {
			dateTimeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			dateTimeFormatter.setTimeZone(tz);
		}
		return dateTimeFormatter;
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

	private String formatValue(Object value) {
		if ( value instanceof Long && getType().equals("datetime") )
			return formatLong((Long) value);
		else if ( value instanceof Date )
			return getDateTimeFormatter().format((Date) value);
		else if ( value instanceof Timestamp )
			return getDateTimeFormatter().format((Timestamp) value);
		else
			return "";
	}

	private String formatLong(Long value) {
		final int second = (int)(value % 100); value /= 100;
		final int minute = (int)(value % 100); value /= 100;
		final int hour = (int)(value % 100); value /= 100;
		final int day = (int)(value % 100); value /= 100;
		final int month = (int)(value % 100);
		final int year = (int)(value / 100);

		return String.format("%04d-%02d-%02d %02d:%02d:%02d",  year, month, day, hour, minute, second);
	}


	@Override
	public String toSQL(Object value) {
		return "'" + formatValue(value) + "'";
	}


	@Override
	public Object asJSON(Object value) {
		return formatValue(value);
	}
}
