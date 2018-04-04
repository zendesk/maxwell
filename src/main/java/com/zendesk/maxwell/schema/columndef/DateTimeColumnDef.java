package com.zendesk.maxwell.schema.columndef;

import java.sql.Timestamp;

import com.google.code.or.common.util.MySQLConstants;

public class DateTimeColumnDef extends ColumnDefWithLength {
	public DateTimeColumnDef(String name, String type, int pos, Long columnLength) {
		super(name, type, pos, columnLength);
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

	protected String formatValue(Object value) {
		// special case for those broken mysql dates.
		if ( value instanceof Long && (Long) value == 0L ) {
			return appendFractionalSeconds("0000-00-00 00:00:00", 0, columnLength);
		}

		Timestamp ts = DateFormatter.extractTimestamp(value);
		String dateString = DateFormatter.formatDateTime(value, ts);
		if ( dateString == null )
			return null;
		else
			return appendFractionalSeconds(dateString, ts.getNanos(), columnLength);
	}
}
