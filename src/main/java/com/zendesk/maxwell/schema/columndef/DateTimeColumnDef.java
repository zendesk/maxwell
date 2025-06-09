package com.zendesk.maxwell.schema.columndef;

import com.zendesk.maxwell.producer.MaxwellOutputConfig;

import java.sql.Timestamp;

public class DateTimeColumnDef extends ColumnDefWithLength {

	private final boolean isTimestamp = getType().equals("timestamp");

	private DateTimeColumnDef(String name, String type, short pos, Long columnLength) {
		super(name, type, pos, columnLength);
	}

	public static DateTimeColumnDef create(String name, String type, short pos, Long columnLength) {
		DateTimeColumnDef temp = new DateTimeColumnDef(name, type, pos, columnLength);
		return (DateTimeColumnDef) INTERNER.intern(temp);
	}

	protected String formatValue(Object value, MaxwellOutputConfig config) throws ColumnDefCastException {
		// special case for those broken mysql dates.
		if ( value instanceof String ) {
			String dateStr = (String) value;
			// bootstrapper just gives up on bothering with date processing
			if ( config.zeroDatesAsNull && dateStr.length() == 19 && 
			((dateStr.charAt(0) == '0' && dateStr.charAt(1) == '0' && dateStr.charAt(2) == '0' && dateStr.charAt(3) == '0') ||
            (dateStr.charAt(5) == '0' && dateStr.charAt(6) == '0') ||
            (dateStr.charAt(8) == '0' && dateStr.charAt(9) == '0'))) {
				return null;
			} else {
				return appendFractionalSeconds(dateStr, 0, getColumnLength());
			}
		} else if ( value instanceof Long ) {
			Long v = (Long) value;
			if ( v == Long.MIN_VALUE || (v == 0L && isTimestamp) ) {
				if ( config.zeroDatesAsNull )
					return null;
				else
					return appendFractionalSeconds("0000-00-00 00:00:00", 0, getColumnLength());
			}
		}

		try {
			Timestamp ts = DateFormatter.extractTimestamp(value);
			String dateString = DateFormatter.formatDateTime(value, ts);
			return appendFractionalSeconds(dateString, ts.getNanos(), getColumnLength());
		} catch ( IllegalArgumentException e ) {
			throw new ColumnDefCastException(this, value);
		}
	}
}
