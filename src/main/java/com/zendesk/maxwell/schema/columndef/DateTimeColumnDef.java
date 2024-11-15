package com.zendesk.maxwell.schema.columndef;

import com.zendesk.maxwell.producer.MaxwellOutputConfig;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class DateTimeColumnDef extends ColumnDefWithLength {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

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
		if ( value instanceof String) {
			String dateString = (String) value;

			if ( "0000-00-00 00:00:00".equals(dateString) ) {
				if ( config.zeroDatesAsNull )
					return null;
				else 
					return appendFractionalSeconds("0000-00-00 00:00:00", 0, getColumnLength());
			} else {
				if ( !DateValidator.isValidDateTime(dateString) )
					return null;

				value = parseDateTime(dateString);
				if (value == null) {
					return null;
				}
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

	private Object parseDateTime(String dateString) {
		try {
			return LocalDateTime.parse(dateString, DATE_TIME_FORMATTER);
		} catch (DateTimeParseException e) {
			return null;
		}
	}
}
