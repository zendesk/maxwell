package com.zendesk.maxwell.schema.columndef;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import com.zendesk.maxwell.producer.MaxwellOutputConfig;

public class DateColumnDef extends ColumnDef {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

	private DateColumnDef(String name, String type, short pos) {
		super(name, type, pos);
	}

	public static DateColumnDef create(String name, String type, short pos) {
		DateColumnDef temp = new DateColumnDef(name, type, pos);
		return (DateColumnDef) INTERNER.intern(temp);
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
	public Object asJSON(Object value, MaxwellOutputConfig config) throws ColumnDefCastException {
		if ( value instanceof String ) {
			String dateString = (String) value;

			if ( config.zeroDatesAsNull && "0000-00-00".equals(dateString) )
				return null;

			if ( !DateValidator.isValidDateTime(dateString) )
				return null;

			value = parseDate(dateString);
			if (value == null) {
				return null;
			}
		} else if ( value instanceof Long && (Long) value == Long.MIN_VALUE ) {
			if ( config.zeroDatesAsNull )
				return null;
			else
				return "0000-00-00";
		}

		try {
			return DateFormatter.formatDate(value);
		} catch ( IllegalArgumentException e ) {
			throw new ColumnDefCastException(this, value);
		}
	}

	private Object parseDate(String dateString) {
		try {
			return LocalDate.parse(dateString, DATE_FORMATTER);
		} catch (DateTimeParseException e) {
			return null;
		}
	}
}
