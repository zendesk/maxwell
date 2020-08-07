package com.zendesk.maxwell.schema.columndef;

import com.zendesk.maxwell.producer.MaxwellOutputConfig;

import java.sql.Timestamp;

public class DateTimeColumnDef extends ColumnDefWithLength {
	public DateTimeColumnDef(String name, String type, short pos, Long columnLength) {
		super(name, type, pos, columnLength);
	}

	final private boolean isTimestamp = getType().equals("timestamp");
	protected String formatValue(Object value, MaxwellOutputConfig config) throws ColumnDefCastException {
		// special case for those broken mysql dates.
		if ( value instanceof Long ) {
			Long v = (Long) value;
			if ( v == Long.MIN_VALUE || (v == 0L && isTimestamp) ) {
				if ( config.zeroDatesAsNull )
					return null;
				else
					return appendFractionalSeconds("0000-00-00 00:00:00", 0, columnLength);
			}
		}

		try {
			Timestamp ts = DateFormatter.extractTimestamp(value);
			String dateString = DateFormatter.formatDateTime(value, ts);
			return appendFractionalSeconds(dateString, ts.getNanos(), columnLength);
		} catch ( IllegalArgumentException e ) {
			throw new ColumnDefCastException(this, value);
		}
	}
}
