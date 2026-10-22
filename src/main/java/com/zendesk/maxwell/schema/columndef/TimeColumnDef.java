package com.zendesk.maxwell.schema.columndef;

import com.zendesk.maxwell.producer.MaxwellOutputConfig;

import java.sql.Time;
import java.sql.Timestamp;

public class TimeColumnDef extends ColumnDefWithLength {
	private TimeColumnDef(String name, String type, short pos, Long columnLength) {
		super(name, type, pos, columnLength);
	}

	public static TimeColumnDef create(String name, String type, short pos, Long columnLength) {
		TimeColumnDef temp = new TimeColumnDef(name, type, pos, columnLength);
		return (TimeColumnDef) INTERNER.intern(temp);
	}

	protected String formatValue(Object value, MaxwellOutputConfig config) throws ColumnDefCastException {
		if ( value instanceof Timestamp timestamp ) {
			Time time = new Time(timestamp.getTime());
			String timeAsStr = String.valueOf(time);

			return appendFractionalSeconds(timeAsStr, timestamp.getNanos(), this.getColumnLength());

		} else if ( value instanceof Long long1 ) {
			Time time = new Time(long1 / 1000);
			String timeAsStr = String.valueOf(time);

			return appendFractionalSeconds(timeAsStr, (int) (long1 % 1000000) * 1000, this.getColumnLength());
		} else if ( value instanceof Time time ){
			return String.valueOf(time);
		} else {
			throw new ColumnDefCastException(this, value);
		}
	}
}
