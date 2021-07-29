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
		if ( value instanceof Timestamp ) {
			Time time = new Time(((Timestamp) value).getTime());
			String timeAsStr = String.valueOf(time);

			return appendFractionalSeconds(timeAsStr, ((Timestamp) value).getNanos(), this.getColumnLength());

		} else if ( value instanceof Long ) {
			Time time = new Time((Long) value / 1000);
			String timeAsStr = String.valueOf(time);

			return appendFractionalSeconds(timeAsStr, (int) ((Long) value % 1000000) * 1000, this.getColumnLength());
		} else if ( value instanceof Time ){
			return String.valueOf((Time) value);
		} else {
			throw new ColumnDefCastException(this, value);
		}
	}
}
