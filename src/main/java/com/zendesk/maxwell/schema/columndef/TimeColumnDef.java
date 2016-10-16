package com.zendesk.maxwell.schema.columndef;

import java.sql.Time;
import java.sql.Timestamp;

import com.google.code.or.common.util.MySQLConstants;

public class TimeColumnDef extends ColumnDefWithLength {
	public TimeColumnDef(String name, String type, int pos, Long columnLength) {
		super(name, type, pos, columnLength);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_TIME || type == MySQLConstants.TYPE_TIME2;
	}

	protected String formatValue(Object value) {
		if ( value instanceof Timestamp ) {
			Time time = new Time(((Timestamp) value).getTime());
			String timeAsStr = String.valueOf(time);

			return objectWithPrecisionToString(timeAsStr, (Timestamp) value, this.columnLength);

		} else {
			return String.valueOf((Time) value);
		}
	}
}
