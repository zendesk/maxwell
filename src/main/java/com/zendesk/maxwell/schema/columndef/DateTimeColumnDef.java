package com.zendesk.maxwell.schema.columndef;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.google.code.or.common.util.MySQLConstants;
import com.zendesk.maxwell.MaxwellConfig;

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
		Timestamp ts = DateFormatter.extractTimestamp(value);
		String dateString = DateFormatter.formatDateTime(value);
		if ( dateString == null )
			return null;
		else
			return objectWithPrecisionToString(dateString, ts, columnLength);
	}
}
