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

	private static SimpleDateFormat dateTimeFormatter, dateTimeFormatterUTC;

	private static SimpleDateFormat getDateTimeFormatterUTC() {
		if ( dateTimeFormatterUTC == null ) {
			dateTimeFormatterUTC = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			dateTimeFormatterUTC.setTimeZone(TimeZone.getTimeZone("UTC"));
		}
		return dateTimeFormatterUTC;
	}

	private static SimpleDateFormat getDateTimeFormatter() {
		if ( dateTimeFormatter == null ) {
			dateTimeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
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

	protected String formatValue(Object value) {
		/* protect against multithreaded access of static dateTimeFormatter */
		synchronized ( DateTimeColumnDef.class ) {
			if ( MaxwellConfig.ShykoMode && value instanceof Long ) {
				/* BinlogConnector gives us long values; milliseconds since epoch. */
				Long longVal = (Long) value;
				Timestamp ts = new Timestamp(longVal);
				String dateString = getDateTimeFormatterUTC().format(ts);

				return objectWithPrecisionToString(dateString, ts, columnLength);

			} else if ( value instanceof Long && getType().equals("datetime") ) {
				/* used?  unused? */
				return formatLong(( Long ) value);

			} else if ( value instanceof Timestamp ) {
				/* the bootstrapper should end up here.  so should open-replicator. */
				Timestamp ts = (Timestamp) value;
				String datetimeAsStr = getDateTimeFormatter().format(ts);

				return objectWithPrecisionToString(datetimeAsStr, ts, this.columnLength);

			} else if ( value instanceof Date ) {
				Timestamp ts = new Timestamp((( Date ) value).getTime());
				String dateAsStr = getDateTimeFormatter().format(ts);

				return objectWithPrecisionToString(dateAsStr, ts, this.columnLength);

			} else {
				return "";
			}
		}
	}

	private String formatLong(Long value) {
		final int second = (int)(value % 100); value /= 100;
		final int minute = (int)(value % 100); value /= 100;
		final int hour = (int)(value % 100); value /= 100;
		final int day = (int)(value % 100); value /= 100;
		final int month = (int)(value % 100);
		final int year = (int)(value / 100);

		return String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
	}
}
