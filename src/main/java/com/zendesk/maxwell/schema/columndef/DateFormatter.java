package com.zendesk.maxwell.schema.columndef;

import java.text.SimpleDateFormat;
import java.sql.Timestamp;
import java.util.Date;
import java.util.TimeZone;

public class DateFormatter {
	private static SimpleDateFormat makeFormatter(String format, boolean utc) {
		SimpleDateFormat dateFormatter = new SimpleDateFormat(format);
		if ( utc )
			dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));

		return dateFormatter;
	}

	private static SimpleDateFormat dateFormatter           = makeFormatter("yyyy-MM-dd", false);
	private static SimpleDateFormat dateUTCFormatter        = makeFormatter("yyyy-MM-dd", true);
	private static SimpleDateFormat dateTimeFormatter       = makeFormatter("yyyy-MM-dd HH:mm:ss", false);
	private static SimpleDateFormat dateTimeUTCFormatter    = makeFormatter("yyyy-MM-dd HH:mm:ss", true);

	public static Timestamp extractTimestamp(Object value) {
		if (value instanceof Long) {
			Long micros = (Long) value;
			long millis = floorDiv(micros, 1000L);
			Timestamp t = new Timestamp(millis);
			long microsOnly = floorMod(micros, (long) 1000000);
			t.setNanos((int) microsOnly * 1000);
			return t;
		} else if (value instanceof Timestamp) {
			return (Timestamp) value;
		} else if ( value instanceof Date ) {
			Long time = ((Date) value).getTime();
			return new Timestamp(time);
		} else
			throw new RuntimeException("couldn't extract date/time out of " + value);
	}

	private static Timestamp MIN_DATE = Timestamp.valueOf("1000-01-01 00:00:00");

	private static String format(SimpleDateFormat formatter, Timestamp ts) {
		if ( ts.before(MIN_DATE) ) {
			return null;
		} else {
			synchronized(formatter) {
					return formatter.format(ts);
			}
		}
	}

	public static String formatDate(Object value) {
		SimpleDateFormat formatter;

		// if value is a Long, this means it's coming back from shyko's binlog connector
		// and we should treat it as a UTC timestamp.
		if ( value instanceof Long )
			formatter = dateUTCFormatter;
		else
			formatter = dateFormatter;

		return format(formatter, extractTimestamp(value));
	}

	public static String formatDateTime(Object value, Timestamp ts) {
		SimpleDateFormat formatter;

		if ( value instanceof Long )
			formatter = dateTimeUTCFormatter;
		else
			formatter = dateTimeFormatter;

		return format(formatter, ts);
	}

	private static long floorDiv(long a, long b) {
		return ((a < 0)?(a - (b - 1)):a) / b;
	}

	private static long floorMod(long x, long y) {
		return x - floorDiv(x, y) * y;
	}
}
