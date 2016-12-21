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
		if ( value instanceof Long )
			return new Timestamp((Long) value);
		else if ( value instanceof Date ) {
			Long time = ((Date) value).getTime();
			return new Timestamp(time);
		} else if ( value instanceof Timestamp )
			return (Timestamp) value;
		else
			throw new RuntimeException("couldn't extract date/time out of " + value);

	}

	public static String formatDate(Object value) {
		SimpleDateFormat formatter;

		// if value is a Long, this means it's coming back from shyko's binlog connector
		// and we should treat it as a UTC timestamp.
		if ( value instanceof Long )
			formatter = dateUTCFormatter;
		else
			formatter = dateFormatter;

		synchronized(formatter) {
			return formatter.format(extractTimestamp(value));
		}
	}

	public static String formatDateTime(Object value) {
		SimpleDateFormat formatter;

		if ( value instanceof Long )
			formatter = dateTimeUTCFormatter;
		else
			formatter = dateTimeFormatter;

		synchronized(formatter) {
			return formatter.format(extractTimestamp(value));
		}
	}
}
