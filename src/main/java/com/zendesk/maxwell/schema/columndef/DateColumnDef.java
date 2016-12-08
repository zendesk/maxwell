package com.zendesk.maxwell.schema.columndef;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.google.code.or.common.util.MySQLConstants;
import com.zendesk.maxwell.MaxwellConfig;

public class DateColumnDef extends ColumnDef {
	public DateColumnDef(String name, String type, int pos) {
		super(name, type, pos);
	}

	private static SimpleDateFormat dateFormatter;

	private static SimpleDateFormat getDateFormatter() {
		if ( dateFormatter == null ) {
			dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
			dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
		}
		return dateFormatter;
	}


	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_DATE;
	}

	private String formatDate(Object value) {
		/* protect against multithreaded access of static dateFormatter */
		synchronized ( DateColumnDef.class ) {
			if ( MaxwellConfig.ShykoMode && value instanceof Long ) {
				Long longVal = (Long) value;
				Date d = new Date(longVal);

				return getDateFormatter().format(d);
			} else {
				return getDateFormatter().format((Date) value);
			}
		}
	}

	@Override
	public String toSQL(Object value) {
		return "'" + formatDate(value) + "'";
	}

	@Override
	public Object asJSON(Object value) {
		return formatDate(value);
	}
}
