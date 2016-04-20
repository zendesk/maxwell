package com.zendesk.maxwell.schema.columndef;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.google.code.or.common.util.MySQLConstants;

public class DateColumnDef extends ColumnDef {
	public DateColumnDef(String name, String type, int pos) {
		super(name, type, pos);
	}

	private static SimpleDateFormat dateFormatter;

	private static SimpleDateFormat getDateFormatter() {
		if ( dateFormatter == null ) {
			dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
		}
		return dateFormatter;
	}


	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_DATE;
	}

	private String formatDate(Object value) {
		// kristiankaufmann
		if ( value instanceof String) {
			return (String) value;
		}
		/* protect against multithreaded access of static dateFormatter */
		synchronized ( DateColumnDef.class ) {
			return getDateFormatter().format((Date) value);
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

	@Override
	public ColumnDef copy() {
		return new DateColumnDef(name, type, pos);
	}

}
