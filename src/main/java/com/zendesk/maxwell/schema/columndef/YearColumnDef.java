package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;

import java.sql.Date;
import java.util.Calendar;

public class YearColumnDef extends ColumnDef {
	public YearColumnDef(String name, String type, int pos) {
		super(name, type, pos);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_YEAR;
	}

	@Override
	public Object asJSON(Object value) {
		if ( value instanceof Date ) {
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(( java.sql.Date ) value);
			return calendar.get(Calendar.YEAR);
		}
		return value;
	}

	@Override
	public String toSQL(Object value) {
		return ((Integer)value).toString();
	}

}
