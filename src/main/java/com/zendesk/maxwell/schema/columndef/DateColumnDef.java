package com.zendesk.maxwell.schema.columndef;

import com.zendesk.maxwell.producer.MaxwellOutputConfig;

public class DateColumnDef extends ColumnDef {
	public DateColumnDef(String name, String type, short pos) {
		super(name, type, pos);
	}

	@Override
	public String toSQL(Object value) {
		String formatted = DateFormatter.formatDate(value);
		if ( formatted == null )
			return null;
		else
			return "'" +  formatted + "'";
	}

	@Override
	public Object asJSON(Object value, MaxwellOutputConfig config) {
		if ( value instanceof Long && (Long) value == Long.MIN_VALUE ) {
			if ( config.zeroDatesAsNull )
				return null;
			else
				return "0000-00-00";
		}

		return DateFormatter.formatDate(value);
	}
}
