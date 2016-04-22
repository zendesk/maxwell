package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;

public class EnumColumnDef extends EnumeratedColumnDef {
	public EnumColumnDef(String name, String type, int pos, String[] enumValues) {
		super(name, type, pos, enumValues);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_ENUM;
	}

	@Override
	public String toSQL(Object value) {
		return "'" + asString(value) + "'";
	}

	@Override
	public String asJSON(Object value) {
		return asString(value);
	}

	private String asString(Object value) {
		if ( value instanceof String ) {
			return ( String ) value;
		}
		Integer i = (Integer) value;

		if ( i == 0 )
			return null;
		else
			return enumValues[((Integer) value) - 1];
	}
}
