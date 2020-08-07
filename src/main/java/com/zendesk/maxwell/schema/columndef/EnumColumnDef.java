package com.zendesk.maxwell.schema.columndef;

import com.zendesk.maxwell.producer.MaxwellOutputConfig;

public class EnumColumnDef extends EnumeratedColumnDef {
	public EnumColumnDef(String name, String type, short pos, String[] enumValues) {
		super(name, type, pos, enumValues);
	}

	@Override
	public String toSQL(Object value) throws ColumnDefCastException {
		return "'" + asString(value) + "'";
	}

	@Override
	public String asJSON(Object value, MaxwellOutputConfig config) throws ColumnDefCastException {
		return asString(value);
	}

	private String asString(Object value) throws ColumnDefCastException {
		if ( value instanceof String ) {
			return ( String ) value;
		} else if ( value instanceof Integer ) {
			Integer i = (Integer) value;

			if (i == 0)
				return null;
			else
				return enumValues[((Integer) value) - 1];
		} else {
			throw new ColumnDefCastException(this, value);
		}
	}
}
