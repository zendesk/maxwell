package com.zendesk.maxwell.schema.columndef;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;

import com.google.code.or.common.util.MySQLConstants;

public class SetColumnDef extends EnumeratedColumnDef {
	public SetColumnDef(String name, String type, int pos, String[] enumValues) {
		super(name, type, pos, enumValues);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_SET;
	}

	@Override
	public String toSQL(Object value) {
		return "'" + StringUtils.join(asList(value), "'") + "'";
	}

	@Override
	public Object asJSON(Object value) {
		return asList(value);
	}

	private ArrayList<String> asList(Object value) {
		if ( value instanceof String ) {
			return new ArrayList<>(Arrays.asList((( String ) value).split(",")));
		}
		ArrayList<String> values = new ArrayList<>();
		long v = (Long) value;
		for(int i = 0; i < enumValues.length; i++) {
			if ( ((v >> i) & 1) == 1 ) {
				values.add(enumValues[i]);
			}
		}
		return values;
	}
}
