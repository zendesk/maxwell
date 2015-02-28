package com.zendesk.maxwell.schema.columndef;

import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;

import com.google.code.or.common.util.MySQLConstants;

public class SetColumnDef extends ColumnDef {
	public SetColumnDef(String tableName, String name, String type, int pos, String[] enumValues) {
		super(tableName, name, type, pos);
		this.enumValues = enumValues;
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
