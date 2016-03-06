package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;

public class FloatColumnDef extends ColumnDef {
	public FloatColumnDef() { }
	public FloatColumnDef(String name, String type, int pos) {
		super(name, type, pos);
	}

	public boolean signed;

	@Override
	public boolean matchesMysqlType(int type) {
		if ( getType().equals("float") )
			return type == MySQLConstants.TYPE_FLOAT;
		else
			return type == MySQLConstants.TYPE_DOUBLE;
	}

	@Override
	public String toSQL(Object value) {
		return value.toString();
	}

	@Override
	public ColumnDef copy() {
		return new FloatColumnDef(name, type, pos);
	}

}
