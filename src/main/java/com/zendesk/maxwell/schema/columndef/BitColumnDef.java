package com.zendesk.maxwell.schema.columndef;

import java.math.BigInteger;

import com.google.code.or.common.util.MySQLConstants;

public class BitColumnDef extends ColumnDef {


	public BitColumnDef(String tableName, String name, String type, int pos) {
		super(tableName, name, type, pos);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_BIT;
	}

	private Object toChar(Object value) {
       int val=Integer.parseInt(value.toString());
		return (char)val;
	}
	@Override
	public String toSQL(Object value) {
		return toChar(value).toString();
	}

	@Override
	public Object asJSON(Object value) {
		return toChar(value);
	}
}
