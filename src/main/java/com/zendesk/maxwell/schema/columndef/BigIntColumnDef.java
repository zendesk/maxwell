package com.zendesk.maxwell.schema.columndef;

import java.math.BigInteger;

import com.google.code.or.common.util.MySQLConstants;

public class BigIntColumnDef extends ColumnDef {
	private final BigInteger longlong_max = BigInteger.ONE.shiftLeft(64);

	public BigIntColumnDef(String tableName, String name, String type, int pos, boolean signed) {
		super(tableName, name, type, pos);
		this.signed = signed;
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_LONGLONG;
	}

	private Object toNumeric(Object value) {
        Long l = (Long)value;
        if ( l < 0 && !signed )
        	return longlong_max.add(BigInteger.valueOf(l));
        else
            return Long.valueOf(l);
	}
	@Override
	public String toSQL(Object value) {
		return toNumeric(value).toString();
	}

	@Override
	public Object asJSON(Object value) {
		return toNumeric(value);
	}
}
