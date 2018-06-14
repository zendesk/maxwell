package com.zendesk.maxwell.schema.columndef;

import java.math.BigInteger;

public class BigIntColumnDef extends ColumnDef {
	static private final BigInteger longlong_max = BigInteger.ONE.shiftLeft(64);

	protected boolean signed;

	public BigIntColumnDef(String name, String type, int pos, boolean signed) {
		super(name, type, pos);
		this.signed = signed;
	}

	private Object toNumeric(Object value) {
        if ( value instanceof BigInteger ) {
          return value;
        }
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

	public boolean isSigned() {
		return signed;
	}

	public void setSigned(boolean signed) {
		this.signed = signed;
	}
}
