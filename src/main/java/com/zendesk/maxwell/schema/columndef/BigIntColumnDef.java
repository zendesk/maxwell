package com.zendesk.maxwell.schema.columndef;

import com.zendesk.maxwell.producer.MaxwellOutputConfig;

import java.math.BigInteger;

public class BigIntColumnDef extends ColumnDef {
	static private final BigInteger longlong_max = BigInteger.ONE.shiftLeft(64);

	protected boolean signed;

	public BigIntColumnDef(String name, String type, short pos, boolean signed) {
		super(name, type, pos);
		this.signed = signed;
	}

	private Object toNumeric(Object value) throws ColumnDefCastException {
        if ( value instanceof BigInteger ) {
          return value;
        } else if ( !(value instanceof Long) ) {
        	throw new ColumnDefCastException(this, value);
		}

        Long l = (Long)value;
        if ( l < 0 && !signed )
        	return longlong_max.add(BigInteger.valueOf(l));
        else
            return Long.valueOf(l);
	}
	@Override
	public String toSQL(Object value) throws ColumnDefCastException {
		return toNumeric(value).toString();
	}

	@Override
	public Object asJSON(Object value, MaxwellOutputConfig config) throws ColumnDefCastException {
		return toNumeric(value);
	}

	public boolean isSigned() {
		return signed;
	}

	public void setSigned(boolean signed) {
		this.signed = signed;
	}
}
