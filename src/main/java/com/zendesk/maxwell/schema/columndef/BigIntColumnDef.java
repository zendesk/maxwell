package com.zendesk.maxwell.schema.columndef;

import com.zendesk.maxwell.producer.MaxwellOutputConfig;

import java.math.BigInteger;
import java.util.Objects;

public class BigIntColumnDef extends ColumnDef {
	static private final BigInteger longlong_max = BigInteger.ONE.shiftLeft(64);

	private boolean signed;

	private BigIntColumnDef(String name, String type, short pos, boolean signed) {
		super(name, type, pos);
		this.signed = signed;
	}

	public static BigIntColumnDef create(String name, String type, short pos, boolean signed) {
		BigIntColumnDef temp = new BigIntColumnDef(name, type, pos, signed);
		return (BigIntColumnDef) INTERNER.intern(temp);
	}

	@Override
	public boolean equals(Object o) {
		if (o.getClass() == getClass()) {
			BigIntColumnDef other = (BigIntColumnDef)o;
			return super.equals(o)
					&& signed == other.signed;
		}
		return false;
	}

	@Override
	public int hashCode() {
		int hash = super.hashCode();
		return 31 * hash + Objects.hash(signed);
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

	public BigIntColumnDef withSigned(boolean signed) {
		return cloneSelfAndSet(clone -> {
			clone.signed = signed;
		});
	}
}
