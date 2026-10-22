package com.zendesk.maxwell.schema.columndef;

import com.zendesk.maxwell.producer.MaxwellOutputConfig;

import java.util.Objects;

public class IntColumnDef extends ColumnDef {
	private final int bits;

	private boolean signed;

	private IntColumnDef(String name, String type, short pos, boolean signed) {
		super(name, type, pos);
		this.signed = signed;
		this.bits = bitsFromType(type);
	}

	public static IntColumnDef create(String name, String type, short pos, boolean signed) {
		IntColumnDef temp = new IntColumnDef(name, type, pos, signed);
		return (IntColumnDef) INTERNER.intern(temp);
	}

	@Override
	public boolean equals(Object o) {
		if (o.getClass() == getClass()) {
			IntColumnDef other = (IntColumnDef)o;
			return super.equals(o)
					&& bits == other.bits
					&& signed == other.signed;
		}
		return false;
	}

	@Override
	public int hashCode() {
		int hash = super.hashCode();
		return 31 * hash + Objects.hash(bits, signed);
	}

	private long castUnsigned(Integer i, long max_value) {
		if ( i < 0 )
			return max_value + i;
		else
			return i;
	}

	private Long toLong(Object value) throws ColumnDefCastException {

		if ( value instanceof Long long1 ) {
			return long1;
		} else if ( value instanceof Boolean boolean1 ) {
			return boolean1 ? 1l: 0l;
		} else if ( value instanceof Integer i ) {

			if (signed)
				return Long.valueOf(i);

			long res = castUnsigned(i, 1L << this.bits);
			return Long.valueOf(res);
		} else {
			throw new ColumnDefCastException(this, value);
		}

	}
	@Override
	public String toSQL(Object value) throws ColumnDefCastException {
		return toLong(value).toString();
	}

	@Override
	public Object asJSON(Object value, MaxwellOutputConfig config) throws ColumnDefCastException {
		return toLong(value);
	}

	private final static int bitsFromType(String type) {
		return switch(type) {
		case "tinyint" -> 8;
		case "smallint" -> 16;
		case "mediumint" -> 24;
		case "int" -> 32;
		default -> 0;
		};
	}

	public boolean isSigned() {
		return signed;
	}

	public IntColumnDef withSigned(boolean signed) {
		return cloneSelfAndSet(clone -> {
			clone.signed = signed;
		});
	}
}
