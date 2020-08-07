package com.zendesk.maxwell.schema.columndef;

import com.zendesk.maxwell.producer.MaxwellOutputConfig;

public class IntColumnDef extends ColumnDef {
	public int bits;

	protected boolean signed;

	public IntColumnDef(String name, String type, short pos, boolean signed) {
		super(name, type, pos);
		this.signed = signed;
		this.bits = bitsFromType(type);
	}


	private long castUnsigned(Integer i, long max_value) {
		if ( i < 0 )
			return max_value + i;
		else
			return i;
	}

	private Long toLong(Object value) throws ColumnDefCastException {

		if ( value instanceof Long ) {
			return ( Long ) value;
		} else if ( value instanceof Boolean ) {
			return ( Boolean ) value ? 1l: 0l;
		} else if ( value instanceof Integer ) {
			Integer i = (Integer) value;

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
		switch(type) {
		case "tinyint":
			return 8;
		case "smallint":
			return 16;
		case "mediumint":
			return 24;
		case "int":
			return 32;
		default:
			return 0;
		}
	}

	public boolean isSigned() {
		return signed;
	}

	public void setSigned(boolean signed) {
		this.signed = signed;
	}
}
