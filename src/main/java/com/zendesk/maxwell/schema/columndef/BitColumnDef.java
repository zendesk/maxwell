package com.zendesk.maxwell.schema.columndef;

import java.math.BigInteger;
import java.util.BitSet;

public class BitColumnDef extends ColumnDef {
	public BitColumnDef(String name, String type, int pos) {
		super(name, type, pos);
	}

	@Override
	public Object asJSON(Object value) {
		byte[] bytes;
		if( value instanceof Boolean ){
			bytes = new byte[]{(byte) (( Boolean ) value ? 1 : 0)};
		} else if ( value instanceof BitSet ) {
			BitSet bs = (BitSet) value;
			bytes = bs.toByteArray();
		} else {
			bytes = (byte[]) value;
		}
		if ( bytes.length == 8 && ((bytes[7] & 0xFF) > 127) ) {
			return bytesToBigInteger(bytes);
		} else {
			return bytesToLong(bytes);
		}
	}

	private BigInteger bytesToBigInteger(byte[] bytes) {
		BigInteger res = BigInteger.ZERO;

		for (int i = 0; i < bytes.length; i++) {
			res = res.add(BigInteger.valueOf(bytes[i] & 0xFF).shiftLeft(i * 8));
		}

		return res;
	}

	private Long bytesToLong(byte[] bytes) {
		long res = 0;

		for (int i = 0; i < bytes.length; i++)
			res += ((bytes[i] & 0xFF) << ( i * 8 ));

		return res;
	}

	@Override
	public String toSQL(Object value) {
		return asJSON(value).toString();
	}
}
