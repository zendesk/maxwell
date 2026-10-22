package com.zendesk.maxwell.schema.columndef;

import com.zendesk.maxwell.producer.MaxwellOutputConfig;

import java.math.BigInteger;
import java.util.BitSet;

public class BitColumnDef extends ColumnDef {
	private BitColumnDef(String name, String type, short pos) {
		super(name, type, pos);
	}

	public static BitColumnDef create(String name, String type, short pos) {
		BitColumnDef temp = new BitColumnDef(name, type, pos);
		return (BitColumnDef) INTERNER.intern(temp);
	}

	@Override
	public Object asJSON(Object value, MaxwellOutputConfig outputConfig) throws ColumnDefCastException {
		byte[] bytes;
		switch (value) {
			case Boolean boolean1 -> bytes = new byte[]{(byte) (boolean1 ? 1 : 0)};
			case BitSet bs -> bytes = bs.toByteArray();
			case byte[] bytes1 -> bytes = bytes1;
			case null, default -> throw new ColumnDefCastException(this, value);
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
	public String toSQL(Object value) throws ColumnDefCastException {
		return asJSON(value, null).toString();
	}
}
