package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;

import java.math.BigInteger;

public class BitColumnDef extends ColumnDef {
	public BitColumnDef(String tableName, String name, String type, int pos) {
		super(tableName, name, type, pos);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_BIT;
	}

	@Override
	public Object asJSON(Object value) {
		byte[] bytes = (byte[]) value;
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
