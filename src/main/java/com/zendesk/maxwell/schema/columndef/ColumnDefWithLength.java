package com.zendesk.maxwell.schema.columndef;

import java.sql.Time;
import java.sql.Timestamp;
import java.math.BigDecimal;
import java.math.BigInteger;

public abstract class ColumnDefWithLength extends ColumnDef {
	public static boolean BinlogConnectorMode = true;
	protected Long columnLength;

	protected static ThreadLocal<StringBuilder> threadLocalBuilder = new ThreadLocal<StringBuilder>() {
		@Override
		protected StringBuilder initialValue() {
			return new StringBuilder();
		}

		@Override
		public StringBuilder get() {
			StringBuilder b = super.get();
			b.setLength(0);
			return b;
		}
	};

	public ColumnDefWithLength(String name, String type, int pos, Long columnLength) {
		super(name, type, pos);
		if ( columnLength == null )
			this.columnLength = 0L;
		else
			this.columnLength = columnLength;
	}

	@Override
	public String toSQL(Object value) {
		return "'" + formatValue(value) + "'";
	}

	@Override
	public Object asJSON(Object value) {
		return formatValue(value);
	}

	public Long getColumnLength() { return columnLength ; }

	public void setColumnLength(long length) {
		this.columnLength = length;
	}

	protected abstract String formatValue(Object value);

	protected static String buildStrFormatForColLength(Long columnLength) {
		StringBuilder result = threadLocalBuilder.get();
		result.append("%0");
		result.append(columnLength);
		result.append("d");
		return result.toString();
	}

	protected static String objectWithPrecisionToString(String value, Timestamp t, Long columnLength) {
		if ( columnLength == 0L )
			return value;

		// 6 is the max precision of datetime2/time6/timestamp2 in MysQL
		// 3: we go from nano (10^9) to micro (10^6)
		int divideBy = (int) Math.pow(10, 6 + 3 - columnLength);
		int fractional = ((int) t.getNanos()) / divideBy;

		String strFractional = String.format(buildStrFormatForColLength(columnLength), fractional);

		StringBuilder result = threadLocalBuilder.get();
		result.append(value);
		result.append(".");
		result.append(strFractional);

		return result.toString();
	}
}
