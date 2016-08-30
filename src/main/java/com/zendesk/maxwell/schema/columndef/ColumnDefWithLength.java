package com.zendesk.maxwell.schema.columndef;

import java.sql.Time;
import java.sql.Timestamp;
import java.math.BigDecimal;
import java.math.BigInteger;

public abstract class ColumnDefWithLength extends ColumnDef {
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

	public Long getColumnLength() { return columnLength ; }

	@Override
	public String toSQL(Object value) {
		return "'" + formatValue(value) + "'";
	}


	@Override
	public Object asJSON(Object value) {
		return formatValue(value);
	}

	protected abstract String formatValue(Object value);

	// Rounds the number of nano secs to the column length.
	// See http://dev.mysql.com/doc/refman/5.6/en/fractional-seconds.html
	// 123 456 789 nano secs for:
	// - col length 3 -> 123 ;
	// - col length 6 -> 123 457.
	// 123 456 nano secs (123 micro secs):
	// - col length 3 -> 0 (milli secs)
	// - col length 6 -> 123 (micro secs).
	protected static int roundNanosToColumnLength(int nanos, Long columnLength) {
		// 6 is the max precision of datetime2/time6/timestamp2 in MysQL
		// 3: we go from nano (10^9) to micro (10^6)
		int divideBy = (int) Math.pow(10, 6 + 3 - columnLength);
		BigDecimal bigDivideBy = new BigDecimal(divideBy);
		java.math.RoundingMode rounding = java.math.RoundingMode.HALF_UP;
		// Scale of result is 0
		BigDecimal result = new BigDecimal(nanos).divide(bigDivideBy, 0, rounding);
		return result.intValue();
	}

	protected static String objectWithPrecisionToString(String value, Timestamp t, Long columnLength) {
		int fraction = roundNanosToColumnLength(t.getNanos(), columnLength);
		if (fraction == 0)
			return value;

		String strFormat = "%0" + columnLength + "d";
		StringBuilder result = threadLocalBuilder.get();
		result.append(value);
		result.append(".");
		result.append(String.format(strFormat, fraction));

		return result.toString();
	}
}
