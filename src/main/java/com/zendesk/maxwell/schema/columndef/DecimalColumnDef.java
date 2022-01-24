package com.zendesk.maxwell.schema.columndef;

import java.math.BigDecimal;

public class DecimalColumnDef extends ColumnDef {
	public DecimalColumnDef(String name, String type, short pos, boolean nullable) {
		super(name, type, pos, nullable);
	}

	public static DecimalColumnDef create(String name, String type, short pos) {
		DecimalColumnDef temp = new DecimalColumnDef(name, type, pos);
		return (DecimalColumnDef) INTERNER.intern(temp);
	}

	@Override
	public String toSQL(Object value) {
		BigDecimal d = (BigDecimal) value;

		return d.toEngineeringString();
	}
}
