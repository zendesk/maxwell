package com.zendesk.maxwell.schema.columndef;

import java.math.BigDecimal;

public class DecimalColumnDef extends ColumnDef {
	private DecimalColumnDef(String name, String type, short pos) {
		super(name, type, pos);
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
