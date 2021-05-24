package com.zendesk.maxwell.schema.columndef;

import java.math.BigDecimal;

public class DecimalColumnDef extends ColumnDef {
	public DecimalColumnDef(String name, String type, short pos, boolean nullable) {
		super(name, type, pos, nullable);
	}

	@Override
	public String toSQL(Object value) {
		BigDecimal d = (BigDecimal) value;

		return d.toEngineeringString();
	}
}
